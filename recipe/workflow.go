package recipe

import (
	"log"
	"time"

	"go.temporal.io/sdk/workflow"
)

type LabelingRecipe struct {
	Name          string         `yaml:"name"`
	SchemaVersion string         `yaml:"schema_version"`
	Rules         []LabelingRule `yaml:"rules"`
}

type LabelingRule struct {
	Name         string       `yaml:"name,omitempty"`
	Skip         bool         `yaml:"skip,omitempty"`
	Dependencies []Dependency `yaml:"dependencies,omitempty"`
	LabelClasses []LabelClass `yaml:"label_classes"`
}

type LabelClass struct {
	Name string `yaml:"name"`
}

type Dependency struct {
	RuleName          string             `yaml:"rule_name,omitempty"`
	LabelClassFilters []LabelClassFilter `yaml:"label_class_filters,omitempty"`
}

type LabelClassFilter struct {
	LabelClassKey string `yaml:"label_class,omitempty"`
}

// type LabelClassState struct {
// 	Name      string
// 	Enqueued  bool
// 	Completed bool
// }

// Function to convert the YAML structure into a map of label class dependencies
func ConvertToLabelClassDependencies(recipe LabelingRecipe) map[string][]string {
	dependencies := make(map[string][]string)
	ruleMap := make(map[string]LabelingRule)
	for _, rule := range recipe.Rules {
		ruleMap[rule.Name] = rule
	}

	// Iterate over each rule
	for _, rule := range recipe.Rules {
		// Iterate over each label class in the rule
		for _, labelClass := range rule.LabelClasses {
			labelClassName := labelClass.Name

			// Check if the rule has any dependencies
			if len(rule.Dependencies) > 0 {
				// Iterate over each dependency of the rule
				for _, dependency := range rule.Dependencies {
					// find target dependency rule name
					dependencyRuleName := dependency.RuleName

					// get target dependency rule from the ruleMap
					dependencyRule, ok := ruleMap[dependencyRuleName]

					if !ok {
						log.Fatalf("Could not find dependency rule %s", dependencyRuleName)
					}

					// Iterate over each label class in the dependency rule
					for _, dependencyLabelClass := range dependencyRule.LabelClasses {
						dependencyLabelClassName := dependencyLabelClass.Name

						// Check if the dependency rule has any label class filters
						if len(dependency.LabelClassFilters) > 0 {
							// Iterate over each label class filter of the dependency rule
							for _, dependencyLabelClassFilter := range dependency.LabelClassFilters {
								dependencyLabelClassFilterName := dependencyLabelClassFilter.LabelClassKey

								// Check if the dependency label class filter matches the label class of the dependency rule
								if dependencyLabelClassName == dependencyLabelClassFilterName {
									// Add the label class name to the list of dependencies
									dependencies[labelClassName] = append(dependencies[labelClassName], dependencyLabelClassName)
								}
							}
						} else {
							// Add the label class name to the list of dependencies
							dependencies[labelClassName] = append(dependencies[labelClassName], dependencyLabelClassName)
						}
					}
				}
			} else {
				dependencies[labelClassName] = []string{}
			}
		}
	}

	return dependencies
}

// Recipe workflow definition
func RecipeWorkflow(ctx workflow.Context, recipe LabelingRecipe) ([]byte, error) {
	LCDependency := ConvertToLabelClassDependencies(recipe)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)
	logger.Info("Start")
	wfSelector := workflow.NewSelector(ctx)
	futureCounts := 0

	for LC, dependencyLC := range LCDependency {
		wfSelector.AddReceive(workflow.GetSignalChannel(ctx, LC), func(c workflow.ReceiveChannel, more bool) {
			logger.Info("Complete Signal received for outer loop")
		})
		futureCounts++ // wait complete signal counts
		if len(dependencyLC) > 0 {
			// setup signals for all dependent label classes
			dependenciesSelector := workflow.NewSelector(ctx)
			for _, dependencyLCName := range dependencyLC {
				dependenciesSelector.AddReceive(workflow.GetSignalChannel(ctx, dependencyLCName), func(c workflow.ReceiveChannel, more bool) {
					logger.Info("Complete Signal received for depdendency", more)
				})
			}
			for i := 0; i < len(dependencyLC); i++ {
				dependenciesSelector.Select(ctx)
				logger.Info("Waiting")
			}
		}
		wfSelector.AddFuture(workflow.ExecuteActivity(ctx, ReEnqueueActivity, LC), func(f workflow.Future) {
			logger.Info("Label Class", LC, "enqueued")
		})
		futureCounts++ // enqueue future counts
	}

	for i := 0; i < futureCounts; i++ { // both wait complete signal counts and enqueue counts
		wfSelector.Select(ctx)
		logger.Info("waiting all futures to complete")
	}

	logger.Info("Recipe Workflow completed.")
	return nil, nil
}

func executeAsync(e, ctx workflow.Context, bindings map[string]string) workflow.Future {
	future, settable := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		err := exe.execute(ctx, bindings)
		settable.Set(nil, err)
	})
	return future
}
