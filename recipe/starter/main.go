package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/sdk/client"
	"gopkg.in/yaml.v3"

	"github.com/temporalio/samples-go/recipe"
)

func main() {

	// Example YAML data
	yamlData := `
name: Example Recipe
schema_version: "1.0"
rules:
  - name: Rule1
    skip: false
    dependencies:
      - rule_name: Rule2
        label_class_filters:
          - label_class: Class3
    label_classes:
      - name: Class1
      - name: Class2
  - name: Rule2
    skip: true
    dependencies:
      - rule_name: Rule3
    label_classes:
      - name: Class3
      - name: Class4
  - name: Rule3
    skip: false
    label_classes:
      - name: Class5
  - name: Rule4
    skip: false
    dependencies:
      - rule_name: Rule1
        label_class_filters:
        - label_class: Class1
    label_classes:
      - name: Class6
`

	var recipeInput recipe.LabelingRecipe

	err := yaml.Unmarshal([]byte(yamlData), &recipeInput)
	if err != nil {
		log.Fatalf("Failed to unmarshal YAML: %v", err)
	}

	// Convert the YAML structure into a map of label class dependencies
	dependencies := recipe.ConvertToLabelClassDependencies(recipeInput)

	// Print the label class dependencies
	for labelClass, deps := range dependencies {
		fmt.Printf("Label Class: %s\n", labelClass)
		fmt.Printf("Dependencies: %v\n", deps)
		fmt.Println("------------------------")
	}

	// The client is a heavyweight object that should be created once per process.
	c, err := client.Dial(client.Options{
		HostPort: client.DefaultHostPort,
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:        "recipe_" + uuid.New(),
		TaskQueue: "recipe",
	}

	we, err := c.ExecuteWorkflow(context.Background(), workflowOptions, recipe.RecipeWorkflow, recipeInput)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())

	signals := []string{"Class5", "Class4", "Class3", "Class1", "Class2", "Class6"}

	for _, signal := range signals {
		signalName := fmt.Sprintf("Signal %s", signal)
		err = c.SignalWorkflow(context.Background(), we.GetID(), we.GetRunID(), signalName, nil)
		if err != nil {
			log.Fatalln("Unable to signals workflow", err)
		}
		log.Println("Sent " + signalName)
		time.Sleep(5 * time.Second)
	}
}
