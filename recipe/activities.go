package recipe

import (
	"context"

	"go.temporal.io/sdk/activity"
)

func ReEnqueueActivity(ctx context.Context, labelClass string) (err error) {
	activity.GetLogger(ctx).Info("LabelClass", labelClass, "Enqueued")
	return nil
}
