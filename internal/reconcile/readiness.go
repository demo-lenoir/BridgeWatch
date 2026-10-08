package reconcile

import "context"

type ReadyChecker interface {
	Ready(context.Context) (bool, error)
}

// BothReady preserves independent chain health while deriving global readiness.
func BothReady(ctx context.Context, source, destination ReadyChecker) (bool, error) {
	sourceReady, err := source.Ready(ctx)
	if err != nil {
		return false, err
	}
	destinationReady, err := destination.Ready(ctx)
	if err != nil {
		return false, err
	}
	return sourceReady && destinationReady, nil
}
