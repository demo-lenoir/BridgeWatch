package rpcpool

import (
	"context"
	"errors"
)

// PublicError keeps endpoint URLs and credentials out of watcher diagnostics.
func PublicError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("RPC request failed")
}
