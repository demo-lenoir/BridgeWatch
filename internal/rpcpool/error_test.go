package rpcpool

import (
	"errors"
	"strings"
	"testing"
)

func TestPublicErrorDoesNotExposeEndpointCredentials(t *testing.T) {
	const secret = "token-secret-example"
	err := PublicError(errors.New("Post https://example.invalid/rpc?token=" + secret + ": unavailable"))
	if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "example.invalid") {
		t.Fatalf("unsafe RPC error: %v", err)
	}
}
