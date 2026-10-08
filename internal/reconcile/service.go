package reconcile

import (
	"context"
	"fmt"

	"bridgewatch/internal/domain"
	"bridgewatch/internal/protocol"
	"bridgewatch/internal/store"
)

type Repository interface {
	IngestSource(context.Context, domain.SourceClaim) (store.IngestResult, error)
	IngestDestination(context.Context, domain.DestinationClaim) (store.IngestResult, error)
}

type Service struct {
	adapter    protocol.Adapter
	repository Repository
}

func New(adapter protocol.Adapter, repository Repository) *Service {
	return &Service{adapter: adapter, repository: repository}
}

func (s *Service) IngestSource(ctx context.Context, entry protocol.EventLog) (store.IngestResult, error) {
	claim, err := s.adapter.DecodeSource(ctx, entry)
	if err != nil {
		return store.IngestResult{}, fmt.Errorf("decode source: %w", err)
	}
	return s.repository.IngestSource(ctx, claim)
}

func (s *Service) IngestDestination(ctx context.Context, entry protocol.EventLog) (store.IngestResult, error) {
	claim, err := s.adapter.DecodeDestination(ctx, entry)
	if err != nil {
		return store.IngestResult{}, fmt.Errorf("decode destination: %w", err)
	}
	return s.repository.IngestDestination(ctx, claim)
}
