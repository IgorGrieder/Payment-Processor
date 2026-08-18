package availability

import (
	"context"
	"fmt"
	"sync"
	"time"

	"payment-processor/internal/database"
	"payment-processor/internal/domain"
)

// Broadcaster publishes an already-persisted availability update. Core-NATS
// delivery is transient; PostgreSQL remains the startup-recovery authority.
type Broadcaster func(context.Context, domain.ProcessorAvailability) error

// Service keeps worker routing entirely local while ensuring every update is
// first committed in PostgreSQL, then applied locally, then broadcast.
type Service struct {
	store     *database.Store
	broadcast Broadcaster

	mu    sync.RWMutex
	state map[domain.ProcessorService]domain.ProcessorAvailability
}

func New(store *database.Store, broadcast Broadcaster) *Service {
	return &Service{
		store:     store,
		broadcast: broadcast,
		state:     make(map[domain.ProcessorService]domain.ProcessorAvailability),
	}
}

// Hydrate applies authoritative startup state. It is safe to call after the
// live subscription is active: versions prevent this read from overwriting a
// newer received event.
func (s *Service) Hydrate(ctx context.Context) error {
	states, err := s.store.ReadProcessorAvailability(ctx)
	if err != nil {
		return err
	}
	for _, state := range states {
		s.Apply(state)
	}
	return nil
}

// Apply accepts only a newer persisted version for that Processor.
func (s *Service) Apply(state domain.ProcessorAvailability) {
	if !state.Service.Valid() || state.Version <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.state[state.Service]
	if !exists || state.Version > current.Version {
		s.state[state.Service] = state
	}
}

// Select returns a Processor only for a new, unassigned Payment. Default
// requires known availability; otherwise Fallback also requires known
// availability, and no assignment is made while neither is known healthy.
func (s *Service) Select() (domain.ProcessorService, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	defaultState, defaultKnown := s.state[domain.DefaultProcessor]
	if defaultKnown && defaultState.Available {
		return domain.DefaultProcessor, true
	}
	fallbackState, fallbackKnown := s.state[domain.FallbackProcessor]
	if fallbackKnown && fallbackState.Available {
		return domain.FallbackProcessor, true
	}
	return "", false
}

// RecordPassiveFailure immediately takes a Processor out of routing after a
// real submission timeout or 5xx.
func (s *Service) RecordPassiveFailure(ctx context.Context, service domain.ProcessorService) error {
	state, err := s.store.RecordPassiveFailure(ctx, service)
	if err != nil {
		return fmt.Errorf("persist passive %s Processor failure: %w", service, err)
	}
	s.Apply(state)
	if s.broadcast == nil {
		return nil
	}
	if err := s.broadcast(ctx, state); err != nil {
		return fmt.Errorf("broadcast passive %s Processor failure: %w", service, err)
	}
	return nil
}

// RecordPollObservation commits a successful health observation. An old
// healthy observation rejected by persistence never changes this cache.
func (s *Service) RecordPollObservation(ctx context.Context, service domain.ProcessorService, available bool, startedAt time.Time) (bool, error) {
	state, accepted, err := s.store.RecordPollObservation(ctx, service, available, startedAt)
	if err != nil {
		return false, err
	}
	if !accepted {
		return false, nil
	}
	s.Apply(state)
	if s.broadcast == nil {
		return true, nil
	}
	if err := s.broadcast(ctx, state); err != nil {
		return true, fmt.Errorf("broadcast %s Processor Availability: %w", service, err)
	}
	return true, nil
}
