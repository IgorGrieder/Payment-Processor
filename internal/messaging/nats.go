package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

func Connect(url string) (*nats.Conn, error) {
	return nats.Connect(url)
}

// WorkPublisher publishes durable Payment work references.
type WorkPublisher interface {
	// Publish sends correlationID to the work stream and waits for JetStream's
	// PubAck or for ctx to expire.
	Publish(ctx context.Context, correlationID uuid.UUID) error
}

type jetStreamWorkPublisher struct {
	js      nats.JetStreamContext
	subject string
}

// ProvisionWorkStream creates the stream when absent and rejects an existing
// stream that does not provide the durability semantics required by the outbox.
func ProvisionWorkStream(conn *nats.Conn, name, subject string, duplicateWindow time.Duration) (WorkPublisher, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("create JetStream context: %w", err)
	}

	info, err := js.StreamInfo(name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		info, err = js.AddStream(&nats.StreamConfig{
			Name:       name,
			Subjects:   []string{subject},
			Storage:    nats.FileStorage,
			Retention:  nats.WorkQueuePolicy,
			Duplicates: duplicateWindow,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("provision JetStream work stream: %w", err)
	}
	if info.Config.Storage != nats.FileStorage || info.Config.Retention != nats.WorkQueuePolicy || info.Config.Duplicates != duplicateWindow || !hasSubject(info.Config.Subjects, subject) {
		return nil, fmt.Errorf("JetStream stream %q does not use required file-backed work-queue settings", name)
	}
	return &jetStreamWorkPublisher{js: js, subject: subject}, nil
}

func (s *jetStreamWorkPublisher) Publish(ctx context.Context, correlationID uuid.UUID) error {
	message := nats.NewMsg(s.subject)
	message.Header.Set(nats.MsgIdHdr, correlationID.String())
	message.Data = []byte(correlationID.String())
	if _, err := s.js.PublishMsg(message, nats.Context(ctx)); err != nil {
		return fmt.Errorf("publish Payment work reference: %w", err)
	}
	return nil
}

func hasSubject(subjects []string, subject string) bool {
	for _, candidate := range subjects {
		if candidate == subject {
			return true
		}
	}
	return false
}
