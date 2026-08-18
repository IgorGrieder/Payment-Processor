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

// WorkConsumer receives Payment work references from the shared durable pull
// consumer. A delivery is acknowledged only by the processing lifecycle after
// durable terminal completion.
type WorkConsumer interface {
	Next(ctx context.Context) (WorkDelivery, error)
}

type WorkDelivery interface {
	CorrelationID() (uuid.UUID, error)
	Ack(ctx context.Context) error
	Nak(delay time.Duration) error
}

type jetStreamWorkConsumer struct {
	sub *nats.Subscription
}

type jetStreamWorkDelivery struct {
	message *nats.Msg
}

const workConsumerDurable = "PAYMENT_PROCESSOR"

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

// ProvisionWorkConsumer ensures the single durable pull consumer shared by
// all instances has the delivery semantics required by processing workers.
func ProvisionWorkConsumer(conn *nats.Conn, stream, subject string, ackWait time.Duration, maxAckPending int) (WorkConsumer, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("create JetStream context: %w", err)
	}

	info, err := js.ConsumerInfo(stream, workConsumerDurable)
	if errors.Is(err, nats.ErrConsumerNotFound) {
		info, err = js.AddConsumer(stream, &nats.ConsumerConfig{
			Durable:       workConsumerDurable,
			DeliverPolicy: nats.DeliverAllPolicy,
			AckPolicy:     nats.AckExplicitPolicy,
			AckWait:       ackWait,
			MaxDeliver:    -1,
			MaxAckPending: maxAckPending,
			FilterSubject: subject,
		})
		if err != nil {
			// Another instance may have created the shared durable consumer
			// after the lookup above.
			info, err = js.ConsumerInfo(stream, workConsumerDurable)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("provision JetStream Payment work consumer: %w", err)
	}
	if info.Config.AckPolicy != nats.AckExplicitPolicy || info.Config.AckWait != ackWait || info.Config.MaxDeliver != -1 || info.Config.MaxAckPending != maxAckPending || info.Config.FilterSubject != subject || info.Config.DeliverSubject != "" {
		return nil, fmt.Errorf("JetStream consumer %q does not use required pull-work settings", workConsumerDurable)
	}

	subscription, err := js.PullSubscribe(subject, workConsumerDurable, nats.Bind(stream, workConsumerDurable))
	if err != nil {
		return nil, fmt.Errorf("bind JetStream Payment work consumer: %w", err)
	}
	return &jetStreamWorkConsumer{sub: subscription}, nil
}

func (c *jetStreamWorkConsumer) Next(ctx context.Context) (WorkDelivery, error) {
	messages, err := c.sub.Fetch(1, nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("fetch Payment work reference: %w", err)
	}
	if len(messages) != 1 {
		return nil, errors.New("JetStream returned no Payment work reference")
	}
	return jetStreamWorkDelivery{message: messages[0]}, nil
}

func (d jetStreamWorkDelivery) CorrelationID() (uuid.UUID, error) {
	correlationID, err := uuid.Parse(string(d.message.Data))
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse Payment work correlation ID: %w", err)
	}
	return correlationID, nil
}

func (d jetStreamWorkDelivery) Ack(ctx context.Context) error {
	if err := d.message.AckSync(nats.Context(ctx)); err != nil {
		return fmt.Errorf("acknowledge Payment work reference: %w", err)
	}
	return nil
}

// Nak schedules a redelivery without waiting for the consumer AckWait.
func (d jetStreamWorkDelivery) Nak(delay time.Duration) error {
	if err := d.message.NakWithDelay(delay); err != nil {
		return fmt.Errorf("delay Payment work redelivery: %w", err)
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
