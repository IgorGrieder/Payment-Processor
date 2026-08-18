package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config contains runtime infrastructure configuration.
type Config struct {
	HTTPAddr                 string
	DatabaseURL              string
	DatabaseMaxConns         int
	NATSURL                  string
	InstanceID               string
	OutboxWorkers            int
	NATSPublishTimeout       time.Duration
	OutboxClaimExpiry        time.Duration
	JetStreamStream          string
	JetStreamSubject         string
	JetStreamDuplicateWindow time.Duration
	ProcessingWorkers        int
	PaymentClaimExpiry       time.Duration
	ProcessorTimeout         time.Duration
	ProcessorDefaultURL      string
	JetStreamAckWait         time.Duration
	JetStreamMaxAckPending   int
}

func FromEnv() (Config, error) {
	maxConns, err := positiveInt("DATABASE_MAX_CONNS", 12)
	if err != nil {
		return Config{}, err
	}
	workers, err := positiveInt("OUTBOX_WORKERS", 1)
	if err != nil {
		return Config{}, err
	}
	processingWorkers, err := positiveInt("PROCESSING_WORKERS", 6)
	if err != nil {
		return Config{}, err
	}
	publishTimeout, err := positiveDuration("NATS_PUBLISH_TIMEOUT", 2*time.Second)
	if err != nil {
		return Config{}, err
	}
	claimExpiry, err := positiveDuration("OUTBOX_CLAIM_EXPIRY", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	paymentClaimExpiry, err := positiveDuration("PAYMENT_CLAIM_EXPIRY", 20*time.Second)
	if err != nil {
		return Config{}, err
	}
	processorTimeout, err := positiveDuration("PROCESSOR_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	consumerAckWait, err := positiveDuration("JETSTREAM_ACK_WAIT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	consumerMaxAckPending, err := positiveInt("JETSTREAM_MAX_ACK_PENDING", 12)
	if err != nil {
		return Config{}, err
	}
	duplicateWindow, err := positiveDuration("JETSTREAM_DUPLICATE_WINDOW", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID, err = os.Hostname()
		if err != nil {
			return Config{}, fmt.Errorf("get hostname: %w", err)
		}
	}

	return Config{
		HTTPAddr:                 envOr("HTTP_ADDR", ":8080"),
		DatabaseURL:              envOr("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/rinha?sslmode=disable"),
		DatabaseMaxConns:         maxConns,
		NATSURL:                  envOr("NATS_URL", "nats://localhost:4222"),
		InstanceID:               instanceID,
		OutboxWorkers:            workers,
		NATSPublishTimeout:       publishTimeout,
		OutboxClaimExpiry:        claimExpiry,
		JetStreamStream:          envOr("JETSTREAM_STREAM", "PAYMENTS"),
		JetStreamSubject:         envOr("JETSTREAM_SUBJECT", "payments.work"),
		JetStreamDuplicateWindow: duplicateWindow,
		ProcessingWorkers:        processingWorkers,
		PaymentClaimExpiry:       paymentClaimExpiry,
		ProcessorTimeout:         processorTimeout,
		ProcessorDefaultURL:      envOr("PROCESSOR_DEFAULT_URL", "http://payment-processor-default:8080"),
		JetStreamAckWait:         consumerAckWait,
		JetStreamMaxAckPending:   consumerMaxAckPending,
	}, nil
}

func positiveInt(key string, fallback int) (int, error) {
	value := envOr(key, strconv.Itoa(fallback))
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return parsed, nil
}

func positiveDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := envOr(key, fallback.String())
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return parsed, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
