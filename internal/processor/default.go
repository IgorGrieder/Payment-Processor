// Package processor contains concrete adapters for external Payment Processors.
package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"payment-processor/internal/domain"
)

// Client submits Payments and reads Processor Availability for one configured
// Payment Processor.
type Client struct {
	service domain.ProcessorService
	url     string
	timeout time.Duration
	client  *http.Client
}

func New(service domain.ProcessorService, url string, timeout time.Duration) *Client {
	return &Client{
		service: service,
		url:     strings.TrimRight(url, "/"),
		timeout: timeout,
		client:  &http.Client{},
	}
}

func (p *Client) Service() domain.ProcessorService {
	return p.service
}

// ProcessResult reports whether a Processor response confirmed Payment
// processing, is retryable, or proves the Processor unavailable for new
// assignments.
type ProcessResult uint8

const (
	ProcessRetryable ProcessResult = iota
	ProcessConfirmed
	ProcessUnavailable
)

// Process sends the stored Payment representation. Both 200 OK and 422
// Unprocessable Entity confirm processing. A deadline expiry or 5xx marks this
// Processor unavailable; other transport and HTTP outcomes are retryable.
func (p *Client) Process(ctx context.Context, payment domain.Payment) (ProcessResult, error) {
	body, err := json.Marshal(processRequest{
		CorrelationID: payment.CorrelationID.String(),
		Amount:        cents(payment.Amount),
		RequestedAt:   payment.RequestedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return ProcessRetryable, fmt.Errorf("encode %s Processor Payment: %w", p.service, err)
	}

	requestCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, p.url+"/payments", bytes.NewReader(body))
	if err != nil {
		return ProcessRetryable, fmt.Errorf("build %s Processor request: %w", p.service, err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := p.client.Do(request)
	if err != nil {
		if requestCtx.Err() == context.DeadlineExceeded {
			return ProcessUnavailable, fmt.Errorf("call %s Processor timed out: %w", p.service, err)
		}
		return ProcessRetryable, fmt.Errorf("call %s Processor: %w", p.service, err)
	}
	defer response.Body.Close()
	discardResponseBody(response.Body)
	if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusUnprocessableEntity {
		return ProcessConfirmed, nil
	}
	if response.StatusCode >= http.StatusInternalServerError && response.StatusCode <= 599 {
		return ProcessUnavailable, fmt.Errorf("%s Processor returned %s", p.service, response.Status)
	}
	return ProcessRetryable, fmt.Errorf("%s Processor returned %s", p.service, response.Status)
}

// Availability obtains the current Processor Availability. Only a complete
// 200 response is an observation; all other outcomes leave persisted state
// unchanged.
func (p *Client) Availability(ctx context.Context) (bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, p.url+"/payments/service-health", nil)
	if err != nil {
		return false, fmt.Errorf("build %s Processor Availability request: %w", p.service, err)
	}

	response, err := p.client.Do(request)
	if err != nil {
		if requestCtx.Err() == context.DeadlineExceeded {
			return false, fmt.Errorf("call %s Processor Availability timed out: %w", p.service, err)
		}
		return false, fmt.Errorf("call %s Processor Availability: %w", p.service, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		discardResponseBody(response.Body)
		return false, fmt.Errorf("%s Processor Availability returned %s", p.service, response.Status)
	}

	var result serviceHealthResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("decode %s Processor Availability: %w", p.service, err)
	}
	if result.Failing == nil {
		return false, fmt.Errorf("decode %s Processor Availability: missing failing", p.service)
	}
	return !*result.Failing, nil
}

func discardResponseBody(body io.Reader) {
	_, _ = io.Copy(io.Discard, body)
}

type processRequest struct {
	CorrelationID string `json:"correlationId"`
	Amount        cents  `json:"amount"`
	RequestedAt   string `json:"requestedAt"`
}

type serviceHealthResponse struct {
	Failing *bool `json:"failing"`
}

// cents is serialized as a decimal JSON number only at the Processor boundary.
type cents int64

func (amount cents) MarshalJSON() ([]byte, error) {
	value := int64(amount)
	if value < 0 {
		return []byte("-" + strconv.FormatInt(-value/100, 10) + "." + fmt.Sprintf("%02d", -value%100)), nil
	}
	return []byte(strconv.FormatInt(value/100, 10) + "." + fmt.Sprintf("%02d", value%100)), nil
}
