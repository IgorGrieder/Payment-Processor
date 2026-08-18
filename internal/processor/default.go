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

// Default sends Payments to the Default Processor.
type Default struct {
	url    string
	client *http.Client
}

func NewDefault(url string) *Default {
	return &Default{
		url:    strings.TrimRight(url, "/"),
		client: &http.Client{},
	}
}

// Process sends the stored Payment representation. Only a confirmed 200 OK is
// success in this slice.
func (p *Default) Process(ctx context.Context, payment domain.Payment) error {
	body, err := json.Marshal(processRequest{
		CorrelationID: payment.CorrelationID.String(),
		Amount:        cents(payment.Amount),
		RequestedAt:   payment.RequestedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("encode Default Processor Payment: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url+"/payments", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build Default Processor request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := p.client.Do(request)
	if err != nil {
		return fmt.Errorf("call Default Processor: %w", err)
	}
	defer response.Body.Close()
	discardResponseBody(response.Body)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Default Processor returned %s", response.Status)
	}
	return nil
}

func discardResponseBody(body io.Reader) {
	_, _ = io.Copy(io.Discard, body)
}

type processRequest struct {
	CorrelationID string `json:"correlationId"`
	Amount        cents  `json:"amount"`
	RequestedAt   string `json:"requestedAt"`
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
