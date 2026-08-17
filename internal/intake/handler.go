package intake

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// PaymentAccepter durably accepts validated Payments.
type PaymentAccepter interface {
	// Accept records a Payment and its outbox row atomically.
	Accept(ctx context.Context, correlationID uuid.UUID, amountCents int64) error
}

// NewHandler validates the public payment representation before durable
// acceptance.
func NewHandler(acceptor PaymentAccepter, logger *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID, amountCents, err := decodePayment(r.Body)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}

		if err := acceptor.Accept(r.Context(), correlationID, amountCents); err != nil {
			logger.Error("accept payment", zap.Error(err), zap.String("correlation_id", correlationID.String()))
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
}

type paymentRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

func decodePayment(body io.Reader) (uuid.UUID, int64, error) {
	var request paymentRequest

	decoder := json.NewDecoder(body)
	if err := decoder.Decode(&request); err != nil {
		return uuid.Nil, 0, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return uuid.Nil, 0, errors.New("request must contain one JSON value")
	}

	correlationID, err := uuid.Parse(request.CorrelationID)
	if err != nil {
		return uuid.Nil, 0, errors.New("invalid correlationId")
	}
	amountCents, err := cents(request.Amount)
	if err != nil || amountCents <= 0 {
		return uuid.Nil, 0, errors.New("invalid amount")
	}
	return correlationID, amountCents, nil
}

// cents rounds the JSON amount to its nearest cent before persistence.
func cents(amount float64) (int64, error) {
	roundedCents := math.Round(amount * 100)
	if math.IsNaN(roundedCents) || math.IsInf(roundedCents, 0) || roundedCents < 1 || roundedCents >= float64(math.MaxInt64) {
		return 0, errors.New("amount out of range")
	}
	return int64(roundedCents), nil
}
