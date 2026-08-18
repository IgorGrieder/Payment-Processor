package summary

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"

	"payment-processor/internal/database"
	"payment-processor/internal/domain"
)

// NewHandler validates summary filters and renders the audit summary.
func NewHandler(store *database.Store, logger *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, err := parseTimestamp(r, "from")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		to, err := parseTimestamp(r, "to")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		result, err := store.PaymentSummary(r.Context(), from, to)
		if err != nil {
			logger.Error("summarize payments", zap.Error(err))
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(responseFrom(result)); err != nil {
			logger.Error("encode payment summary", zap.Error(err))
		}
	})
}

func parseTimestamp(r *http.Request, name string) (*time.Time, error) {
	values, present := r.URL.Query()[name]
	if !present {
		return nil, nil
	}
	if len(values) != 1 {
		return nil, errors.New("timestamp must occur once")
	}

	timestamp, err := time.Parse(time.RFC3339, values[0])
	if err != nil {
		return nil, err
	}
	_, offset := timestamp.Zone()
	if offset != 0 {
		return nil, errors.New("timestamp must be UTC")
	}
	timestamp = timestamp.UTC()
	return &timestamp, nil
}

type response struct {
	Default  processorTotal `json:"default"`
	Fallback processorTotal `json:"fallback"`
}

type processorTotal struct {
	TotalRequests int64        `json:"totalRequests"`
	TotalAmount   decimalCents `json:"totalAmount"`
}

func responseFrom(result domain.PaymentSummary) response {
	return response{
		Default:  processorTotalFrom(result.Default),
		Fallback: processorTotalFrom(result.Fallback),
	}
}

func processorTotalFrom(total domain.ProcessorTotal) processorTotal {
	return processorTotal{
		TotalRequests: total.TotalRequests,
		TotalAmount:   decimalCents(total.TotalAmountCents),
	}
}

// decimalCents writes an exact decimal JSON number without a float conversion.
type decimalCents int64

func (c decimalCents) MarshalJSON() ([]byte, error) {
	cents := int64(c)
	fraction := cents % 100
	if fraction < 0 {
		fraction = -fraction
	}

	var number []byte
	if cents < 0 && cents > -100 {
		number = append(number, '-', '0')
	} else {
		number = strconv.AppendInt(number, cents/100, 10)
	}
	number = append(number, '.', byte('0'+fraction/10), byte('0'+fraction%10))
	return number, nil
}
