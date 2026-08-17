package main

import (
	"net/http"

	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	logger.Info("payment-processor listening", zap.String("address", ":8080"))
	if err := http.ListenAndServe(":8080", mux); err != nil {
		logger.Fatal("payment-processor stopped", zap.Error(err))
	}
}
