FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /payment-api ./cmd/api

FROM alpine:3.21
COPY --from=build /payment-api /payment-api
EXPOSE 8080
ENTRYPOINT ["/payment-api"]
