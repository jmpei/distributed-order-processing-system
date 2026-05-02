FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY shared/ ./shared/

COPY services/payment-service/go.mod services/payment-service/go.sum ./services/payment-service/
RUN cd services/payment-service && go mod download

COPY services/payment-service/ ./services/payment-service/
WORKDIR /app/services/payment-service
RUN CGO_ENABLED=0 GOOS=linux go build -o /payment-service ./cmd

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /payment-service /payment-service
EXPOSE 8083
ENTRYPOINT ["/payment-service"]
