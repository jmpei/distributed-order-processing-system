FROM golang:1.25-alpine AS builder
WORKDIR /app

# Copy shared module first (needed for replace directive at ../../shared)
COPY shared/ ./shared/

# Copy service deps and download (layer-cached separately from source)
COPY services/order-service/go.mod services/order-service/go.sum ./services/order-service/
RUN cd services/order-service && go mod download

# Copy service source and build
COPY services/order-service/ ./services/order-service/
WORKDIR /app/services/order-service
RUN CGO_ENABLED=0 GOOS=linux go build -o /order-service ./cmd

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /order-service /order-service
EXPOSE 8081
ENTRYPOINT ["/order-service"]
