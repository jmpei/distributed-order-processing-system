FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY shared/ ./shared/

COPY services/inventory-service/go.mod services/inventory-service/go.sum ./services/inventory-service/
RUN cd services/inventory-service && go mod download

COPY services/inventory-service/ ./services/inventory-service/
WORKDIR /app/services/inventory-service
RUN CGO_ENABLED=0 GOOS=linux go build -o /inventory-service ./cmd

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /inventory-service /inventory-service
EXPOSE 8082
ENTRYPOINT ["/inventory-service"]
