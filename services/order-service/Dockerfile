FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /order-service .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /order-service /order-service
EXPOSE 8081
ENTRYPOINT ["/order-service"]
