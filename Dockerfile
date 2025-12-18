FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY core/go.mod core/go.sum ./

RUN go mod tidy

COPY core/ ./

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-v -w" -o orca .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/orca .

ENTRYPOINT ["./orca"]

