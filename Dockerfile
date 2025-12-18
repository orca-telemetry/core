FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the pre-built binary from GoReleaser
COPY orca /app/orca

ENTRYPOINT ["/app/orca"]
