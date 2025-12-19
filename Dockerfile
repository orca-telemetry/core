FROM gcr.io/distroless/static:nonroot

RUN apk --no-cache add ca-certificates

WORKDIR /app

# copy the pre-built binary from GoReleaser
COPY orca /app/orca

ENTRYPOINT ["/app/orca"]
