FROM gcr.io/distroless/static-debian12:nonroot

COPY orca /app/orca

ENTRYPOINT ["/app/orca"]
