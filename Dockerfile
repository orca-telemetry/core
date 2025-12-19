FROM gcr.io/distroless/static-debian12:nonroot

ARG TARGETPLATFORM

COPY ${TARGETPLATFORM}/orca /app/orca

ENTRYPOINT ["/app/orca"]
