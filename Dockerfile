# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Builder
#   Compiles a statically-linked binary so the runtime image needs no Go toolchain.
# ─────────────────────────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

# git  — some go modules resolve via VCS at build time
# ca-certificates — needed for go mod download over HTTPS
RUN apk add --no-cache git ca-certificates

WORKDIR /build

# Download dependencies in a separate layer so they are cached
# independently of source-code changes.
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy the full source tree (respects .dockerignore)
COPY . .

# Build a stripped, statically-linked binary.
#   CGO_ENABLED=0  → no libc dependency; runs in Alpine or scratch
#   -trimpath      → remove host path info from the binary
#   -ldflags "-w -s" → strip DWARF debug info and symbol table (~30% smaller)
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-w -s" \
    -o /app/geonera-ingestion \
    .

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Runtime
#   Minimal Alpine image: only the binary + its static assets.
# ─────────────────────────────────────────────────────────────────────────────
FROM alpine:3.21

# ca-certificates — for outbound HTTPS to Dukascopy datafeed and Cloudflare R2
# tzdata          — ensures time.LoadLocation("UTC") resolves correctly
RUN apk add --no-cache ca-certificates tzdata && \
    # Create a dedicated non-root user/group for the process \
    addgroup -S geonera && \
    adduser  -S -G geonera -h /app -s /sbin/nologin geonera

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/geonera-ingestion ./geonera-ingestion

# Copy the OpenAPI spec — served at GET /openapi.yaml by the Fiber app
COPY openapi.yaml ./openapi.yaml

# Copy the SQL init script — applied as migration v1 by RunMigrations() at startup.
# os.ReadFile("database/init.sql") uses a relative path, so the file must be
# at /app/database/init.sql inside the container.
COPY database/ ./database/

# Drop privileges: run as the non-root geonera user
USER geonera

# Document the default port; override at runtime with APP_PORT=<port>
EXPOSE 8080

# Default timezone — all timestamps in the ingestion pipeline are UTC
ENV TZ=UTC

ENTRYPOINT ["/app/geonera-ingestion"]
