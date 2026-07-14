# Build stage
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o /gtsdb .

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

COPY --from=builder /gtsdb /usr/local/bin/gtsdb

RUN mkdir -p /data /etc/gtsdb

EXPOSE 5555 5556

VOLUME ["/data"]

ENTRYPOINT ["/docker-entrypoint.sh"]
