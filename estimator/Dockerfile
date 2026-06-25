FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# modernc.org/sqlite is pure Go, so we can build static (CGO off).
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/estimator .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && mkdir -p /data
COPY --from=builder /app/estimator /estimator
ENV DB_PATH=/data/auctions.db
EXPOSE 8080
ENTRYPOINT ["/estimator"]
