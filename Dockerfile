FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/telegram-s3 ./cmd/telegram-s3

FROM alpine:3.22

RUN adduser -D -h /app appuser
WORKDIR /app
COPY --from=build /out/telegram-s3 /usr/local/bin/telegram-s3

ENV LISTEN_ADDR=:9000
EXPOSE 9000

USER appuser
CMD ["telegram-s3"]
