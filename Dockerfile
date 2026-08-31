# Multi-stage build for the app runtime image. The app never migrates; the
# dedicated Atlas container in docker-compose.yml owns the schema.

# -- build stage --------------------------------------------------------------
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/app

# -- runtime stage ------------------------------------------------------------
FROM alpine:3.20 AS app
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 app \
    && adduser -S -D -u 10001 -G app app
COPY --from=build /out/app /usr/local/bin/app
USER app
EXPOSE 8080
ENTRYPOINT ["app"]
