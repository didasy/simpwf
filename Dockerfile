# Multi-stage build for the app runtime image. The app never migrates; the
# dedicated Atlas container in docker-compose.yml owns the schema.

# -- build stage --------------------------------------------------------------
# Go minor must match go.mod (drift guard in ci.yml enforces this).
FROM golang:1.27-alpine@sha256:738d1cf061836894ff6bb8c33881080ac66de8cf0586615012a0c8f592649cfa AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/app

# -- runtime stage ------------------------------------------------------------
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc AS app
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 app \
    && adduser -S -D -u 10001 -G app app
COPY --from=build /out/app /usr/local/bin/app
USER app
EXPOSE 8080
ENTRYPOINT ["app"]
