FROM golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build

WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o /bridgewatch ./cmd/bridgewatch

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /bridgewatch /bridgewatch
USER 65532:65532
ENV BRIDGEWATCH_LISTEN_ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["/bridgewatch"]
