FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/geo ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/geo /geo
EXPOSE 8081 9091/udp
ENTRYPOINT ["/geo", "-http", ":8081", "-udp", ":9091"]
