FROM golang:1.16 AS builder
WORKDIR /go/src/inverse-tcp-tunnel
COPY . .
RUN go build -o /go/bin/server ./cmd/server && go build -o /go/bin/client ./cmd/client

FROM gcr.io/distroless/base-debian10
COPY --from=builder /go/bin/server /bin
COPY --from=builder /go/bin/client /bin
