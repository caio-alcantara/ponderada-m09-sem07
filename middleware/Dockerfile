FROM golang:1.22-alpine AS build

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
RUN go build -o middleware .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /app/middleware .
CMD ["./middleware"]
