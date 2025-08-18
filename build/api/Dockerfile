FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/api ./services/api

FROM alpine:3.19
COPY --from=build /bin/api /api
EXPOSE 8080 9090
ENTRYPOINT ["/api"]
