FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/scheduler ./services/scheduler

FROM alpine:3.19
COPY --from=build /bin/scheduler /scheduler
EXPOSE 8081
ENTRYPOINT ["/scheduler"]
