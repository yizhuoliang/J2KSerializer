# # Start from a base image with Go installed
# FROM golang:1.17 as builder

# # Set the working directory inside the container
# WORKDIR /app

# # Copy the Go Modules manifests
# COPY go.mod .
# COPY go.sum .
# COPY resultsHub/main.go .
# # COPY J2KResultsHub_grpc.pb.go .
# # COPY J2KResultsHub.pb.go .

# RUN go build -o bin .

# ENTRYPOINT [ "/app/bin" ]

# # Expose port (if your app listens on a port)
# EXPOSE 50051

FROM golang:1.17-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

COPY . ./

RUN go build -o /bin/myapp resultsHub/main.go
RUN chmod +x /bin/myapp

EXPOSE 50051
CMD ["/bin/myapp"]