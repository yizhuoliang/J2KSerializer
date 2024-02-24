# Start from a base image with Go installed
FROM golang:1.17 as builder

# Set the working directory inside the container
WORKDIR /app

# Copy the Go Modules manifests
COPY go.mod go.sum ./
# Download Go modules
RUN go mod download

# Copy the rest of your project's sources
COPY . .

# Build your program
# -o specifies the output binary name, you can choose any name
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o resultsHub .

# Use a minimal base image to create the final stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/resultsHub .

# Expose port (if your app listens on a port)
EXPOSE 50051

# Command to run the executable
CMD ["./resultsHub"]
