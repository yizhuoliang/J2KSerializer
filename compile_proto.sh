python3 -m grpc_tools.protoc -I./ --python_out=./python_pb/ --grpc_python_out=./python_pb/ ./J2kResultsHub.proto
protoc --go_out=. --go-grpc_out=. J2KResultsHub.proto