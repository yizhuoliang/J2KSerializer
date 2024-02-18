import grpc
import J2kSerializer_pb2
import J2kSerializer_pb2_grpc

def run():
    # Assuming your server is running on localhost at port 50051
    channel = grpc.insecure_channel('localhost:50051')
    stub = J2kSerializer_pb2_grpc.BrokerServiceStub(channel)

    # Replace 'your_sender_id' and 'your_message' with appropriate values
    response = stub.SayHello(J2kSerializer_pb2.HelloRequest(senderId='your_sender_id', message='your_message'))
    print("Pong server responded: " + response.message)

if __name__ == '__main__':
    run()
