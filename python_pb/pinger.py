import grpc
import pickle
import numpy as np 
import J2kResultsHub_pb2
import J2kResultsHub_pb2_grpc

def run():
    # Setup gRPC channel and stub
    channel = grpc.insecure_channel('localhost:50051')
    stub = J2kResultsHub_pb2_grpc.ResultsHubStub(channel)

    # Testing SayHello
    response = stub.SayHello(J2kResultsHub_pb2.HelloRequest(senderId='your_sender_id', message='your_message'))
    print("Pong server responded: " + response.message)

    # These variables can be any type!
    var1 = {'name': 'var1', 'data': [1, 2, 3]}
    list2 = [1, 2, 3, 4]
    var2 = np.array(list2)

    # Serialize data using pickle
    var1_bytes = pickle.dumps(var1)
    var2_bytes = pickle.dumps(var2)

    # ClaimCellFinished RPC
    cell_results = J2kResultsHub_pb2.VarResults(
        cellNumber=1,
        varResuls=[
            J2kResultsHub_pb2.VarResult(varName='var1', varType='dict', varBytes=var1_bytes, available=True),
            J2kResultsHub_pb2.VarResult(varName='var2', varType='str', varBytes=var2_bytes, available=True)
        ]
    )
    print("ClaimCellFinished RPC invoked.")
    stub.ClaimCellFinished(cell_results)
    print("ClaimCellFinished RPC returned.")

    # FetchVarResult RPC
    fetch_request = J2kResultsHub_pb2.FetchVarResultRequest(varName='var1', varAncestorCell=1)
    fetched_var = stub.FetchVarResult(fetch_request)

    # Deserialize the fetched data
    if fetched_var.available:
        deserialized_data = pickle.loads(fetched_var.varBytes)
        print("Fetched and deserialized data:", deserialized_data)
    else:
        print("Variable not available")

if __name__ == '__main__':
    run()
