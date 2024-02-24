import grpc
import pickle
import time
import numpy as np 
import J2kResultsHub_pb2
import J2kResultsHub_pb2_grpc

class ResultsHubSubmission:
    def __init__(self, cell_number):
        import grpc
        import pickle
        import time
        self.cell_number = cell_number
        self.var_results = J2kResultsHub_pb2.VarResults(cellNumber=cell_number)

    def addVar(self, var_name, var):
        var_bytes = pickle.dumps(var)
        var_result = J2kResultsHub_pb2.VarResult(
            varName=var_name, varType=str(type(var)), varBytes=var_bytes, available=True
        )
        self.var_results.varResuls.append(var_result)

    def submit(self):
        # keep retrying until success
        while True:
            try:
                with grpc.insecure_channel('localhost:50051') as channel:
                    stub = J2kResultsHub_pb2_grpc.ResultsHubStub(channel)
                    stub.ClaimCellFinished(self.var_results)
                    print("Submission RPC returned successfully.")
                    return
            except grpc.RpcError as e:
                print(f"Submission failed: {e}, retrying...")
                time.sleep(2)  # Wait for 2 seconds before retrying
        
        print("RPC failed after maximum retries.")

def fetchVarResult(varName, varAncestorCell):
    while True:
        try:
            with grpc.insecure_channel('localhost:50051') as channel:
                stub = J2kResultsHub_pb2_grpc.ResultsHubStub(channel)
                fetch_request = J2kResultsHub_pb2.FetchVarResultRequest(
                    varName=varName, varAncestorCell=varAncestorCell
                )
                fetched_var = stub.FetchVarResult(fetch_request)
                return pickle.loads(fetched_var.varBytes)
        except grpc.RpcError as e:
            print(f"Fetching var {varName} failed: {e}, retrying in 2 seconds...")
            time.sleep(2)

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

    # Using the ResultsHubSubmission class
    submission = ResultsHubSubmission(cell_number=1)
    submission.addVar('var1', var1)
    submission.addVar('var2', var2)
    submission.submit()
    print("Submission Success.")

    # Retrieve the variable
    fetched_var = fetchVarResult('var1', varAncestorCell=1)
    print(f"fetched variable: '{fetched_var}'")

if __name__ == '__main__':
    run()
