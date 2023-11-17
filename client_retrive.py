import grpc
import route_pb2
import route_pb2_grpc

def retrieve_record():
    channel = grpc.insecure_channel("localhost:50052")  # Update with the correct server address
    stub = route_pb2_grpc.RouteServiceStub(channel)

    # Set the issue date to retrieve
    issue_date = "2013-06-09"  # Update with the desired issue date in the correct format

    # Prepare the request
    request = route_pb2.QueryRequest(issue_date=issue_date.encode())  # Convert issue date to bytes

    try:
        # Make the gRPC request to the server
        response = stub.query(request)
        print("hello")
        # Store the received CSV file
        with open("./new.csv", "wb") as csv_file:
            csv_file.write(response.payload)

        print("CSV file received and stored.")
    except grpc.RpcError as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    retrieve_record()
