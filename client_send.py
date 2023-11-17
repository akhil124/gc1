import grpc
import route_pb2
import route_pb2_grpc

def upload_csv_to_server(csv_file_path):
    channel = grpc.insecure_channel("localhost:50052")
    stub = route_pb2_grpc.RouteServiceStub(channel)

    def generate_routes():
        with open(csv_file_path, "rb") as csv_file:
            while True:
                data = csv_file.read(1024)  # Adjust the chunk size as needed
                if not data:
                    break
                route = route_pb2.Route(payload=data)
                yield route

    routes = generate_routes()
    response = stub.upload(routes)
    print("File uploaded successfully.")

if __name__ == "__main__":
    csv_file_path = "./2013.csv"  # Path to the 2GB CSV file
    upload_csv_to_server(csv_file_path)

