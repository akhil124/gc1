from datetime import datetime, timedelta
import grpc
from concurrent import futures
import route_pb2
import route_pb2_grpc
import csv
import os
import pandas as pd

class RouteService(route_pb2_grpc.RouteServiceServicer):
    def __init__(self):
        self.servers = [
            "localhost:50057",  # Server 1 address (Leader)
            "localhost:50058",  # Server 2 address
            "localhost:50059",    # Server 3 address
            "localhost:50060",
            "localhost:50061"
        ]
        self.csv_directory = "csv_files_20"
        self.header_written = False
        

    def upload(self, request_iterator, context):
        file = "./sub_server/client_file.csv"
        print("got the csv file")
        with open(file, "wb") as csv_file:
            for route in request_iterator:
                payload = route.payload
                csv_file.write(payload)
        df = pd.read_csv(file, low_memory=False)
        cols = df.columns

        # Create the 'csv_files' folder if it doesn't exist
        folder_name = "./csv_files_20"
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        for i in set(df['Issue Date']):
            j = i.split("/")
            year = int(j[2])
            filename = f"{j[2]}-{j[0]}-{j[1]}.csv"
            file_path = os.path.join(folder_name, filename)
            print(file_path)
            df.loc[df['Issue Date'] == i].to_csv(file_path, index=False, columns=cols)
        return route_pb2.Route()

    def query(self, request, context):
        print("hello")
        issue_date = request.issue_date
        if len(issue_date)==21:
            start_date_str, end_date_str = issue_date.split(':')
            print("split")
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            start_date=start_date.date()
            end_date=end_date.date()
        else:
            start_date_str=issue_date
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            start_date=start_date.date()
            end_date=None
            print("pr")
        results = []
        print("proccess")
        print(start_date)
        current_date = start_date
        print(current_date)
        if end_date==None:
            print("end is none")
            filename = f"{self.csv_directory}/{current_date.strftime('%Y-%m-%d')}.csv"
            if os.path.exists(filename):
                print("found")
               
                print("fileName")
                with open(filename, "rb") as file:
                    payload = file.read()
                    print("payload")
                    with open(filename, "rb") as file:
                        payload = file.read()
                        print("File found and sent")
                        return route_pb2.Route(payload=payload)
        print("help2")
        current_server =3
        filename = f"{self.csv_directory}/{current_date.strftime('%Y-%m-%d')}.csv"
        if os.path.exists(filename):
            while (current_date<=end_date):
                print("hello")
                print(current_date.strftime('%Y-%m-%d'))
                filename = f"{self.csv_directory}/{current_date.strftime('%Y-%m-%d')}.csv"
                print(filename)
                if os.path.exists(filename):
                    print("found")
                    with open(filename, "rb") as file:
                        payload = file.read()
                        if not self.header_written:
                            results.append(payload)  # Include the header for the first file
                            self.header_written = True
                            print("found1")
                        else:
                            results.append(payload[bytes(payload).index(b"\n")+1:])
                print("appended")
                current_date +=timedelta(days=1)
                print(current_date)
        print("exit")
        
        if results:
            print("entered result")
            # Concatenate all the file payloads into a single payload
            final_payload = b"".join(results)
            return route_pb2.Route(payload=final_payload)
        # Forward the request to the next server in the ring
        print("hello12")
        next_server = self.servers[3]
        with grpc.insecure_channel(next_server) as channel:
            stub = route_pb2_grpc.RouteServiceStub(channel)
            response = stub.query(route_pb2.QueryRequest(issue_date=request.issue_date.encode()))
            if response.payload:
                return response

        # If no files are found after completing the loop, terminate the request
        error_message = f"No CSV files found in the specified date range"
        context.set_details(error_message)
        context.set_code(grpc.StatusCode.NOT_FOUND)
        return route_pb2.Route()
def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    route_pb2_grpc.add_RouteServiceServicer_to_server(RouteService(), server)
    server.add_insecure_port("localhost:50059")
    server.start()
    print("Server 2 started.")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
