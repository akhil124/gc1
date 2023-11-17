import grpc
from concurrent import futures
import route_pb2
import route_pb2_grpc
import csv
import os
import pandas as pd

class RouteService(route_pb2_grpc.RouteServiceServicer):
    def upload(self, request_iterator, context):
        file = "./server/base_file.csv"
        with open(file, "wb") as csv_file:
            for route in request_iterator:
                payload = route.payload
                csv_file.write(payload)
        input_file = './server/base_file.csv'
        output_file_2013 = './csv/2013.csv'
        output_file_2014 = './csv/2014.csv'
        output_file_2015 = './csv/2015.csv'
        output_file_2016 = './csv/2016.csv'
        output_file_2017 = './csv/2017.csv'
        output_file_2018 = './csv/2018.csv'
        output_file_2019 = './csv/2019.csv'
        output_file_2020 = './csv/2020.csv'
        output_file_2021 = './csv/2021.csv'
        output_file_2022 = './csv/2022.csv'
        print("split started")
        with open(input_file, 'r') as file:
            reader = csv.DictReader(file)
            data_2013 = []
            data_2014 = []
            data_2015 = []
            data_2016 = []
            data_2017 = []
            data_2018 = []
            data_2019 = []
            data_2020 = []
            data_2021 = []
            data_2022 = []
            for row in reader:
                issue_date = row['Issue Date']
                # Extracting the year from the date
                list_years=[]
                year = issue_date.split('/')[2]
                if year == '2013':
                    data_2013.append(row)
                    list_years.append(year)
                elif year == '2014':
                    data_2014.append(row)
                    list_years.append(year)
                elif year == '2015':
                    data_2015.append(row)
                    list_years.append(year)
                elif year == '2016':
                    data_2016.append(row)
                    list_years.append(year)
                elif year == '2017':
                    data_2017.append(row)
                    list_years.append(year)
                elif year == '2018':
                    data_2018.append(row)
                    list_years.append(year)
                elif year == '2019':
                    data_2019.append(row)
                    list_years.append(year)
                elif year == '2020':
                    data_2020.append(row)
                    list_years.append(year)
                elif year == '2021':
                    data_2021.append(row)
                    list_years.append(year)
                elif year == '2022':
                    data_2022.append(row)

        with open(output_file_2013, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2013)
        with open(output_file_2014, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2014)
        with open(output_file_2015, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2015)
        with open(output_file_2016, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2016)
        with open(output_file_2017, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2017)
        with open(output_file_2018, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2018)
        with open(output_file_2019, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2019)
        with open(output_file_2020, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2020)
        with open(output_file_2021, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2021)
        with open(output_file_2022, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(data_2022)
        print("Splitting of the CSV file is complete.")
        for i in list_years:
            print(i)
            if(i=="2013" or i=="2014"):
                channel = grpc.insecure_channel("localhost:50057")
                stub = route_pb2_grpc.RouteServiceStub(channel)
                print("sending the CSV file to sub server")
                def generate_routes():
                    with open(f"./csv/{int(i)}.csv", "rb") as csv_file:
                        while True:
                            data = csv_file.read(4096)  # Adjust the chunk size as needed
                            if not data:
                                break
                            route = route_pb2.Route(payload=data)
                            yield route

                routes = generate_routes()
                response = stub.upload(routes)
                print("File uploaded successfully to the sub server1")
            if(i=="2015" or i=="2016"):
                channel = grpc.insecure_channel("localhost:50058")
                stub = route_pb2_grpc.RouteServiceStub(channel)
                print("sending the CSV file to sub server")
                def generate_routes():
                    with open(f"./csv/{int(i)}.csv", "rb") as csv_file:
                        while True:
                            data = csv_file.read(4096)  # Adjust the chunk size as needed
                            if not data:
                                break
                            route = route_pb2.Route(payload=data)
                            yield route

                routes = generate_routes()
                response = stub.upload(routes)
                print("File uploaded successfully to the sub server1")
            if(i=="2017" or i=="2018"):
                channel = grpc.insecure_channel("localhost:50059")
                stub = route_pb2_grpc.RouteServiceStub(channel)
                print("sending the CSV file to sub server")
                def generate_routes():
                    with open(f"./csv/{int(i)}.csv", "rb") as csv_file:
                        while True:
                            data = csv_file.read(4096)  # Adjust the chunk size as needed
                            if not data:
                                break
                            route = route_pb2.Route(payload=data)
                            yield route

                routes = generate_routes()
                response = stub.upload(routes)
                print("File uploaded successfully to the sub server1")
            if(i=="2019" or i=="2020"):
                channel = grpc.insecure_channel("localhost:50060")
                stub = route_pb2_grpc.RouteServiceStub(channel)
                print("sending the CSV file to sub server")
                def generate_routes():
                    with open(f"./csv/{int(i)}.csv", "rb") as csv_file:
                        while True:
                            data = csv_file.read(4096)  # Adjust the chunk size as needed
                            if not data:
                                break
                            route = route_pb2.Route(payload=data)
                            yield route

                routes = generate_routes()
                response = stub.upload(routes)
                print("File uploaded successfully to the sub server1")
            if(i=="2021" or i=="2022"):
                channel = grpc.insecure_channel("localhost:50061")
                stub = route_pb2_grpc.RouteServiceStub(channel)
                print("sending the CSV file to sub server")
                def generate_routes():
                    with open(f"./csv/{int(i)}.csv", "rb") as csv_file:
                        while True:
                            data = csv_file.read(4096)  # Adjust the chunk size as needed
                            if not data:
                                break
                            route = route_pb2.Route(payload=data)
                            yield route

                routes = generate_routes()
                response = stub.upload(routes)
                print("File uploaded successfully to the sub server1")
            
        return route_pb2.Route()

    def query(self, request, context):
        channel = grpc.insecure_channel("localhost:50055")
        stub = route_pb2_grpc.RouteServiceStub(channel)
        request = route_pb2.QueryRequest(issue_date=request.issue_date.encode())  # Convert issue date to bytes

        try:
            # Make the gRPC request to the server
            response = stub.query(request)
            print("hello")
            # Store the received CSV file
            with open("./server_res/new.csv", "wb") as csv_file:
                csv_file.write(response.payload)

            print("CSV file received and stored.")
        except grpc.RpcError as e:
            print(f"An error occurred: {e}")
        if os.path.exists("./server_res/new.csv"):
            with open("./server_res/new.csv", "rb") as file:
                payload = file.read()
                return route_pb2.Route(payload=payload)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    route_pb2_grpc.add_RouteServiceServicer_to_server(RouteService(), server)
    server.add_insecure_port("localhost:50052")
    server.start()
    print("Main server started.")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
