from concurrent import futures
import os
import io
from collections import defaultdict
import logging
from pathlib import Path

import grpc
from grpc_reflection.v1alpha import reflection

import dataverse_pb2 as service
import dataverse_pb2_grpc as rpc

# Global sets to store uploaded file paths and connected hosts
cache = set()
connectedHosts = set()


class ImageServiceServer(rpc.GreeterServicer):
    def __init__(self):
        self.images = defaultdict(io.BytesIO)
        logging.info("Successfully created the images store")

        cache_file = Path("./cache.txt")
        if cache_file.is_file():
            with open(cache_file, 'r') as filehandle:
                for line in filehandle:
                    cache.add(line.strip())
            logging.info("Successfully initialized the cache")

    def Upload(self, request_iterator, context):
        for request in request_iterator:
            if request.StatusCode == service.ImageUploadStatusCode.InProgress:
                logging.info(f'> {request.Id} - receiving chunk')
                self.images[request.Id].write(request.Content)
                result = service.ImageUploadResponse(
                    Id=request.Id,
                    StatusCode=service.ImageUploadStatusCode.Ok,
                    Message='Receiving...',
                    Username=request.Username
                )
                yield result  # Send intermediate response (optional)

            elif request.StatusCode == service.ImageUploadStatusCode.Ok:
                logging.info(f'> {request.Id} - transfer complete, storing')
                image = self.images[request.Id].getvalue()
                path = os.path.join("data", request.Username)
                os.makedirs(path, exist_ok=True)

                file_path = os.path.join(path, request.Id)
                with open(file_path, 'wb') as f:
                    f.write(image)

                del self.images[request.Id]
                cache.add(file_path)

                logging.info(f'> {request.Id} - stored and removed from memory')
                yield service.ImageUploadResponse(
                    Id=request.Id,
                    StatusCode=service.ImageUploadStatusCode.Ok,
                    Message="Uploaded successfully",
                    Username=request.Username,
                    nodeConnections=list(connectedHosts)
                )
                break  # Done after final OK

    def Search(self, request, context):
        uname = request.Username
        fname = request.Filename
        file_path = os.path.join("data", uname, fname)

        if file_path in cache:
            logging.info(f'> {file_path} found in local cache')
            with open(file_path, 'rb') as f:
                content = f.read()
            return service.SearchResponse(
                found="YES",
                Content=content,
                File=fname
            )

        logging.info(f'> {file_path} not found. Asking connected nodes...')
        return service.SearchResponse(
            found="NO",
            nodeConnections=list(connectedHosts)
        )

    def Config(self, request, context):
        server_address = request.Server
        connectedHosts.add(server_address)
        logging.info(f'> Configured: {server_address}')
        return service.ConfigResponse(Status=f"Server {server_address} added")

    def Relocate(self, request, context):
        uname = request.Username
        fname = request.Filename

        self.images[fname].write(request.Content)
        image = self.images[fname].getvalue()

        path = os.path.join("data", uname)
        os.makedirs(path, exist_ok=True)
        file_path = os.path.join(path, fname)

        with open(file_path, 'wb') as f:
            f.write(image)

        del self.images[fname]
        cache.add(file_path)

        logging.info(f'> Relocated: {file_path}')
        return service.RelocateResponse(status="Relocated successfully")


def serve():
    logging.basicConfig(level=logging.INFO)
    port = 3001

    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rpc.add_GreeterServicer_to_server(ImageServiceServer(), grpc_server)

    SERVICE_NAMES = (
        service.DESCRIPTOR.services_by_name['Greeter'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, grpc_server)

    grpc_server.add_insecure_port(f'[::]:{port}')
    logging.info(f'Starting gRPC server on port {port}...')
    grpc_server.start()

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        with open('cache.txt', 'w') as filehandle:
            for file in cache:
                filehandle.write(f"{file}\n")
        logging.info("Server shutting down. Cache saved.")


if __name__ == '__main__':
    serve()
