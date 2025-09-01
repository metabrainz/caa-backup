#!/usr/bin/env python3
# monitor.py

import threading
import http.server
import json
import socketserver
import time

# Define a default port for the web server to listen on.
DEFAULT_PORT = 8000

# Create a custom ThreadingTCPServer that can hold a reference to the downloader object.
class CustomThreadingTCPServer(socketserver.ThreadingTCPServer):
    """
    A custom TCP server that allows passing a downloader instance to the
    request handler. This is necessary because the standard server does not
    provide a mechanism to pass custom arguments to the handler's constructor.
    """
    def __init__(self, server_address, RequestHandlerClass, downloader, bind_and_activate=True):
        self.downloader = downloader
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)

# The handler class that will process incoming HTTP requests.
# It inherits from http.server.BaseHTTPRequestHandler to handle the requests.
class CAAServiceMonitorHandler(http.server.BaseHTTPRequestHandler):
    """
    A custom HTTP request handler for the CAAServiceMonitor web server.
    """
    # Override the log_message method to prevent the server from printing
    # a line for each request.
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        """
        Handle GET requests. This method is called by the server for every GET request.
        """
        # Check if the requested path is '/status'.
        if self.path == '/status':
            try:
                # Access the downloader instance through the server object.
                response_data = self.server.downloader.stats()
                # Respond with a 200 OK status code.
                self.send_response(200)
                
            except AttributeError:
                # If the downloader or stats() method is not found, return an error.
                self.send_response(500)
                response_data = {"error": "Downloader instance or stats() method is not available."}
            except Exception as e:
                # Catch any other exceptions during the stats call.
                self.send_response(500)
                response_data = {"error": f"An error occurred while getting stats: {str(e)}"}
                
            # Set the Content-type header to 'application/json' so the client knows
            # to expect JSON data.
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            # Convert the Python dictionary to a JSON string and encode it to bytes.
            response_bytes = json.dumps(response_data, indent=4).encode('utf-8')
            # Write the response to the client.
            self.wfile.write(response_bytes)

        else:
            # If the path is not '/status', respond with a 404 Not Found error.
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"404 Not Found")

# The main class that runs the web server in a separate thread.
# It inherits from threading.Thread.
class CAAServiceMonitor(threading.Thread):
    """
    A threaded web server to monitor a service.
    It provides a simple HTTP endpoint for status checks.
    """
    def __init__(self, downloader, host='0.0.0.0', port=DEFAULT_PORT):
        """
        Initialize the thread and the server parameters.
        
        Args:
            downloader (object): An instance of the downloader service with a stats() method.
            host (str): The hostname to bind to.
            port (int): The port to listen on.
        """
        # Call the parent class's constructor.
        super().__init__()
        # Set the server address.
        self.server_address = (host, port)
        # Create the HTTP server instance using the custom server class.
        # This is where we pass the downloader instance to the server.
        self.httpd = CustomThreadingTCPServer(self.server_address, CAAServiceMonitorHandler, downloader)
        # A flag to control the shutdown of the server.
        self._is_running = True
        self.downloader = downloader # This is now for potential use in other monitor methods.

    def run(self):
        """
        The main method for the thread. This is where the server is started.
        """
        print(f"Starting CAAServiceMonitor on {self.server_address[0]}:{self.server_address[1]}...")
        # Serve requests until the shutdown method is called.
        self.httpd.serve_forever()
        print("CAAServiceMonitor shut down.")

    def shutdown(self):
        """
        Shut down the web server gracefully.
        """
        self.httpd.shutdown()
        self.httpd.server_close()
        self._is_running = False
