#!/usr/bin/env python3

import threading
import http.server
import json
import socketserver
import time

DEFAULT_PORT = 8000

# The handler class that will process incoming HTTP requests.
# It inherits from http.server.BaseHTTPRequestHandler to handle the requests.
class CAAServiceMonitorHandler(http.server.BaseHTTPRequestHandler):
    """
    A custom HTTP request handler for the CAAServiceMonitor web server.
    """
    def do_GET(self):
        """
        Handle GET requests. This method is called by the server for every GET request.
        """
        # Check if the requested path is '/status'.
        if self.path == '/status':
            try:
                response_data = self.downloader.stats()
                # Respond with a 200 OK status code.
                self.send_response(200)
                
            except ImportError:
                # If the caa_downloader module is not found, return an error.
                self.send_response(500)
                response_data = {"error": "caa_downloader module not found or stat() function is missing"}
                
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
    def __init__(self, downloader, host='localhost', port=DEFAULT_PORT):
        """
        Initialize the thread and the server parameters.
        
        Args:
            host (str): The hostname to bind to.
            port (int): The port to listen on.
        """
        # Call the parent class's constructor.
        super().__init__()
        # Set the server address.
        self.server_address = (host, port)
        # Create the HTTP server instance.
        self.httpd = socketserver.TCPServer(self.server_address, CAAServiceMonitorHandler)
        # A flag to control the shutdown of the server.
        self._is_running = True
        self.downloader = downloader

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