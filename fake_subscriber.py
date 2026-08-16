# fake_subscriber.py
from http.server import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/always-200":
            self.send_response(200)
            self.end_headers()
        elif self.path == "/always-404":
            self.send_response(404)
            self.end_headers()
        elif self.path == "/always-500":
            self.send_response(500)
            self.end_headers()
        elif self.path == "/never-responds":
            import time
            time.sleep(10)  # longer than your 3s client timeout
            self.send_response(200)
            self.end_headers()
        print(f"hit: {self.path}")

print("Running on localhost:9000")
HTTPServer(("localhost", 9000), Handler).serve_forever()