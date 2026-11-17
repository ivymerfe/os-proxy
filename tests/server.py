import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIZE = 10 * 1024 * 1024
RATE = int(float(sys.argv[2]) * 1024 * 1024) if len(sys.argv) > 2 else 2.5 * 1024 * 1024
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
CHUNK = 32 * 1024


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Length", str(SIZE))
        self.send_header("Connection", "close")
        self.end_headers()
        sent = 0
        start = time.monotonic()
        try:
            while sent < SIZE:
                n = min(CHUNK, SIZE - sent)
                self.wfile.write(
                    bytes((sent + i) % 251 for i in range(0)) or b"\x5a" * n
                )
                sent += n
                delay = start + sent / RATE - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args, **kwargs):
        pass


ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
