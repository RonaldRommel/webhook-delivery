"""
Fake subscriber server for testing webhook retry logic.

Endpoints:
  /always-200              -> always succeeds
  /always-404              -> always terminal failure (4xx)
  /always-500              -> always fails (never recovers, will end up "dead")
  /flaky/<id>/N              -> fails N times, then succeeds forever after
                                 e.g. /flaky/a/1 fails once then succeeds
  /flaky-random?p=0.4       -> fails with independent probability p on every
                                 single request (default p=0.4 if omitted).
                                 Used for the statistical recovery-rate test.

Run:  python3 fake_subscriber.py
Logs every hit to stdout with a running count per path, so you can watch
attempts land in real time alongside your worker's logs.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import re
import random

fail_counts = {}

FLAKY_RE = re.compile(r"^/flaky/([^/]+)/(\d+)$")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # silence default HTTP logging, we print our own below

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/always-200":
            self._respond(200, path)
            return

        if path == "/always-404":
            self._respond(404, path)
            return

        if path == "/always-500":
            self._respond(500, path)
            return

        if path == "/flaky-random":
            qs = parse_qs(parsed.query)
            fail_prob = float(qs.get("p", ["0.4"])[0])
            if random.random() < fail_prob:
                self._respond(500, self.path, extra=f"(random fail, p={fail_prob})")
            else:
                self._respond(200, self.path, extra=f"(random success, p={fail_prob})")
            return

        m = FLAKY_RE.match(path)
        if m:
            key = m.group(1)
            fail_threshold = int(m.group(2))
            fail_counts[key] = fail_counts.get(key, 0) + 1
            attempt_num = fail_counts[key]
            if attempt_num <= fail_threshold:
                self._respond(500, path, extra=f"(attempt {attempt_num}, will fail until >{fail_threshold})")
            else:
                self._respond(200, path, extra=f"(attempt {attempt_num}, now recovering)")
            return

        self._respond(404, path, extra="(unknown path)")

    def _respond(self, code, path, extra=""):
        self.send_response(code)
        self.end_headers()
        print(f"[hit] {path} -> {code} {extra}")


if __name__ == "__main__":
    port = 9000
    print(f"fake subscriber server listening on :{port}")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()