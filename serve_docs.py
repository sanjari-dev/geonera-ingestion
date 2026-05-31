"""Minimal docs server: serves /docs (Scalar UI) and /openapi.yaml."""
import http.server
import os

PORT = 8080
OPENAPI_PATH = os.path.join(os.path.dirname(__file__), "openapi.yaml")

DOCS_HTML = """\
<!doctype html>
<html>
  <head>
    <title>Geonera Ingestion API Docs</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script
      id="api-reference"
      data-url="/openapi.yaml"
      src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>"""


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/docs"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(DOCS_HTML.encode())
        elif self.path == "/openapi.yaml":
            with open(OPENAPI_PATH, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "application/yaml")
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        print(fmt % args)


if __name__ == "__main__":
    with http.server.HTTPServer(("", PORT), Handler) as httpd:
        print(f"Docs server running at http://localhost:{PORT}/docs")
        httpd.serve_forever()
