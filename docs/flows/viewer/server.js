#!/usr/bin/env node
/**
 * Zero-dependency static file server for the flow diagram viewer.
 *
 * Why this exists: the viewer fetches the sibling `.puml` sources via
 * `fetch("../<file>.puml")`, which the browser blocks under the `file://`
 * origin (CORS). Serving the `docs/flows` folder over plain HTTP fixes that
 * — no npm install required, just plain Node's `http` + `fs`.
 *
 * Usage (from this `viewer` folder):
 *   node server.js [port]
 *
 * Then open:  http://localhost:<port>/viewer/
 *
 * (Root of the server is the parent `flows` folder, so both the viewer app
 * AND the .puml sources are reachable from the same origin.)
 */

const http = require("http");
const fs = require("fs");
const path = require("path");

// PORT resolution order: CLI arg (local dev convenience) -> $PORT env var
// (what Docker / most cloud platforms inject) -> 4040 default.
const PORT = Number(process.argv[2]) || Number(process.env.PORT) || 4040;
const HOST = process.env.HOST || "0.0.0.0";
const ROOT = path.resolve(__dirname, ".."); // docs/flows

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".puml": "text/plain; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".json": "application/json; charset=utf-8",
  ".txt": "text/plain; charset=utf-8",
  ".md": "text/markdown; charset=utf-8",
};

const server = http.createServer((req, res) => {
  let urlPath = decodeURIComponent(req.url.split("?")[0]);
  if (urlPath === "/") urlPath = "/viewer/";
  if (urlPath.endsWith("/")) urlPath += "index.html";

  const filePath = path.join(ROOT, urlPath);

  // Prevent path traversal outside ROOT.
  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403);
    return res.end("Forbidden");
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      return res.end(`Not found: ${urlPath}`);
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, {
      "Content-Type": MIME[ext] || "application/octet-stream",
      "Cache-Control": "no-cache",
      "Access-Control-Allow-Origin": "*",
    });
    res.end(data);
  });
});

server.listen(PORT, HOST, () => {
  console.log(`\nGeonera Ingestion — Flow Viewer`);
  console.log(`  Serving:  ${ROOT}`);
  console.log(`  Listening: http://${HOST}:${PORT}/`);
  console.log(`  Open:      http://localhost:${PORT}/viewer/\n`);
  console.log(`  Press Ctrl+C to stop.\n`);
});

// Graceful shutdown — important inside containers (SIGTERM on `docker stop`).
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    console.log(`\nReceived ${sig}, shutting down…`);
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 3000).unref();
  });
}
