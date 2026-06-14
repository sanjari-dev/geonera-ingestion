#!/usr/bin/env node
/**
 * Flow diagram viewer server.
 *
 * Routes:
 *   GET /render/<format>/<file.puml>  — read file, encode, proxy to plantuml.com, return image
 *   GET /viewer/                      — static viewer app (index.html, app.js, style.css)
 *   GET /<file>.puml                  — raw .puml source (for source pane)
 *
 * Usage:
 *   node server.js [port]
 *   Then open http://localhost:<port>/viewer/
 */

const http  = require("http");
const https = require("https");
const zlib  = require("zlib");
const fs    = require("fs");
const path  = require("path");

const PORT = Number(process.argv[2]) || Number(process.env.PORT) || 4040;
const HOST = process.env.HOST || "0.0.0.0";
const ROOT = path.resolve(__dirname, ".."); // docs/flows

const PLANTUML_HOST = "www.plantuml.com";

// PlantUML 6-bit alphabet encoding (same as plantuml.com uses)
const ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
function encodePlantUml(buf) {
  let out = "";
  for (let i = 0; i < buf.length; i += 3) {
    const b1 = buf[i], b2 = buf[i + 1] || 0, b3 = buf[i + 2] || 0;
    out += ALPHABET[b1 >> 2];
    out += ALPHABET[((b1 & 0x3) << 4) | (b2 >> 4)];
    out += ALPHABET[((b2 & 0xf) << 2) | (b3 >> 6)];
    out += ALPHABET[b3 & 0x3f];
  }
  return out;
}

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js":   "text/javascript; charset=utf-8",
  ".css":  "text/css; charset=utf-8",
  ".puml": "text/plain; charset=utf-8",
  ".svg":  "image/svg+xml",
  ".png":  "image/png",
  ".json": "application/json; charset=utf-8",
  ".txt":  "text/plain; charset=utf-8",
  ".md":   "text/markdown; charset=utf-8",
};

function proxyToPlantUml(format, token, res) {
  const upstreamPath = `/plantuml/${format}/${token}`;
  const req = https.request(
    { hostname: PLANTUML_HOST, path: upstreamPath, method: "GET",
      headers: { "User-Agent": "geonera-flow-viewer/1.0" } },
    (upstream) => {
      res.writeHead(upstream.statusCode, {
        "Content-Type":  upstream.headers["content-type"] || "application/octet-stream",
        "Cache-Control": "public, max-age=300",
        "Access-Control-Allow-Origin": "*",
      });
      upstream.pipe(res);
    }
  );
  req.on("error", (e) => { res.writeHead(502); res.end("plantuml.com error: " + e.message); });
  req.end();
}

const server = http.createServer((req, res) => {
  let urlPath = decodeURIComponent(req.url.split("?")[0]);

  // ── /render/<format>/<filename> ─────────────────────────────────────────
  // Server reads the .puml file, encodes, and proxies to plantuml.com.
  // Keeps the browser URL short and avoids client-side compression quirks.
  const renderMatch = urlPath.match(/^\/render\/(svg|png)\/(.+\.puml)$/);
  if (renderMatch) {
    const [, format, relFile] = renderMatch;
    const filePath = path.join(ROOT, relFile);
    if (!filePath.startsWith(ROOT)) { res.writeHead(403); return res.end("Forbidden"); }

    fs.readFile(filePath, "utf8", (err, source) => {
      if (err) { res.writeHead(404); return res.end("Not found: " + relFile); }
      zlib.deflateRaw(Buffer.from(source, "utf8"), (zerr, compressed) => {
        if (zerr) { res.writeHead(500); return res.end("Compression error"); }
        proxyToPlantUml(format, encodePlantUml(compressed), res);
      });
    });
    return;
  }

  // ── Static files ─────────────────────────────────────────────────────────
  if (urlPath === "/") urlPath = "/viewer/";
  if (urlPath.endsWith("/")) urlPath += "index.html";

  const filePath = path.join(ROOT, urlPath);
  if (!filePath.startsWith(ROOT)) { res.writeHead(403); return res.end("Forbidden"); }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); return res.end(`Not found: ${urlPath}`); }
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
  console.log(`  Serving:   ${ROOT}`);
  console.log(`  Listening: http://${HOST}:${PORT}/`);
  console.log(`  Open:      http://localhost:${PORT}/viewer/\n`);
  console.log(`  Press Ctrl+C to stop.\n`);
});

for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    console.log(`\nReceived ${sig}, shutting down…`);
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 3000).unref();
  });
}
