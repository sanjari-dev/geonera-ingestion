# Flow Diagram Viewer

An interactive, web-based viewer for the PlantUML flow diagrams in
`docs/flows/*.puml` — render, zoom, pan, and browse all 7 processes of the
Geonera Ingestion pipeline directly in your browser, **without** pasting
anything into https://editor.plantuml.com/.

## How it works

The viewer is a static HTML/CSS/JS app (no build step, no npm dependencies).
At runtime it:

1. Fetches the raw `.puml` source of the selected diagram (`../<file>.puml`).
2. Encodes it using PlantUML's **standard transport**: raw-deflate the UTF-8
   source with the browser-native `CompressionStream("deflate-raw")`, then
   pack it with PlantUML's custom 6-bit/64-char alphabet (the exact same
   encoding `editor.plantuml.com` uses internally) — no external zlib library
   needed. This keeps the resulting URL short (roughly 1/3–1/6th of the
   source length).
3. Requests the rendered SVG/PNG from a PlantUML rendering server via a
   normal `<img>` tag:
   `https://www.plantuml.com/plantuml/svg/<encoded-token>`
4. Displays it in a pannable / zoomable canvas.

> **Why not the simpler `~h<hex>` transport?** It needs no compression at all,
> but doubles the byte size of the source. For these diagrams that produces
> URLs of 8,000–14,000 characters — long enough that the public
> `plantuml.com` server's CDN/WAF rejects them outright with `HTTP 403`. The
> deflate-based encoding above keeps URLs in the ~2,000–4,000 character range,
> which renders fine. (As a last-resort fallback, browsers without
> `CompressionStream` support — very old ones — automatically drop back to hex,
> with a note in the status bar; this mainly matters if you're pointing at a
> self-hosted server with no URL-length restriction.)

Because the diagram is requested as an `<img>`, there are no CORS issues with
the rendering server — only the `fetch()` of the local `.puml` source needs to
be served over `http(s)://` instead of `file://` (browsers block
cross-origin `fetch` from the `file:` scheme). That's what `server.js` is for.

## Running it

From this `viewer/` folder, start the bundled zero-dependency static server:

```bash
node server.js          # serves on http://localhost:4040 by default
node server.js 5050     # or pick your own port
```

Then open **http://localhost:4040/viewer/** in your browser.

No Node available? Any static file server rooted at `docs/flows/` works just
as well, e.g.:

```bash
# Python 3
cd docs/flows && python -m http.server 4040

# npx (no install)
cd docs/flows && npx serve -l 4040
```

Then browse to `http://localhost:4040/viewer/`.

> ⚠️ Opening `index.html` directly via `file://` will show the diagram list
> but fail to load the `.puml` source (browser CORS policy on `fetch`). Always
> serve it over HTTP.

## Running it with Docker (for online / shared access)

A `Dockerfile`, `Dockerfile.dockerignore` and `docker-compose.yml` are
included so you can build, ship and host the viewer anywhere Docker runs
(a VPS, internal server, Kubernetes, Cloud Run, etc.) — no Node/Python
install required on the host, just Docker.

**Quickest path — Docker Compose:**

```bash
cd docs/flows/viewer
docker compose up -d --build
```

Open **http://localhost:4040/viewer/** (or `http://<server-ip>:4040/viewer/`
if running on a remote host — make sure the port is reachable/open).

**Plain `docker build` / `docker run`:**

```bash
# ⚠️ Build context MUST be docs/flows (the *parent* of viewer/), because the
# image needs both the viewer app and the sibling .puml sources — see the
# header comment in Dockerfile for why.

# from inside docs/flows/
docker build -f viewer/Dockerfile -t geonera-flow-viewer .
docker run -d --name geonera-flow-viewer -p 4040:4040 geonera-flow-viewer

# ...or from the repo root in one shot
docker build -f docs/flows/viewer/Dockerfile -t geonera-flow-viewer docs/flows
docker run -d --name geonera-flow-viewer -p 4040:4040 geonera-flow-viewer
```

The container:
- Runs the same zero-dependency `server.js` (no extra runtime deps — the
  image is just `node:20-alpine` + static files).
- Listens on `$PORT` (defaults to `4040`; override with `-e PORT=8080
  -p 8080:8080`, handy for platforms like Cloud Run / Render that inject
  their own `$PORT`).
- Runs as the image's built-in non-root `node` user.
- Ships a `HEALTHCHECK` that polls `/viewer/` (`docker ps` shows `healthy`
  once it's serving).
- Shuts down cleanly on `docker stop` (handles `SIGTERM`/`SIGINT`).

> 🌍 **Going public:** the container only serves static files and proxies
> nothing — diagram rendering still happens client-side, by the *visitor's*
> browser talking to the configured PlantUML server (public by default; see
> *Render settings* in the sidebar to point it elsewhere). So all you need to
> expose is this container's HTTP port — e.g. behind your existing reverse
> proxy / TLS termination (nginx, Caddy, Traefik, a cloud load balancer, …).

## Deploying from the published image (no source checkout needed)

The image is also published to Docker Hub as
**`sansalfian/geonera-ingestion-docs`** (same `sansalfian/geonera-*` naming
convention as the rest of the stack — see `docker-compose.portainer.yml` at
the repo root). That means a server can run the viewer by just pulling the
image — no git clone, no build step.

> ⚠️ **Heads-up on visibility:** this image bakes in the `.puml` sources
> (architecture details — lock IDs, queue layout, state machine, retry/claim
> logic). Make sure the `sansalfian/geonera-ingestion-docs` repo on Docker Hub
> is set to the visibility level (private/public) appropriate for your org
> before relying on it for deployment.

**Option A — one-shot script** (`run.sh` for Linux/macOS/WSL,
`run.ps1` for Windows/PowerShell). Pulls the image, replaces any previous
instance, and starts the container — re-runnable any time you want to
redeploy a newer tag:

```bash
# Linux / macOS / WSL
./run.sh                       # defaults: :latest tag, host port 4040
PORT=8080 ./run.sh             # publish on a different host port
IMAGE_TAG=1.0 ./run.sh         # pin a specific image tag
CONTAINER_NAME=docs-viewer ./run.sh
```

```powershell
# Windows PowerShell
.\run.ps1                                  # defaults: :latest tag, host port 4040
.\run.ps1 -Port 8080                       # publish on a different host port
.\run.ps1 -ImageTag 1.0 -ContainerName docs-viewer
```

Both scripts read every knob from environment variables / parameters, so they
also drop cleanly into CI/CD pipelines or a `systemd` unit's `ExecStart=`.

**Option B — Docker Compose** (`docker-compose.deploy.yml` — *runs* the
published image, as opposed to `docker-compose.yml` which *builds* it
locally; same pull-don't-build pattern as `docker-compose.portainer.yml`):

```bash
docker compose -f docker-compose.deploy.yml up -d
# override the tag / host port if needed:
IMAGE_TAG=1.0 PORT=8080 docker compose -f docker-compose.deploy.yml up -d
```

> 🔌 **Network requirement:** this compose file attaches the viewer to
> **`geonera_geonera`** — the Docker network created by the main stack
> (`docker-compose.portainer.yml` declares `networks.geonera`; Portainer
> prefixes it with the stack name `geonera`, yielding `geonera_geonera`). It's
> declared `external: true`, so **the main `geonera` stack must already be
> deployed first** (it owns/creates that network — this file only joins it).
> Joining it lets the viewer reach the ingestion stack's services directly by
> container name (`ingestion`, `admin-backend`, …) over the internal Docker
> network — handy if you want to point *Render settings* at an internal
> PlantUML server, or front everything with a shared reverse-proxy container
> on the same network.
>
> If you deploy the viewer standalone (main stack not present, or its network
> uses a different name/prefix), either create the network first
> (`docker network create geonera_geonera`) or remove the `networks:` /
> `external:` blocks from `docker-compose.deploy.yml` to fall back to a
> private default network.

This file is also Portainer-ready — paste it into **Stacks → Add stack →
Web editor**, optionally setting `IMAGE_TAG` / `PORT` as stack-level env vars.

## Features

- **Live rendering** of all 7 processes (`00-overview-and-locking.puml` …
  `06-sync-outbox.puml`) — picked straight from the sidebar.
- **Zoom & pan** — scroll/pinch to zoom toward the cursor, drag to pan,
  "fit to screen", and "reset to 100%" buttons. Double-click also fits.
- **Source viewer** — toggle a side panel (`{ }` button) showing the raw
  `.puml` text for the selected diagram.
- **SVG or PNG** output — SVG is recommended (crisp at any zoom level).
- **Bring-your-own render server** — by default the viewer uses the public
  `https://www.plantuml.com/plantuml`. Open *Render settings* in the sidebar
  to point it at a local/self-hosted instance instead, e.g. the official
  Docker image, for fully offline / private rendering:

  ```bash
  docker run -d -p 8080:8080 plantuml/plantuml-server:jetty
  ```

  then set the server URL to `http://localhost:8080`.
- **Light / dark UI chrome** toggle — purely cosmetic for the viewer shell;
  the diagrams themselves always render on a guaranteed white background
  (`skinparam backgroundColor #FFFFFF` is set in every `.puml`), so contrast
  stays correct regardless of your OS/browser theme.
- **Download** the currently rendered image, or **open the raw `.puml`**
  source in a new tab.
- Settings (server URL, format, theme) persist in `localStorage`.

## Files

| File | Purpose |
|---|---|
| `index.html` | App shell: sidebar, toolbar, viewer canvas |
| `style.css` | Styling (light/dark themes for the UI chrome) |
| `app.js` | Diagram list, PlantUML URL encoding, rendering, zoom/pan, settings |
| `server.js` | Zero-dependency static file server (`node server.js [port]`) |
| `Dockerfile` | Container image definition (build context = `docs/flows/`) |
| `Dockerfile.dockerignore` | Build-context exclusions for the image (BuildKit-adjacent ignore file) |
| `docker-compose.yml` | Build the image locally & run it (`docker compose up -d --build`) |
| `docker-compose.deploy.yml` | Run the **published** `sansalfian/geonera-ingestion-docs` image — no build, no source checkout |
| `run.sh` / `run.ps1` | One-shot create/recreate-container scripts that pull & run the published image (Linux/macOS/WSL and Windows respectively) |
