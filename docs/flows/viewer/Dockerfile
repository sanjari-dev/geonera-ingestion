# syntax=docker/dockerfile:1

# ============================================================================
# Geonera Ingestion — Flow Diagram Viewer (containerized)
#
# Packages the static viewer app together with the sibling `.puml` sources
# it renders, behind the bundled zero-dependency Node static server
# (./server.js), so the whole thing can be deployed and reached online.
#
# IMPORTANT — build context must be the `docs/flows` folder (the *parent* of
# this Dockerfile's directory), NOT `viewer/` itself. The server serves both
# the viewer app AND the `../*.puml` sources from one static root, so both
# need to be inside the image.
#
#   # from inside docs/flows/
#   docker build -f viewer/Dockerfile -t geonera-flow-viewer .
#
#   # or from the repo root
#   docker build -f docs/flows/viewer/Dockerfile -t geonera-flow-viewer docs/flows
#
# Run:
#   docker run --rm -p 4040:4040 geonera-flow-viewer
#   # -> open http://localhost:4040/viewer/
#
# (See docker-compose.yml for the one-liner version of the above.)
# ============================================================================

FROM node:20-alpine

LABEL org.opencontainers.image.title="Geonera Ingestion — Flow Diagram Viewer" \
      org.opencontainers.image.description="Interactive web viewer that renders the ingestion pipeline's PlantUML flow diagrams live (zoom & pan, no editor.plantuml.com round-trip needed)"

ENV PORT=4040 \
    HOST=0.0.0.0 \
    NODE_ENV=production

WORKDIR /app/flows

# Plain static assets + a vanilla `http` server — no package.json, no
# npm install, no build step. Copy everything from the build context
# (the .puml sources live alongside the viewer/ folder).
COPY --chown=node:node . .

# The official Node images already ship a non-root `node` user.
USER node

EXPOSE 4040

# Hit the viewer's own index page to confirm the server is alive and able
# to read from disk (busybox wget ships with node:alpine).
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -q -O /dev/null "http://127.0.0.1:4040/viewer/" || exit 1

# `sh -c` so $PORT (e.g., injected by the platform at runtime) is honored;
# server.js itself also falls back to $PORT if no CLI arg is given.
CMD ["sh", "-c", "node viewer/server.js \"$PORT\""]
