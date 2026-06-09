/* ============================================================================
 * Geonera Ingestion — Flow Diagram Viewer
 *
 * Renders the .puml sources in ../ live in the browser by talking to a
 * PlantUML rendering server (the public one by default, or any local/self
 * hosted instance — e.g. the `plantuml/plantuml-server` Docker image).
 *
 * No build step, no dependencies. Encoding uses PlantUML's "hex" transport
 * (`~h<hex>`), which needs no compression library — just UTF-8 -> hex.
 * ==========================================================================*/

const DEFAULT_SERVER = "https://www.plantuml.com/plantuml";

const DIAGRAMS = [
  {
    file: "00-overview-and-locking.puml",
    num: "0",
    name: "Overview & Advisory Locking",
    desc: "Queue trigger → Parent Handler → advisory lock acquisition, lock health monitor, fork-join.",
  },
  {
    file: "01-ticks-regular.puml",
    num: "1",
    name: "Ticks Regular (T-0 / T-1 / T-2)",
    desc: "Hourly regular tick ingestion: 3-way split across Ingest, Verify and Confirm/Demote workers.",
  },
  {
    file: "02-backfill-ticks.puml",
    num: "2",
    name: "Backfill Ticks",
    desc: "Sweeper for failed/stuck ticks: Ingestion, Reset & Recovery, and Final Validation groups.",
  },
  {
    file: "03-candles-regular.puml",
    num: "3",
    name: "Candles Regular",
    desc: "Daily seeding (00:00 UTC) plus the 05:08 UTC stream-based 19-timeframe aggregation run.",
  },
  {
    file: "04-backfill-candles.puml",
    num: "4",
    name: "Backfill Candles",
    desc: "Candle sweeper: reset path for FAILED/BROKEN/PROCESSED rows vs. the ready-to-aggregate catch-up path.",
  },
  {
    file: "05-maintenance.puml",
    num: "5",
    name: "Maintenance",
    desc: "Forward seeding, gap detection / IsPause handling, and Mark-&-Sweep pruning (DB + R2).",
  },
  {
    file: "06-sync-outbox.puml",
    num: "6",
    name: "Sync — Outbox Worker",
    desc: "Transactional Outbox consumer: atomic CTE claim, idempotent ResolvedTickCount recompute, conditional delete.",
  },
];

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const state = {
  current: null,       // diagram descriptor
  source: "",          // raw .puml text
  format: localStorage.getItem("puml-viewer-format") || "svg",
  serverUrl: localStorage.getItem("puml-viewer-server") || DEFAULT_SERVER,
  scale: 1,
  panX: 40,
  panY: 40,
  naturalW: 0,
  naturalH: 0,
};

// ---------------------------------------------------------------------------
// DOM refs
// ---------------------------------------------------------------------------
const $ = (id) => document.getElementById(id);

const els = {
  list: $("diagram-list"),
  search: $("search"),
  title: $("diagram-title"),
  subtitle: $("diagram-subtitle"),
  stage: $("stage"),
  panZoom: $("pan-zoom"),
  img: $("diagram-img"),
  empty: $("empty-state"),
  loading: $("loading"),
  error: $("error"),
  zoomLevel: $("zoom-level"),
  zoomIn: $("zoom-in"),
  zoomOut: $("zoom-out"),
  zoomFit: $("zoom-fit"),
  zoomReset: $("zoom-reset"),
  toggleSource: $("toggle-source"),
  sourcePane: $("source-pane"),
  sourceCode: $("source-code"),
  openRaw: $("open-raw"),
  downloadImage: $("download-image"),
  toggleTheme: $("toggle-theme"),
  themeIcon: $("theme-icon"),
  themeLabel: $("theme-label"),
  serverUrlInput: $("server-url"),
  formatSelect: $("format-select"),
  resetServer: $("reset-server"),
  statusText: $("status-text"),
};

// ---------------------------------------------------------------------------
// PlantUML URL encoding.
//
// Two transports are supported:
//
//  1. "Standard" transport (preferred): raw-deflate the UTF-8 source, then
//     pack it with PlantUML's custom 6-bit alphabet (3 bytes -> 4 chars).
//     This is exactly what editor.plantuml.com / plantuml.com use, and
//     produces URLs roughly 1/3-1/6th the length of the raw source — well
//     under the URL-length limits enforced by the public server's CDN/WAF.
//     Compression uses the browser-native `CompressionStream("deflate-raw")`
//     (broadly supported in modern Chrome/Edge/Firefox/Safari) — no external
//     zlib library needed.
//
//  2. "Hex" transport (fallback, `~h<hex>`): a plain UTF-8 -> hex dump, no
//     compression required. Always correct, but produces URLs ~2x the byte
//     length of the source — long enough that the public plantuml.com server
//     rejects them with HTTP 403 (its CDN/WAF caps URL length). Kept as a
//     fallback for browsers without `CompressionStream` (e.g. very old ones),
//     which will mostly only matter against self-hosted servers anyway.
// ---------------------------------------------------------------------------
const PLANTUML_ALPHABET =
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";

function encode6bit(b) {
  return PLANTUML_ALPHABET[b & 0x3f];
}

function append3bytes(b1, b2, b3) {
  const c1 = b1 >> 2;
  const c2 = ((b1 & 0x3) << 4) | (b2 >> 4);
  const c3 = ((b2 & 0xf) << 2) | (b3 >> 6);
  const c4 = b3 & 0x3f;
  return encode6bit(c1) + encode6bit(c2) + encode6bit(c3) + encode6bit(c4);
}

function encodePlantUmlBytes(bytes) {
  let out = "";
  for (let i = 0; i < bytes.length; i += 3) {
    if (i + 2 < bytes.length) {
      out += append3bytes(bytes[i], bytes[i + 1], bytes[i + 2]);
    } else if (i + 1 < bytes.length) {
      out += append3bytes(bytes[i], bytes[i + 1], 0);
    } else {
      out += append3bytes(bytes[i], 0, 0);
    }
  }
  return out;
}

const supportsDeflate = typeof CompressionStream !== "undefined";

async function deflateRaw(bytes) {
  const cs = new CompressionStream("deflate-raw");
  const writer = cs.writable.getWriter();
  writer.write(bytes);
  writer.close();
  const buf = await new Response(cs.readable).arrayBuffer();
  return new Uint8Array(buf);
}

function encodeHex(bytes) {
  let hex = "";
  for (const b of bytes) hex += b.toString(16).padStart(2, "0");
  return "~h" + hex;
}

/**
 * Returns `{ token, mode }` where `token` is the URL path segment to append
 * after `/<format>/`, and `mode` describes which transport was used
 * (surfaced in the status bar for transparency).
 */
async function encodeForServer(text) {
  const bytes = new TextEncoder().encode(text);
  if (supportsDeflate) {
    try {
      const compressed = await deflateRaw(bytes);
      return { token: encodePlantUmlBytes(compressed), mode: "deflate" };
    } catch (e) {
      // fall through to hex on any unexpected compression failure
    }
  }
  return { token: encodeHex(bytes), mode: "hex" };
}

async function buildRenderUrl(serverUrl, format, source) {
  const base = serverUrl.replace(/\/+$/, "");
  const { token, mode } = await encodeForServer(source);
  return { url: `${base}/${format}/${token}`, mode };
}

// ---------------------------------------------------------------------------
// Sidebar list
// ---------------------------------------------------------------------------
function renderList(filter = "") {
  const q = filter.trim().toLowerCase();
  els.list.innerHTML = "";

  DIAGRAMS
    .filter((d) =>
      !q ||
      d.name.toLowerCase().includes(q) ||
      d.desc.toLowerCase().includes(q) ||
      d.file.toLowerCase().includes(q)
    )
    .forEach((d) => {
      const btn = document.createElement("button");
      btn.className = "diagram-item" + (state.current && state.current.file === d.file ? " active" : "");
      btn.innerHTML = `
        <span class="num">PROCESS ${d.num}</span>
        <span class="name">${escapeHtml(d.name)}</span>
        <span class="desc">${escapeHtml(d.desc)}</span>
      `;
      btn.addEventListener("click", () => selectDiagram(d));
      els.list.appendChild(btn);
    });

  if (!els.list.children.length) {
    const empty = document.createElement("p");
    empty.className = "desc";
    empty.style.padding = "12px";
    empty.textContent = "No diagrams match your search.";
    els.list.appendChild(empty);
  }
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// ---------------------------------------------------------------------------
// Selecting & rendering a diagram
// ---------------------------------------------------------------------------
async function selectDiagram(d) {
  state.current = d;
  renderList(els.search.value);

  els.title.textContent = `${d.num}. ${d.name}`;
  els.subtitle.textContent = d.file;
  els.openRaw.href = `../${d.file}`;
  els.empty.classList.add("hidden");
  els.error.classList.add("hidden");
  els.loading.classList.remove("hidden");
  els.img.removeAttribute("src");

  try {
    const res = await fetch(`../${d.file}`, { cache: "no-store" });
    if (!res.ok) throw new Error(`Failed to fetch ${d.file} (HTTP ${res.status}). Are you serving this folder over HTTP? See README.md.`);
    state.source = await res.text();
    els.sourceCode.textContent = state.source;

    const { url, mode } = await buildRenderUrl(state.serverUrl, state.format, state.source);
    await loadImage(url);
    els.downloadImage.href = url;
    els.downloadImage.download = d.file.replace(/\.puml$/, `.${state.format}`);
    els.loading.classList.add("hidden");
    fitToScreen();
    const encNote = mode === "hex"
      ? " (hex transport — your browser lacks compression support; very long URLs may be rejected by public servers)"
      : "";
    els.statusText.textContent =
      `Rendered "${d.name}" via ${state.serverUrl}${encNote} • Drag to pan • Scroll / pinch to zoom • Double-click to fit`;
  } catch (err) {
    els.loading.classList.add("hidden");
    els.error.textContent = "⚠ " + err.message;
    els.error.classList.remove("hidden");
  }
}

function loadImage(url) {
  return new Promise((resolve, reject) => {
    const probe = new Image();
    probe.onload = () => {
      state.naturalW = probe.naturalWidth || probe.width || 800;
      state.naturalH = probe.naturalHeight || probe.height || 600;
      els.img.src = url;
      els.img.style.width = state.naturalW + "px";
      els.img.style.height = state.naturalH + "px";
      resolve();
    };
    probe.onerror = () => reject(new Error(
      "Could not render the diagram. The PlantUML server may be unreachable, " +
      "or the source has a syntax error. Check the server URL in “Render settings”."
    ));
    probe.src = url;
  });
}

// ---------------------------------------------------------------------------
// Zoom & pan
// ---------------------------------------------------------------------------
function applyTransform() {
  els.panZoom.style.transform = `translate(${state.panX}px, ${state.panY}px) scale(${state.scale})`;
  els.zoomLevel.textContent = Math.round(state.scale * 100) + "%";
}

function setScale(newScale, anchorX, anchorY) {
  newScale = Math.min(8, Math.max(0.05, newScale));
  const rect = els.stage.getBoundingClientRect();
  const ax = anchorX != null ? anchorX - rect.left : rect.width / 2;
  const ay = anchorY != null ? anchorY - rect.top : rect.height / 2;

  // Keep the point under the cursor stationary while zooming.
  const worldX = (ax - state.panX) / state.scale;
  const worldY = (ay - state.panY) / state.scale;
  state.scale = newScale;
  state.panX = ax - worldX * state.scale;
  state.panY = ay - worldY * state.scale;
  applyTransform();
}

function fitToScreen() {
  if (!state.naturalW || !state.naturalH) return;
  const rect = els.stage.getBoundingClientRect();
  const pad = 48;
  const scaleX = (rect.width - pad * 2) / state.naturalW;
  const scaleY = (rect.height - pad * 2) / state.naturalH;
  state.scale = Math.max(0.05, Math.min(scaleX, scaleY, 1.5));
  state.panX = (rect.width - state.naturalW * state.scale) / 2;
  state.panY = (rect.height - state.naturalH * state.scale) / 2;
  applyTransform();
}

function resetZoom() {
  if (!state.naturalW) return;
  const rect = els.stage.getBoundingClientRect();
  state.scale = 1;
  state.panX = (rect.width - state.naturalW) / 2;
  state.panY = 24;
  applyTransform();
}

// Drag to pan
let dragging = false, dragStartX = 0, dragStartY = 0, panStartX = 0, panStartY = 0;
els.stage.addEventListener("pointerdown", (e) => {
  if (!state.current) return;
  dragging = true;
  els.stage.classList.add("grabbing");
  dragStartX = e.clientX; dragStartY = e.clientY;
  panStartX = state.panX; panStartY = state.panY;
  els.stage.setPointerCapture(e.pointerId);
});
els.stage.addEventListener("pointermove", (e) => {
  if (!dragging) return;
  state.panX = panStartX + (e.clientX - dragStartX);
  state.panY = panStartY + (e.clientY - dragStartY);
  applyTransform();
});
['pointerup', 'pointercancel', 'pointerleave'].forEach((evt) =>
  els.stage.addEventListener(evt, () => {
    dragging = false;
    els.stage.classList.remove("grabbing");
  })
);

// Scroll / pinch to zoom
els.stage.addEventListener("wheel", (e) => {
  if (!state.current) return;
  e.preventDefault();
  const factor = Math.exp(-e.deltaY * 0.0015);
  setScale(state.scale * factor, e.clientX, e.clientY);
}, { passive: false });

// Double-click to fit
els.stage.addEventListener("dblclick", () => state.current && fitToScreen());

// Toolbar zoom buttons
els.zoomIn.addEventListener("click", () => setScale(state.scale * 1.25));
els.zoomOut.addEventListener("click", () => setScale(state.scale / 1.25));
els.zoomFit.addEventListener("click", fitToScreen);
els.zoomReset.addEventListener("click", resetZoom);

window.addEventListener("resize", () => state.current && fitToScreen());

// ---------------------------------------------------------------------------
// Source pane toggle
// ---------------------------------------------------------------------------
els.toggleSource.addEventListener("click", () => {
  els.sourcePane.classList.toggle("open");
});

// ---------------------------------------------------------------------------
// Theme toggle (UI chrome only — rendered diagrams always have a white
// background, forced in the .puml skinparams, so they stay readable either way)
// ---------------------------------------------------------------------------
function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  els.themeIcon.textContent = theme === "dark" ? "☀️" : "🌙";
  els.themeLabel.textContent = theme === "dark" ? "Light UI" : "Dark UI";
  localStorage.setItem("puml-viewer-theme", theme);
}
els.toggleTheme.addEventListener("click", () => {
  const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
  applyTheme(next);
});
applyTheme(localStorage.getItem("puml-viewer-theme") || "light");

// ---------------------------------------------------------------------------
// Render settings (server URL / format)
// ---------------------------------------------------------------------------
els.serverUrlInput.value = state.serverUrl;
els.formatSelect.value = state.format;

function rerenderCurrent() {
  if (state.current) selectDiagram(state.current);
}

els.serverUrlInput.addEventListener("change", () => {
  const v = els.serverUrlInput.value.trim() || DEFAULT_SERVER;
  state.serverUrl = v;
  localStorage.setItem("puml-viewer-server", v);
  rerenderCurrent();
});

els.formatSelect.addEventListener("change", () => {
  state.format = els.formatSelect.value;
  localStorage.setItem("puml-viewer-format", state.format);
  rerenderCurrent();
});

els.resetServer.addEventListener("click", () => {
  state.serverUrl = DEFAULT_SERVER;
  els.serverUrlInput.value = DEFAULT_SERVER;
  localStorage.setItem("puml-viewer-server", DEFAULT_SERVER);
  rerenderCurrent();
});

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------
els.search.addEventListener("input", () => renderList(els.search.value));

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
renderList();
applyTransform();
selectDiagram(DIAGRAMS[0]);
