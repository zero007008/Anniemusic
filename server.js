
require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const { Readable } = require("node:stream");
const { pipeline } = require("node:stream/promises");

const app = express();

const PORT = Number(process.env.PORT) || 3000;
const API_KEY = process.env.ANNIE_API_KEY;
const API_URL = (
  process.env.ANNIE_API_URL || "https://api.alonexraj.shop"
).replace(/\/+$/, "");

app.disable("x-powered-by");

app.use(
  helmet({
    contentSecurityPolicy: false,
    crossOriginResourcePolicy: { policy: "cross-origin" }
  })
);

app.use(express.json({ limit: "20kb" }));
app.use(express.static("public"));

if (!API_KEY) {
  console.error("ERROR: ANNIE_API_KEY is missing in .env");
  process.exit(1);
}

// API status
app.get("/api/status", async (req, res) => {
  try {
    const response = await fetch(`${API_URL}/status`, {
      signal: AbortSignal.timeout(10000)
    });

    const data = await response.json().catch(() => ({}));

    if (!response.ok) {
      return res.status(502).json({
        success: false,
        backend: "Online",
        annie: "Offline",
        upstreamStatus: response.status
      });
    }

    res.json({
      success: true,
      backend: "Online",
      annie: data
    });
  } catch (error) {
    res.status(502).json({
      success: false,
      backend: "Online",
      annie: "Offline",
      error: error.message
    });
  }
});

// Audio / Video streaming
app.get("/api/music/download", async (req, res) => {
  const mediaUrl = String(req.query.url || "").trim();
  const type = String(req.query.type || "audio").toLowerCase();

  if (!mediaUrl) {
    return res.status(400).json({
      success: false,
      error: "Music URL or search query is required"
    });
  }

  if (!["audio", "video"].includes(type)) {
    return res.status(400).json({
      success: false,
      error: "Invalid media type"
    });
  }

  if (mediaUrl.length > 2000) {
    return res.status(400).json({
      success: false,
      error: "Query is too long"
    });
  }

  const controller = new AbortController();
  let finished = false;

  res.on("close", () => {
    if (!res.writableEnded) {
      controller.abort();
    }
  });

  try {
    const upstreamURL = new URL(`${API_URL}/download`);
    upstreamURL.searchParams.set("url", mediaUrl);
    upstreamURL.searchParams.set("type", type);
    upstreamURL.searchParams.set("api_key", API_KEY);

    const headers = {
      "X-API-Key": API_KEY
    };

    // Forward browser seeking requests
    if (req.headers.range) {
      headers.Range = req.headers.range;
    }

    if (req.headers["if-range"]) {
      headers["If-Range"] = req.headers["if-range"];
    }

    const upstream = await fetch(upstreamURL, {
      method: "GET",
      headers,
      signal: controller.signal
    });

    if (!upstream.ok && upstream.status !== 206) {
      const errorText = await upstream.text();

      if (!res.headersSent) {
        return res.status(upstream.status).json({
          success: false,
          upstreamStatus: upstream.status,
          error: errorText.slice(0, 1000)
        });
      }

      return res.end();
    }

    // Preserve upstream response status
    res.status(upstream.status);

    const allowedHeaders = [
      "content-type",
      "content-length",
      "content-range",
      "accept-ranges",
      "content-disposition",
      "etag",
      "last-modified"
    ];

    for (const header of allowedHeaders) {
      const value = upstream.headers.get(header);

      if (value) {
        res.setHeader(header, value);
      }
    }

    res.setHeader("Cache-Control", "no-store");

    if (!upstream.body) {
      return res.end();
    }

    const stream = Readable.fromWeb(upstream.body);

    finished = true;
    await pipeline(stream, res);
  } catch (error) {
    if (
      error.name === "AbortError" ||
      error.code === "ERR_STREAM_PREMATURE_CLOSE"
    ) {
      return;
    }

    console.error("Streaming error:", error.message);

    if (!res.headersSent) {
      res.status(502).json({
        success: false,
        error: "Unable to stream media",
        details: error.message
      });
    } else if (!res.writableEnded) {
      res.destroy(error);
    }
  }
});

// Health check
app.get("/health", (req, res) => {
  res.json({
    success: true,
    app: "AnnieMusic",
    version: "2.0.0"
  });
});

// 404
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: "Route not found"
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log("--------------------------------");
  console.log("🎵 AnnieMusic v2.0");
  console.log(`🌐 http://localhost:${PORT}`);
  console.log(`🔌 API: ${API_URL}`);
  console.log("--------------------------------");
});
