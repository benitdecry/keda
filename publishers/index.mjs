import express from "express";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

// ── Config ────────────────────────────────────────────────────────────────
const DAPR_PORT     = process.env.DAPR_HTTP_PORT   || "3500";
const DAPR_BASE     = `http://localhost:${DAPR_PORT}/v1.0`;
const PUBSUB_NAME   = process.env.PUBSUB_NAME      || "pubsub";
const TOPIC_NAME    = process.env.TOPIC_NAME       || "orders";
const STATESTORE    = process.env.STATESTORE_NAME  || "statestore";
const APP_PORT      = parseInt(process.env.APP_PORT || "6000");
const APP_VERSION   = process.env.APP_VERSION      || "v1";

// ── Health ────────────────────────────────────────────────────────────────
app.get("/health", (_req, res) => {
  res.json({
    status:  "ok",
    service: "publisher",
    version: APP_VERSION,
    time:    new Date().toISOString(),
  });
});

// ── Publish order via Dapr pub/sub ────────────────────────────────────────
app.post("/publish", async (req, res) => {
  const order = {
    id:       req.body.id       || `order-${Date.now()}`,
    item:     req.body.item     || "default-item",
    quantity: req.body.quantity || 1,
    ts:       new Date().toISOString(),
    version:  APP_VERSION,
  };

  console.log(`[publisher] Publishing → ${JSON.stringify(order)}`);

  try {
    const resp = await fetch(
      `${DAPR_BASE}/publish/${PUBSUB_NAME}/${TOPIC_NAME}`,
      {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify(order),
      }
    );

    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Dapr publish failed [${resp.status}]: ${text}`);
    }

    console.log(`[publisher] Order ${order.id} published OK`);
    res.status(200).json({ success: true, order });

  } catch (err) {
    console.error(`[publisher] Publish error: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Save state via Dapr state store ───────────────────────────────────────
app.post("/state", async (req, res) => {
  const { key, value } = req.body;

  if (!key || value === undefined) {
    return res.status(400).json({ error: "key and value are required" });
  }

  console.log(`[publisher] Saving state → key=${key}`);

  try {
    const resp = await fetch(
      `${DAPR_BASE}/state/${STATESTORE}`,
      {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify([{ key, value }]),
      }
    );

    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Dapr state save failed [${resp.status}]: ${text}`);
    }

    console.log(`[publisher] State saved OK → key=${key}`);
    res.status(200).json({ success: true, key });

  } catch (err) {
    console.error(`[publisher] State save error: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Read state from Dapr state store ──────────────────────────────────────
app.get("/state/:key", async (req, res) => {
  const { key } = req.params;

  console.log(`[publisher] Reading state → key=${key}`);

  try {
    const resp = await fetch(`${DAPR_BASE}/state/${STATESTORE}/${key}`);

    if (resp.status === 204 || resp.status === 404) {
      return res.status(404).json({ error: `Key '${key}' not found` });
    }

    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Dapr state get failed [${resp.status}]: ${text}`);
    }

    const data = await resp.json();
    console.log(`[publisher] State read OK → key=${key}`);
    res.status(200).json({ key, value: data });

  } catch (err) {
    console.error(`[publisher] State read error: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────
app.listen(APP_PORT, () => {
  console.log(`[publisher ${APP_VERSION}] Listening on port ${APP_PORT}`);
  console.log(`[publisher ${APP_VERSION}] Dapr sidecar on port ${DAPR_PORT}`);
});