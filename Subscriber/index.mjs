import express from "express";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

// ── Config ────────────────────────────────────────────────────────────────
const DAPR_PORT   = process.env.DAPR_HTTP_PORT  || "3500";
const DAPR_BASE   = `http://localhost:${DAPR_PORT}/v1.0`;
const PUBSUB_NAME = process.env.PUBSUB_NAME     || "pubsub";
const TOPIC_NAME  = process.env.TOPIC_NAME      || "orders";
const STATESTORE  = process.env.STATESTORE_NAME || "statestore";
const APP_PORT    = parseInt(process.env.APP_PORT || "6001");
const APP_VERSION = process.env.APP_VERSION     || "v1";

// ── Health ────────────────────────────────────────────────────────────────
app.get("/health", (_req, res) => {
  res.json({
    status:  "ok",
    service: "subscriber",
    version: APP_VERSION,
    time:    new Date().toISOString(),
  });
});

// ── Dapr subscription discovery ───────────────────────────────────────────
// Dapr calls GET /dapr/subscribe on startup to know which topics to consume.
// This replaces any YAML subscription config — the app declares itself.
app.get("/dapr/subscribe", (_req, res) => {
  console.log("[subscriber] Dapr requested subscription list");
  res.json([
    {
      pubsubname: PUBSUB_NAME,
      topic:      TOPIC_NAME,
      route:      "/orders",      // Dapr will POST messages here
    },
  ]);
});

// ── Receive orders from Dapr (pub/sub delivery) ───────────────────────────
// Dapr wraps the payload in a CloudEvents envelope.
// Return 200 → message acknowledged (removed from queue).
// Return 4xx/5xx → Dapr retries delivery up to max-delivery-count.
app.post("/orders", async (req, res) => {
  // Unwrap CloudEvents envelope if present
  const order = req.body.data ?? req.body;

  console.log(`[subscriber] Received order → ${JSON.stringify(order)}`);

  if (!order?.id) {
    console.warn("[subscriber] Order missing id — acknowledging without persist");
    return res.status(200).json({ success: true });
  }

  // Persist to Dapr state store (backed by Cosmos DB)
  const stateKey     = `order-${order.id}`;
  const statePayload = [
    {
      key:   stateKey,
      value: {
        ...order,
        receivedAt:  new Date().toISOString(),
        processedBy: APP_VERSION,
      },
    },
  ];

  try {
    const resp = await fetch(
      `${DAPR_BASE}/state/${STATESTORE}`,
      {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify(statePayload),
      }
    );

    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Dapr state save failed [${resp.status}]: ${text}`);
    }

    console.log(`[subscriber] Order ${order.id} persisted → key=${stateKey}`);

    // 200 = ACK to Dapr → message removed from Service Bus
    res.status(200).json({ success: true, orderId: order.id });

  } catch (err) {
    console.error(`[subscriber] Persist error: ${err.message}`);
    // Non-200 = Dapr will retry delivery
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Read a persisted order from state store ───────────────────────────────
app.get("/orders/:id", async (req, res) => {
  const stateKey = `order-${req.params.id}`;

  console.log(`[subscriber] Reading order → key=${stateKey}`);

  try {
    const resp = await fetch(`${DAPR_BASE}/state/${STATESTORE}/${stateKey}`);

    if (resp.status === 204 || resp.status === 404) {
      return res.status(404).json({ error: `Order '${req.params.id}' not found` });
    }

    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Dapr state get failed [${resp.status}]: ${text}`);
    }

    const data = await resp.json();
    console.log(`[subscriber] Order read OK → key=${stateKey}`);
    res.status(200).json({ orderId: req.params.id, data });

  } catch (err) {
    console.error(`[subscriber] State read error: ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────
app.listen(APP_PORT, () => {
  console.log(`[subscriber ${APP_VERSION}] Listening on port ${APP_PORT}`);
  console.log(`[subscriber ${APP_VERSION}] Dapr sidecar on port ${DAPR_PORT}`);
});