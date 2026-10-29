// QuestDB failover test for @questdb/nodejs-client, the Node.js twin of
// failover.html. Writes one row per tick and queries it back continuously,
// printing which node each side uses and whether any row was lost.
//
//   QDB_CLIENT_CONF='wss::addr=h1:9000,h2:9000,h3:9000;username=<user>;password=<password>;tls_verify=unsafe_off;' \
//     node failover.mjs
//
// Optional: WRITES=0 (read only), RUN_ID=<run>, WRITE_MS=200, QUERY_MS=1000,
// DURATION_S=<seconds> to stop on its own.
// Ctrl-C or SIGTERM stops: it waits for the acknowledgement of every row, then closes.

import { connectQwpNodeClient } from "@questdb/nodejs-client";

const TABLE = "browser_failover"; // shared with failover.html, so either can read the other's runs
const WRITES = process.env.WRITES !== "0";
const WRITE_MS = Number(process.env.WRITE_MS ?? 200);
const QUERY_MS = Number(process.env.QUERY_MS ?? 1000);

let conf = process.env.QDB_CLIENT_CONF;
if (!conf) throw new Error("QDB_CLIENT_CONF is not set");
if (!conf.endsWith(";")) conf += ";";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const msg = (e) => (e?.cause ? `${e.message} (cause: ${e.cause.message ?? e.cause})` : e?.message ?? String(e));
const log = (side, text) => console.log(`${new Date().toISOString().slice(11, 23)} [${side}] ${text}`);

const ingress = { endpoint: "-", seq: 0, failures: 0 };
const egress = { endpoint: "-", ok: 0, failures: 0, lastMs: 0 };
let verify = {};
let sender, run, running = true;

function onConnEvent(side, e) {
  const state = side === "ingress" ? ingress : egress;
  if (["connected", "reconnected", "failed-over"].includes(e.kind) && e.endpoint) state.endpoint = String(e.endpoint);
  const from = e.previousEndpoint ? ` from ${e.previousEndpoint}` : "";
  const cause = e.cause ? ` (${msg(e.cause)})` : "";
  const head = [e.kind + (e.attempt ? " #" + e.attempt : ""), e.endpoint && String(e.endpoint)];
  log(side, head.filter(Boolean).join(" ") + from + cause);
}

// The connect string carries endpoints, credentials and TLS. lazy_connect=on
// lets the client start while no primary is reachable: senders connect in the
// background and queue rows meanwhile. Read-only runs open no ingestion
// connection. Callbacks go in the typed options.
const db = await connectQwpNodeClient(
  conf + (WRITES ? "" : "sender_pool_min=0;") +
    "lazy_connect=on;query_timeout_ms=10000;initial_credit=1048576;",
  {
    ingress: {
      onSenderError: (e) => log("ingress", `rejected ${e.category} (${e.appliedPolicy}) ${e.serverMessage ?? ""}`),
      onError: (e) => { if (e.terminal) log("ingress", "terminal: " + msg(e.error)); },
      reconnect: { onEvent: (e) => onConnEvent("ingress", e) },
    },
    egress: {
      reconnect: { onEvent: (e) => onConnEvent("egress", e) },
    },
  },
);
log("client", "started (connections open in the background)");

try {
  if (WRITES) {
    // Replay after a reconnect is at least once, so DEDUP keeps the table exact.
    // While the primary is down this runs on a replica and fails. The table
    // normally exists from an earlier run, so carry on and let writes queue.
    try {
      await exec(`CREATE TABLE IF NOT EXISTS ${TABLE} (
        run SYMBOL, seq LONG, value DOUBLE, timestamp TIMESTAMP
      ) TIMESTAMP(timestamp) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(timestamp, run, seq)`);
    } catch (e) {
      log("client", "CREATE TABLE failed, continuing: " + msg(e));
    }
    run = process.env.RUN_ID || "node-run-" + Date.now();
  } else {
    run = process.env.RUN_ID || (await latestRun());
  }
  log("client", `started ${run} (${WRITES ? "writes and reads" : "reads only"})`);

  const stop = () => {
    log("client", "stopping");
    running = false;
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  if (process.env.DURATION_S) setTimeout(stop, Number(process.env.DURATION_S) * 1000);
  await Promise.all([WRITES ? writeLoop() : null, queryLoop(), statusLoop()]);

  if (sender) {
    log("ingress", "waiting up to 10 s for acknowledgements, then up to 5 s in close(). Ctrl-C again quits now" +
      (conf.includes("sf_dir=") ? "; the journal keeps unacknowledged rows" : " and loses unacknowledged rows"));
    try {
      log("ingress", `final flush acked: ${await sender.flushAndWait(10_000)}`);
    } catch (e) {
      log("ingress", "final flush failed: " + msg(e));
    }
    printStatus(); // before close(): a returned lease throws QwpClientClosedError
    await sender.close().catch(() => {});
    sender = undefined;
  }
} finally {
  await db.close();
  log("client", "closed");
}

// One row per tick. seq increments for every completed row, so a seq missing
// from the table is a row that was really lost.
async function writeLoop() {
  while (running) {
    try {
      sender ??= await db.borrowSender();
      const seq = ingress.seq++;
      await sender.table(TABLE)
        .symbol("run", run)
        .intColumn("seq", seq)
        .floatColumn("value", Math.sin(seq / 25) * 10 + Math.random())
        .at(Date.now(), "ms");
    } catch (e) {
      // A terminal error ends the sender. Drop it, and the next borrow replaces it.
      ingress.failures++;
      log("ingress", "sender failed: " + msg(e));
      const dead = sender;
      sender = undefined;
      await dead?.close().catch(() => {});
    }
    await sleep(WRITE_MS);
  }
}

// A fresh lease per round: a query session that exhausts its failover limits
// stays failed, and closing the lease makes the pool replace it.
async function queryLoop() {
  while (running) {
    const started = performance.now();
    let lease;
    try {
      lease = await db.borrowQuery();
      const [[n, distinct, maxSeq, lastTs]] = await select(lease,
        `SELECT count(), count_distinct(seq), max(seq), max(timestamp) FROM ${TABLE} WHERE run = $1`);
      const expected = maxSeq === null ? 0 : Number(maxSeq) + 1;
      verify = {
        rows: Number(n),
        missing: expected - Number(distinct),
        duplicates: Number(n) - Number(distinct),
        lastTs: lastTs === null ? null : Number(lastTs / 1000n),
      };
      egress.ok++;
      egress.lastMs = Math.round(performance.now() - started);
    } catch (e) {
      egress.failures++;
      log("egress", "query failed: " + msg(e));
    } finally {
      await lease?.close().catch(() => {});
    }
    await sleep(QUERY_MS);
  }
}

async function exec(sql) {
  const lease = await db.borrowQuery();
  try {
    const q = await lease.query(sql);
    await q.completion;
  } finally {
    await lease.close();
  }
}

// The most recent run in the table: its last row by designated timestamp.
async function latestRun() {
  const lease = await db.borrowQuery();
  try {
    const q = await lease.query(`SELECT run FROM ${TABLE} LIMIT -1`);
    let latest;
    for await (const batch of q) for (const [r] of batch.rows()) latest = r;
    await q.completion;
    if (!latest) throw new Error(`${TABLE} has no rows: run once with writes on`);
    return latest;
  } finally {
    await lease.close();
  }
}

async function select(lease, sql) {
  let rows = [];
  const q = await lease.query(sql, {
    binds: (b) => b.setVarchar(0, run),
    onFailoverReset: () => { rows = []; }, // a failed-over query restarts from row one
  });
  for await (const batch of q) for (const row of batch.rows()) rows.push(row);
  await q.completion;
  return rows;
}

async function statusLoop() {
  while (running) {
    await sleep(5_000);
    printStatus();
  }
}

function printStatus() {
  const lag = WRITES && verify.lastTs ? `${Date.now() - verify.lastTs} ms` : "-";
  if (WRITES) {
    log("status", `ingress ${ingress.endpoint} sent=${ingress.seq} ` +
      `published=${sender?.publishedSequence ?? "-"} acked=${sender?.ackedSequence ?? "-"} ` +
      `replay=${sender?.metrics.ingress?.memoryReplayUsedBytes ?? 0}B failures=${ingress.failures}`);
  }
  log("status", `egress ${egress.endpoint} ok=${egress.ok} failures=${egress.failures} latency=${egress.lastMs}ms`);
  log("status", `verify ${run} visible=${verify.rows ?? "-"} missing=${verify.missing ?? "-"} ` +
    `duplicates=${verify.duplicates ?? "-"} lag=${lag}`);
}
