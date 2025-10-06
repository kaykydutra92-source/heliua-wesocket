/*
 * Full Liquidity/Pool Creation Tracker
 *
 * This script listens for Solana on-chain events that hint at new liquidity being added
 * or pools being created on decentralized exchanges. It uses Helius WebSockets for
 * real‑time updates, HTTP RPC calls for verification, and optionally the Solscan Pro API
 * for additional metadata. Detected tokens/pools are stored in an SQLite DB and a
 * notification is sent via Telegram. Configuration is via environment variables.
 *
 * Features:
 *  - Configurable subscription modes: logsSubscribe (mentions), programSubscribe,
 *    or logsSubscribe all (firehose).
 *  - Configurable commitments per WebSocket and HTTP RPC requests.
 *  - Rate limiter and circuit breaker to handle RPC 429s gracefully.
 *  - Concurrency limited verification queue and fallback inspector for program
 *    notifications without signatures.
 *  - Optional Solscan API integration for chain information and token metadata.
 *  - Automatic token filtering by monitored program IDs or owner whitelist.
 *  - Time filters to notify only for today or within a maximum age window.
 *  - Express dashboard to view detected tokens/pools in real time.
 *
 * To use:
 *   1. Ensure Node.js (v18+), npm, and Python build tools are installed.
 *   2. Copy this file to your project root (e.g. helius-liquidity-bot).
 *   3. Create .env (see README) with BOT_TOKEN, CHAT_ID, HELIUS_API_KEY, etc.
 *   4. Run `npm install` to install dependencies.
 *   5. Start with `node full-liquidity-tracker.js`.
 */

const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const axios = require('axios');
const express = require('express');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
require('dotenv').config();

/* =====================================
 * Configuration from .env
 * ===================================*/
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
// Helius API key and optional RPC override
const HELIUS_API_KEY = process.env.HELIUS_API_KEY || '';
const RPC_URL = process.env.RPC_URL || (HELIUS_API_KEY
  ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`
  : '');
const RPC_ALT_URLS = (process.env.RPC_ALT_URLS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);
// Optional Solscan Pro API key for metadata
const SOLSCAN_API_KEY = process.env.SOLSCAN_API_KEY || '';
// HTTP RPC commitment, defaults to WS commitment if not set
const WS_COMMITMENT = process.env.WS_COMMITMENT || 'processed';
const RPC_COMMITMENT = process.env.RPC_COMMITMENT || WS_COMMITMENT;
// WebSocket subscription modes
const USE_LOGS_MENTIONS = (process.env.USE_LOGS_MENTIONS || 'true') === 'true';
const USE_PROGRAM_SUBSCRIBE = (process.env.USE_PROGRAM_SUBSCRIBE || 'false') === 'true';
const USE_LOGS_ALL = (process.env.USE_LOGS_ALL || 'false') === 'true';
// Behavior flags and timing
const DELAY_SECONDS = Number(process.env.DELAY_SECONDS || 0) * 1000;
const PORT = Number(process.env.PORT || 3000);
const USER_TZ = process.env.USER_TZ || 'UTC';
const ONLY_TODAY = (process.env.ONLY_TODAY || 'false') === 'true';
const MAX_AGE_SECONDS = Number(process.env.MAX_AGE_SECONDS || 0);
const VERIFY_MINT_BEFORE_NOTIFY = (process.env.VERIFY_MINT_BEFORE_NOTIFY || 'true') === 'true';
// Concurrency and retry
const VERIFY_CONCURRENCY = Number(process.env.VERIFY_CONCURRENCY || 5);
const VERIFY_RETRIES = Number(process.env.VERIFY_RETRIES || 3);
const VERIFY_BASE_DELAY_MS = Number(process.env.VERIFY_BASE_DELAY_MS || 400);
const FALLBACK_SIGNATURE_LIMIT = Number(process.env.FALLBACK_SIGNATURE_LIMIT || 5);
const FALLBACK_CONCURRENCY = Number(process.env.FALLBACK_CONCURRENCY || 2);
const INSPECT_TTL_MS = Number(process.env.FALLBACK_INSPECT_TTL_MS || 60000);
// Rate limiter & circuit breaker parameters
const RATE_LIMIT_RPS = Number(process.env.RATE_LIMIT_RPS || 6);
const RATE_LIMIT_BURST = Number(process.env.RATE_LIMIT_BURST || 12);
const RATE_LIMIT_REFILL_MS = Number(process.env.RATE_LIMIT_REFILL_MS || 250);
const RATE_LIMIT_MAX_BACKOFF_MS = Number(process.env.RATE_LIMIT_MAX_BACKOFF_MS || 15000);
const CIRCUIT_429_THRESHOLD = Number(process.env.CIRCUIT_429_THRESHOLD || 6);
const CIRCUIT_WINDOW_MS = Number(process.env.CIRCUIT_WINDOW_MS || 10000);
const CIRCUIT_OPEN_MS = Number(process.env.CIRCUIT_OPEN_MS || 20000);
// Owner whitelist (if provided) or derived from programs
let OWNER_WHITELIST = (process.env.OWNER_WHITELIST || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);
// Liquidity markers, with optional extras
const EXTRA_MARKERS = (process.env.EXTRA_MARKERS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);
// Debug flag
const DEBUG = (process.env.DEBUG || 'false') === 'true';

/* =====================================
 * Helper functions
 * ===================================*/
function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}
function jitter(ms) {
  const delta = Math.floor(ms * 0.25);
  return ms + (Math.random() * 2 * delta - delta);
}
function debugLog(...args) {
  if (DEBUG) console.debug(new Date().toISOString(), '[DEBUG]', ...args);
}
function log(...args) {
  console.log(new Date().toISOString(), ...args);
}
function warn(...args) {
  console.warn(new Date().toISOString(), '[WARN]', ...args);
}
function errLog(...args) {
  console.error(new Date().toISOString(), '[ERROR]', ...args);
}
function isB58(str) {
  return typeof str === 'string' && /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(str);
}
function isBase58OrSystem(str) {
  if (str === '11111111111111111111111111111111') return true;
  return isB58(str);
}

/* =====================================
 * Rate limiter & circuit breaker
 * ===================================*/
let bucketTokens = RATE_LIMIT_BURST;
let lastRefill = Date.now();
let recent429 = [];
let circuitOpenUntil = 0;

function refillBucket() {
  const now = Date.now();
  const elapsed = now - lastRefill;
  if (elapsed >= RATE_LIMIT_REFILL_MS) {
    const add = Math.floor((elapsed / 1000) * RATE_LIMIT_RPS);
    bucketTokens = Math.min(RATE_LIMIT_BURST, bucketTokens + add);
    lastRefill = now;
  }
}
async function acquireRpcToken() {
  for (;;) {
    refillBucket();
    if (bucketTokens > 0) {
      bucketTokens--;
      return;
    }
    await sleep(50);
  }
}
function mark429() {
  const now = Date.now();
  recent429.push(now);
  recent429 = recent429.filter(t => now - t <= CIRCUIT_WINDOW_MS);
  if (recent429.length >= CIRCUIT_429_THRESHOLD) {
    circuitOpenUntil = now + CIRCUIT_OPEN_MS;
    recent429 = [];
    debugLog('Circuit breaker: OPEN (throttle mode) for', CIRCUIT_OPEN_MS, 'ms');
  }
}
function isCircuitOpen() {
  return Date.now() < circuitOpenUntil;
}
function currentThrottleDelay(base) {
  if (!isCircuitOpen()) return base;
  return Math.min(RATE_LIMIT_MAX_BACKOFF_MS, base * 2 + 500);
}

/* =====================================
 * Programs file & effective whitelist
 * ===================================*/
const programsFile = path.join(__dirname, 'programs.json');
let PROGRAMS = [];
let PROGRAM_IDS = [];
let EFFECTIVE_WHITELIST = [];

function computeEffectiveWhitelist() {
  if (Array.isArray(OWNER_WHITELIST) && OWNER_WHITELIST.length > 0) {
    EFFECTIVE_WHITELIST = OWNER_WHITELIST.filter(Boolean);
  } else {
    const base = new Set(PROGRAM_IDS.filter(Boolean));
    base.add('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
    EFFECTIVE_WHITELIST = Array.from(base);
  }
  debugLog('Computed effective whitelist: ', EFFECTIVE_WHITELIST);
}
function loadPrograms() {
  PROGRAMS = [];
  PROGRAM_IDS = [];
  try {
    if (fs.existsSync(programsFile)) {
      const raw = fs.readFileSync(programsFile, 'utf8');
      const parsed = JSON.parse(raw);
      const arr = Array.isArray(parsed.programs) ? parsed.programs : [];
      PROGRAMS = arr.filter(p => p && p.id && isBase58OrSystem(p.id));
      PROGRAM_IDS = PROGRAMS.map(p => p.id).filter(Boolean);
      log(`Loaded ${PROGRAMS.length} program entries from programs.json`);
    } else {
      warn('programs.json not found — no program filtering will apply');
    }
  } catch (e) {
    errLog('Failed to parse programs.json:', e);
  }
  computeEffectiveWhitelist();
}
loadPrograms();
// Watch programs.json for changes
try {
  fs.watchFile(programsFile, { interval: 2000 }, (curr, prev) => {
    if (curr.mtimeMs !== prev.mtimeMs) {
      log('programs.json changed — reloading');
      loadPrograms();
    }
  });
} catch {
  // ignore watchers on unsupported platforms
}

/* =====================================
 * SQLite database
 * ===================================*/
const DB_FILE = path.join(__dirname, 'tokens.db');
const dbPromise = open({ filename: DB_FILE, driver: sqlite3.Database });
(async () => {
  const db = await dbPromise;
  await db.exec(`CREATE TABLE IF NOT EXISTS tokens (
    token_address TEXT PRIMARY KEY,
    first_seen INTEGER,
    liquidity_tx TEXT,
    program_id TEXT,
    notified INTEGER DEFAULT 0,
    verified INTEGER DEFAULT 0,
    metadata TEXT
  );`);
  await db.exec(`CREATE TABLE IF NOT EXISTS verified_mints (
    token_address TEXT PRIMARY KEY,
    verified_at INTEGER
  );`);
})();

/* =====================================
 * State & Queues
 * ===================================*/
const dumpedTxs = new Set();
const verifiedCache = new Map();
const verifyQueue = [];
let activeVerifications = 0;
const notifyQueue = [];
let notifyProcessing = false;
const inspectingPubkeys = new Map();
let activeInspectors = 0;
const inspectQueue = [];

// Liquidity markers
const baseMarkers = [/mint/i, /addliquidity/i, /create/i, /deposit/i, /pool/i, /swap/i, /initialize/i];
const extraRegex = EXTRA_MARKERS.map(m => {
  try { return new RegExp(m, 'i'); } catch { return null; }
}).filter(Boolean);
const liquidityMarkers = baseMarkers.concat(extraRegex);

/* =====================================
 * Telegram sending
 * ===================================*/
async function sendTelegram(text) {
  if (!BOT_TOKEN || !CHAT_ID) {
    warn('Telegram BOT_TOKEN or CHAT_ID not set; skipping message');
    return null;
  }
  try {
    const res = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      chat_id: CHAT_ID,
      text,
      parse_mode: 'HTML',
      disable_web_page_preview: true
    }, { timeout: 15000 });
    debugLog('Telegram response', res.data);
    return res.data;
  } catch (e) {
    errLog('Telegram send error', e?.response?.data || e?.message || e);
    return null;
  }
}

/* =====================================
 * DB helpers
 * ===================================*/
async function insertTokenRow(token, txSig, pid, verified = 0, metadata = null) {
  const db = await dbPromise;
  try {
    await db.run(
      'INSERT OR IGNORE INTO tokens (token_address, first_seen, liquidity_tx, program_id, verified, metadata) VALUES (?, ?, ?, ?, ?, ?)',
      [token, Math.floor(Date.now() / 1000), txSig || null, pid || null, verified ? 1 : 0, metadata ? JSON.stringify(metadata) : null]
    );
  } catch (e) {
    errLog('insertTokenRow error', e?.message || e);
  }
}
async function markTokenNotified(token) {
  const db = await dbPromise;
  await db.run('UPDATE tokens SET notified=1 WHERE token_address=?', [token]);
}
async function markTokenVerified(token, metadata) {
  const db = await dbPromise;
  await db.run('UPDATE tokens SET verified=1, metadata=? WHERE token_address=?', [metadata ? JSON.stringify(metadata) : null, token]);
  await db.run('INSERT OR REPLACE INTO verified_mints (token_address, verified_at) VALUES (?, ?)', [token, Math.floor(Date.now() / 1000)]);
}
async function isTokenNotified(token) {
  const db = await dbPromise;
  const r = await db.get('SELECT notified FROM tokens WHERE token_address=?', [token]);
  return !!(r && r.notified);
}
async function isTokenVerifiedInDB(token) {
  const db = await dbPromise;
  const r = await db.get('SELECT verified FROM tokens WHERE token_address=?', [token]);
  if (r && r.verified) return true;
  const v = await db.get('SELECT verified_at FROM verified_mints WHERE token_address=?', [token]).catch(() => null);
  return !!v;
}

/* =====================================
 * Concurrency helpers
 * ===================================*/
function scheduleVerify(fn) {
  return new Promise((resolve, reject) => {
    verifyQueue.push({ fn, resolve, reject });
    processVerifyQueue();
  });
}
async function processVerifyQueue() {
  const cap = isCircuitOpen() ? Math.max(1, Math.floor(VERIFY_CONCURRENCY / 2)) : VERIFY_CONCURRENCY;
  if (activeVerifications >= cap) return;
  const job = verifyQueue.shift();
  if (!job) return;
  activeVerifications++;
  try {
    const r = await job.fn();
    job.resolve(r);
  } catch (e) {
    job.reject(e);
  } finally {
    activeVerifications--;
    setImmediate(processVerifyQueue);
  }
}
function acquireInspectSlot() {
  return new Promise(resolve => {
    const cap = isCircuitOpen() ? Math.max(1, Math.floor(FALLBACK_CONCURRENCY / 2)) : FALLBACK_CONCURRENCY;
    if (activeInspectors < cap) {
      activeInspectors++;
      return resolve();
    }
    inspectQueue.push(resolve);
  });
}
function releaseInspectSlot() {
  activeInspectors = Math.max(0, activeInspectors - 1);
  const next = inspectQueue.shift();
  if (next) {
    activeInspectors++;
    next();
  }
}

/* =====================================
 * RPC helpers with retries & backoff
 * ===================================*/
async function httpPostWithRetry(url, body, retries = VERIFY_RETRIES) {
  let attempt = 0;
  let lastErr = null;
  const roster = [url, ...RPC_ALT_URLS];
  let endpointIdx = 0;
  while (attempt <= retries) {
    if (typeof acquireRpcToken === 'function') await acquireRpcToken();
    if (isCircuitOpen()) await sleep(currentThrottleDelay(0));
    const endpoint = roster[endpointIdx % roster.length];
    try {
      const res = await axios.post(endpoint, body, { timeout: 15000, headers: { 'content-type': 'application/json' } });
      return res.data;
    } catch (e) {
      lastErr = e;
      const status = e?.response?.status;
      if (status === 429) mark429();
      const isNet = !status;
      if (status === 429 || isNet || status >= 500) endpointIdx++;
      attempt++;
      const baseDelay = VERIFY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
      const wait = currentThrottleDelay(baseDelay);
      errLog('RPC error', status ? `HTTP ${status}` : (e?.message || e), 'attempt', attempt, 'waiting', wait);
      await sleep(jitter(wait));
    }
  }
  throw lastErr;
}

/* =====================================
 * Solscan API helpers (optional)
 * ===================================*/
async function solscanGetChainInfo() {
  if (!SOLSCAN_API_KEY) return null;
  try {
    const res = await axios.get('https://pro-api.solscan.io/v2.0/chaininfo', {
      headers: { token: SOLSCAN_API_KEY },
      timeout: 15000
    });
    return res.data;
  } catch (e) {
    debugLog('Solscan chaininfo error', e?.message || e);
    return null;
  }
}
async function solscanGetTokenMetadata(mint) {
  if (!SOLSCAN_API_KEY || !mint) return null;
  try {
    const url = `https://pro-api.solscan.io/v2.0/token/meta?tokenAddress=${mint}`;
    const res = await axios.get(url, { headers: { token: SOLSCAN_API_KEY }, timeout: 15000 });
    return res.data;
  } catch (e) {
    debugLog('Solscan metadata error', e?.message || e);
    return null;
  }
}

/* =====================================
 * Verification & metadata
 * ===================================*/
async function getTransactionParsed(sig) {
  if (!RPC_URL) return null;
  try {
    const body = { jsonrpc: '2.0', id: 1, method: 'getTransaction', params: [sig, { encoding: 'jsonParsed', commitment: RPC_COMMITMENT }] };
    const data = await httpPostWithRetry(RPC_URL, body);
    return data?.result || null;
  } catch (e) {
    debugLog('getTransactionParsed error', e?.message || e);
    return null;
  }
}
async function getAccountInfoParsed(addr) {
  if (!RPC_URL) return null;
  try {
    const body = { jsonrpc: '2.0', id: 1, method: 'getAccountInfo', params: [addr, { encoding: 'jsonParsed', commitment: RPC_COMMITMENT }] };
    const data = await httpPostWithRetry(RPC_URL, body);
    return data?.result?.value || null;
  } catch (e) {
    debugLog('getAccountInfoParsed error', e?.message || e);
    return null;
  }
}
function isTodayInTZ(epochSec, tz) {
  try {
    const d = new Date(epochSec * 1000);
    const now = new Date();
    const opts = { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' };
    return d.toLocaleDateString('en-CA', opts) === now.toLocaleDateString('en-CA', opts);
  } catch {
    return false;
  }
}
function isFreshByAge(epochSec) {
  if (!MAX_AGE_SECONDS || MAX_AGE_SECONDS <= 0) return true;
  const age = Math.floor(Date.now() / 1000) - (epochSec || 0);
  return age >= 0 && age <= MAX_AGE_SECONDS;
}
async function verifyMintViaTransaction(sig, candidate) {
  const tx = await getTransactionParsed(sig);
  if (!tx) return { ok: false };
  const blockTime = tx.blockTime || null;
  if (ONLY_TODAY && blockTime && !isTodayInTZ(blockTime, USER_TZ)) return { ok: false };
  if (!isFreshByAge(blockTime || Math.floor(Date.now() / 1000))) return { ok: false };
  const meta = tx.meta || {};
  const post = meta.postTokenBalances || [];
  for (const p of post) {
    if (p && p.mint === candidate) return { ok: true, metadata: { source: 'postTokenBalances', info: p } };
  }
  const instructions = tx.transaction?.message?.instructions || [];
  for (const ins of instructions) {
    const parsed = ins.parsed || {};
    const info = parsed.info || {};
    if (info.mint === candidate || info.tokenMint === candidate || info.account === candidate) {
      return { ok: true, metadata: { source: 'instruction', info: parsed } };
    }
  }
  return { ok: false };
}
async function verifyMintViaAccount(addr) {
  const val = await getAccountInfoParsed(addr);
  if (!val) return { ok: false };
  const parsed = val.data?.parsed;
  if (parsed && parsed.type === 'mint') return { ok: true, metadata: { source: 'account', info: parsed } };
  return { ok: false };
}
async function verifyCandidate(sig, candidate) {
  if (!candidate) return { ok: false };
  if (verifiedCache.has(candidate)) return { ok: true, source: 'cache' };
  if (await isTokenVerifiedInDB(candidate)) {
    verifiedCache.set(candidate, { verifiedAt: Date.now() });
    return { ok: true, source: 'db' };
  }
  return scheduleVerify(async () => {
    // Try transaction verification first
    if (sig) {
      for (let i = 0; i < VERIFY_RETRIES; i++) {
        const r = await verifyMintViaTransaction(sig, candidate);
        if (r.ok) {
          verifiedCache.set(candidate, { verifiedAt: Date.now() });
          await markTokenVerified(candidate, r.metadata);
          return { ok: true, method: 'transaction', metadata: r.metadata };
        }
        const base = VERIFY_BASE_DELAY_MS * Math.pow(2, i);
        await sleep(currentThrottleDelay(base));
      }
    }
    // Fallback to account inspection
    for (let i = 0; i < VERIFY_RETRIES; i++) {
      const r2 = await verifyMintViaAccount(candidate);
      if (r2.ok) {
        verifiedCache.set(candidate, { verifiedAt: Date.now() });
        await markTokenVerified(candidate, r2.metadata);
        return { ok: true, method: 'account', metadata: r2.metadata };
      }
      const base = VERIFY_BASE_DELAY_MS * Math.pow(2, i);
      await sleep(currentThrottleDelay(base));
    }
    return { ok: false };
  });
}

/* =====================================
 * Candidate extraction and helpers
 * ===================================*/
const reBase58 = /([1-9A-HJ-NP-Za-km-z]{32,44})/g;
function extractCandidatesFromLogs(logs) {
  const scores = new Map();
  for (const raw of logs || []) {
    if (typeof raw !== 'string') continue;
    if (!/[1-9A-HJ-NP-Za-km-z]/.test(raw)) continue;
    let m;
    while ((m = reBase58.exec(raw)) !== null) {
      const candidate = m[1];
      if (!candidate) continue;
      let score = scores.get(candidate) || 0;
      if (liquidityMarkers.some(rx => rx.test(raw))) score += 3;
      for (const pid of PROGRAM_IDS) {
        if (pid && raw.includes(pid)) { score += 2; break; }
      }
      if (/Program log:|invoke \[|success/i.test(raw)) score += 1;
      scores.set(candidate, Math.max(scores.get(candidate) || 0, score));
    }
  }
  return [...scores.entries()].sort((a, b) => b[1] - a[1]).map(([candidate, score]) => ({ candidate, score }));
}
function findProgramInLogs(logs) {
  if (!Array.isArray(logs)) return null;
  for (const line of logs) {
    if (typeof line !== 'string') continue;
    for (const p of PROGRAMS) {
      if (p && p.id && line.includes(p.id)) return p.id;
    }
  }
  return null;
}

/* =====================================
 * Fallback inspector: programNotification pubkey -> signatures -> tx -> mint
 * ===================================*/
async function getRecentSignaturesForAddress(pubkey, limit = FALLBACK_SIGNATURE_LIMIT) {
  if (!RPC_URL) return [];
  try {
    const body = { jsonrpc: '2.0', id: 1, method: 'getSignaturesForAddress', params: [pubkey, { limit }] };
    const data = await httpPostWithRetry(RPC_URL, body);
    return Array.isArray(data?.result) ? data.result : [];
  } catch (e) {
    debugLog('getRecentSignaturesForAddress error', e?.message || e);
    return [];
  }
}
async function inspectSignaturesForMint(pubkey, limit = FALLBACK_SIGNATURE_LIMIT) {
  try {
    const last = inspectingPubkeys.get(pubkey);
    if (last && (Date.now() - last) < INSPECT_TTL_MS) {
      debugLog('inspectSignaturesForMint: skipping TTL for', pubkey);
      return false;
    }
    inspectingPubkeys.set(pubkey, Date.now());
    await acquireInspectSlot();
    try {
      const sigRows = await getRecentSignaturesForAddress(pubkey, limit);
      if (!sigRows.length) return false;
      for (const row of sigRows) {
        const signature = row?.signature || (typeof row === 'string' ? row : null);
        if (!signature) continue;
        const tx = await getTransactionParsed(signature);
        if (!tx) continue;
        const blockTime = tx.blockTime || null;
        if (ONLY_TODAY && blockTime && !isTodayInTZ(blockTime, USER_TZ)) continue;
        if (!isFreshByAge(blockTime || Math.floor(Date.now() / 1000))) continue;
        // Optionally use Solscan for chain info (we can call and log but not required)
        // 1) Check postTokenBalances
        const post = tx.meta?.postTokenBalances || [];
        for (const p of post) {
          if (p && p.mint) {
            const mint = p.mint;
            const pid = findProgramInLogs(tx.meta?.logMessages || []) || null;
            await insertTokenRow(mint, signature, pid, 0, { source: 'fallback-post', info: p });
            await enqueueNotification(mint, signature, pid);
            return true;
          }
        }
        // 2) Logs
        const logs = tx.meta?.logMessages || [];
        const candidates = extractCandidatesFromLogs(logs);
        if (candidates && candidates.length) {
          const top = candidates[0].candidate;
          const pid = findProgramInLogs(logs) || null;
          await insertTokenRow(top, signature, pid, 0, { source: 'fallback-log', score: candidates[0].score });
          await enqueueNotification(top, signature, pid);
          return true;
        }
      }
      return false;
    } finally {
      releaseInspectSlot();
      inspectingPubkeys.set(pubkey, Date.now());
    }
  } catch (e) {
    debugLog('inspectSignaturesForMint error', e?.message || e);
    return false;
  }
}

/* =====================================
 * Notification queue
 * ===================================*/
async function enqueueNotification(token, signature, pid) {
  await insertTokenRow(token, signature, pid, 0, null);
  if (!notifyQueue.some(x => x.token === token)) {
    notifyQueue.push({ token, signature, pid, ts: Date.now() });
  }
  processNotifyQueue();
}
async function processNotifyQueue() {
  if (notifyProcessing) return;
  notifyProcessing = true;
  try {
    while (notifyQueue.length > 0) {
      const item = notifyQueue.shift();
      const { token, signature, pid, ts } = item;
      const delay = DELAY_SECONDS - (Date.now() - ts);
      if (delay > 0) await sleep(delay);
      if (await isTokenNotified(token)) { debugLog('Already notified', token); continue; }
      if (VERIFY_MINT_BEFORE_NOTIFY) {
        const v = await verifyCandidate(signature, token);
        if (!v.ok) { warn('Verification failed for', token); continue; }
      }
      const programLabel = PROGRAMS.find(p => p.id === pid)?.name || pid || 'Unknown';
      const msg = `🚀 <b>New liquidity/pool detected!</b>\n\nDEX / Program: <b>${programLabel}</b>\nMint: <code>${token}</code>\nTx: <a href="https://solscan.io/tx/${signature}">Link</a>`;
      const sent = await sendTelegram(msg);
      if (sent && sent.ok) {
        await markTokenNotified(token);
        log('Notification sent for', token);
      } else {
        warn('Telegram failed for', token);
        notifyQueue.push({ token, signature, pid, ts: Date.now() + 30000 });
        await sleep(1000);
      }
    }
  } catch (e) {
    errLog('processNotifyQueue error', e?.message || e);
  } finally {
    notifyProcessing = false;
  }
}

/* =====================================
 * Event processing
 * ===================================*/
async function processEvent(evt) {
  try {
    if (!evt) return;
    // Program notifications may come without signature
    if (evt.value && evt.value.pubkey && !evt.signature && !evt.txSignature) {
      debugLog('Received programNotification pubkey', evt.value.pubkey);
      await inspectSignaturesForMint(evt.value.pubkey).catch(() => {});
      return;
    }
    const signature = evt.signature || evt.txSignature || (evt.transaction && evt.transaction.signatures && evt.transaction.signatures[0]);
    if (!signature) return;
    if (dumpedTxs.has(signature)) return;
    dumpedTxs.add(signature);
    setTimeout(() => dumpedTxs.delete(signature), 60000);
    debugLog('Processing event', signature);
    // Structured payloads
    if (evt.tokenTransfers && evt.tokenTransfers.length) {
      const mints = new Set();
      for (const t of evt.tokenTransfers) if (t.mint) mints.add(t.mint);
      const pid = findProgramInLogs(evt.logs) || null;
      for (const mint of mints) {
        await insertTokenRow(mint, signature, pid, 0, null);
        await enqueueNotification(mint, signature, pid);
      }
      return;
    }
    if (evt.events && evt.events.amm) {
      try {
        const amm = evt.events.amm;
        const tokenA = amm.tokenA?.mint || amm.tokenA?.token?.mint;
        const tokenB = amm.tokenB?.mint || amm.tokenB?.token?.mint;
        const pid = findProgramInLogs(evt.logs) || null;
        if (tokenA) { await insertTokenRow(tokenA, signature, pid, 0, null); await enqueueNotification(tokenA, signature, pid); }
        if (tokenB) { await insertTokenRow(tokenB, signature, pid, 0, null); await enqueueNotification(tokenB, signature, pid); }
        return;
      } catch (e) {
        debugLog('AMM parse error', e?.message || e);
      }
    }
    if (!evt.logs || !Array.isArray(evt.logs)) return;
    // Owner whitelist check
    if (EFFECTIVE_WHITELIST.length) {
      const inWl = evt.logs.some(line => typeof line === 'string' && EFFECTIVE_WHITELIST.some(pid => pid && line.includes(pid)));
      if (!inWl) return;
    }
    // Program IDs filter
    if (PROGRAM_IDS.length) {
      const hasProg = evt.logs.some(line => typeof line === 'string' && PROGRAM_IDS.some(pid => pid && line.includes(pid)));
      if (!hasProg) return;
    }
    // Liquidity markers
    const hasMark = evt.logs.some(l => liquidityMarkers.some(rx => rx.test(String(l))));
    if (!hasMark) return;
    debugLog('Liquidity markers found for', signature);
    const candidates = extractCandidatesFromLogs(evt.logs);
    if (!candidates.length) return;
    for (const { candidate } of candidates) {
      const v = await verifyCandidate(signature, candidate);
      if (v.ok) {
        const pid = findProgramInLogs(evt.logs);
        await insertTokenRow(candidate, signature, pid, 1, v.metadata || null);
        await enqueueNotification(candidate, signature, pid);
        log('Verified & notified', candidate);
        return;
      }
    }
  } catch (e) {
    errLog('processEvent error', e?.message || e);
  }
}

/* =====================================
 * WebSocket connection & subscription
 * ===================================*/
function getWsUrl() {
  if (HELIUS_API_KEY) return `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
  if (RPC_URL) return RPC_URL.replace(/^http/, 'ws');
  return 'wss://mainnet.helius-rpc.com';
}
let wsInstance = null;
function connectWS() {
  const wsUrl = getWsUrl();
  if (!wsUrl) { errLog('Missing WS URL'); return; }
  log('Connecting WebSocket', wsUrl);
  wsInstance = new WebSocket(wsUrl, { handshakeTimeout: 20000 });
  wsInstance.on('open', () => {
    log('WS connected');
    try {
      if (USE_LOGS_MENTIONS && PROGRAM_IDS.length) {
        let subId = 2000;
        for (const pid of PROGRAM_IDS) {
          if (!isBase58OrSystem(pid)) continue;
          wsInstance.send(JSON.stringify({ jsonrpc: '2.0', id: subId++, method: 'logsSubscribe', params: [{ mentions: [pid] }, { commitment: WS_COMMITMENT }] }));
        }
      } else if (USE_PROGRAM_SUBSCRIBE && PROGRAM_IDS.length) {
        let subId = 3000;
        for (const pid of PROGRAM_IDS) {
          if (!isBase58OrSystem(pid)) continue;
          wsInstance.send(JSON.stringify({ jsonrpc: '2.0', id: subId++, method: 'programSubscribe', params: [pid, { commitment: WS_COMMITMENT }] }));
        }
      } else if (USE_LOGS_ALL) {
        wsInstance.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'logsSubscribe', params: ['all', { commitment: WS_COMMITMENT }] }));
      } else {
        // Smart default: programSubscribe if programs defined, else logsSubscribe all
        if (PROGRAM_IDS.length) {
          let subId = 4000;
          for (const pid of PROGRAM_IDS) {
            if (!isBase58OrSystem(pid)) continue;
            wsInstance.send(JSON.stringify({ jsonrpc: '2.0', id: subId++, method: 'programSubscribe', params: [pid, { commitment: WS_COMMITMENT }] }));
          }
        } else {
          wsInstance.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'logsSubscribe', params: ['all', { commitment: WS_COMMITMENT }] }));
        }
      }
    } catch (e) {
      errLog('Subscription error', e?.message || e);
    }
  });
  wsInstance.on('message', async raw => {
    try {
      const s = raw.toString();
      let data;
      try { data = JSON.parse(s); } catch { return; }
      if (data?.method === 'logsNotification') {
        await processEvent(data.params.result);
      } else if (data?.method === 'programNotification' && data.params?.result?.value) {
        const v = data.params.result.value;
        if (v.pubkey) {
          inspectSignaturesForMint(v.pubkey).catch(() => {});
        }
      } else if (data?.params?.result) {
        const evt = data.params.result;
        if (evt && (evt.tokenTransfers || evt.events || evt.logs)) await processEvent(evt);
      }
    } catch (e) {
      errLog('WS message error', e?.message || e);
    }
  });
  wsInstance.on('close', (code, reason) => {
    warn('WS closed; reconnecting...', code, reason && reason.toString());
    setTimeout(connectWS, 3000);
  });
  wsInstance.on('error', err => {
    errLog('WS error', err?.message || err);
  });
}

/* =====================================
 * Dashboard server
 * ===================================*/
function startDashboard() {
  const app = express();
  app.set('view engine', 'ejs');
  // Determine views directory
  const viewsDir = fs.existsSync(path.join(__dirname, 'views')) ? path.join(__dirname, 'views') : __dirname;
  app.set('views', viewsDir);
  app.get('/', async (req, res) => {
    try {
      const db = await dbPromise;
      const tokens = await db.all('SELECT * FROM tokens ORDER BY first_seen DESC LIMIT 500');
      res.render('index', { tokens });
    } catch (e) {
      res.status(500).send('DB error: ' + (e?.message || e));
    }
  });
  // Try binding to configured port; if busy, increment
  const preferred = PORT;
  const maxTries = 3;
  (function tryPort(p, left) {
    const server = app.listen(p, () => {
      log(`Dashboard running at http://localhost:${p}`);
    });
    server.on('error', err => {
      if (err && err.code === 'EADDRINUSE' && left > 0) {
        warn(`Port ${p} in use — trying ${p + 1}`);
        tryPort(p + 1, left - 1);
      } else {
        errLog('Express listen error', err?.message || err);
      }
    });
  })(preferred, maxTries);
}

/* =====================================
 * Main entry
 * ===================================*/
if (require.main === module) {
  // Start services
  connectWS();
  startDashboard();
  // Periodically poll Solscan chain info (optional) to warm API and log chain state
  if (SOLSCAN_API_KEY) {
    setInterval(async () => {
      const info = await solscanGetChainInfo();
      if (info && info.data) {
        debugLog('Solscan chain height', info.data.height);
      }
    }, 60000);
  }
}

