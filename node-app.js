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

async function solscanVerifyMint(mint) {
  if (!SOLSCAN_API_KEY || !mint) return { ok: false };
  try {
    const meta = await solscanGetTokenMetadata(mint);
    if (meta && meta.data) {
      return {
        ok: true,
        metadata: {
          source: 'solscan',
          info: {
            name: meta.data.name,
            symbol: meta.data.symbol,
            supply: meta.data.tokenInfo?.supply || null
          }
        }
      };
    }
  } catch (e) {
    debugLog('solscanVerifyMint error', e?.message || e);
  }
  return { ok: false };
}

async function getTokenSupply(mint) {
  if (!RPC_URL || !mint) return null;
  try {
    const body = {
      jsonrpc: '2.0',
      id: 1,
      method: 'getTokenSupply',
      params: [mint, { commitment: RPC_COMMITMENT }]
    };
    const data = await httpPostWithRetry(RPC_URL, body);
    return data?.result || null;
  } catch (e) {
    debugLog('getTokenSupply error', e?.message || e);
    return null;
  }
}

async function verifyMintViaTokenSupply(mint) {
  const res = await getTokenSupply(mint);
  if (!res) return { ok: false };
  const amount = Number(res.value?.amount || 0);
  if (Number.isFinite(amount) && amount >= 0) {
    return {
      ok: true,
      metadata: {
        source: 'tokenSupply',
        info: {
          amount,
          decimals: res.value?.decimals ?? null
        }
      }
    };
  }
  return { ok: false };
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
    if (p && p.mint === candidate) {
      return {
        ok: true,
        metadata: {
          source: 'postTokenBalances',
          info: p,
          blockTime
        }
      };
    }
  }
  const instructions = tx.transaction?.message?.instructions || [];
  for (const ins of instructions) {
    const parsed = ins.parsed || {};
    const info = parsed.info || {};
    if (info.mint === candidate || info.tokenMint === candidate || info.account === candidate) {
      return {
        ok: true,
        metadata: {
          source: 'instruction',
          info: parsed,
          blockTime
        }
      };
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
  if (!candidate || !isBase58OrSystem(candidate)) return { ok: false };
  if (verifiedCache.has(candidate)) return { ok: true, source: 'cache' };
  if (await isTokenVerifiedInDB(candidate)) {
    verifiedCache.set(candidate, { verifiedAt: Date.now() });
    return { ok: true, source: 'db' };
  }
  return scheduleVerify(async () => {
    const strategies = [
      {
        name: 'transaction',
        enabled: !!sig,
        retries: VERIFY_RETRIES,
        fn: () => verifyMintViaTransaction(sig, candidate)
      },
      {
        name: 'account',
        enabled: true,
        retries: Math.max(1, Math.floor(VERIFY_RETRIES / 2)),
        fn: () => verifyMintViaAccount(candidate)
      },
      {
        name: 'tokenSupply',
        enabled: true,
        retries: 1,
        fn: () => verifyMintViaTokenSupply(candidate)
      }
    ];
    if (SOLSCAN_API_KEY) {
      strategies.push({
        name: 'solscan',
        enabled: true,
        retries: 1,
        fn: () => solscanVerifyMint(candidate)
      });
    }
    for (const strat of strategies) {
      if (!strat.enabled) continue;
      const retries = Math.max(1, strat.retries || 1);
      for (let attempt = 0; attempt < retries; attempt++) {
        const res = await strat.fn();
        if (res && res.ok) {
          const metadata = {
            ...(res.metadata || {}),
            strategy: strat.name
          };
          verifiedCache.set(candidate, { verifiedAt: Date.now(), strategy: strat.name });
          await markTokenVerified(candidate, metadata);
          return { ok: true, method: strat.name, metadata };
        }
        const base = VERIFY_BASE_DELAY_MS * Math.pow(2, attempt);
        await sleep(currentThrottleDelay(base));
      }
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

function extractMintsFromTokenTransfers(evt, context) {
  const transfers = Array.isArray(evt?.tokenTransfers) ? evt.tokenTransfers : [];
  const derived = [];
  for (const transfer of transfers) {
    if (!transfer || !transfer.mint) continue;
    derived.push({
      mint: transfer.mint,
      programId: transfer.programId || context.primaryProgram || null,
      source: 'tokenTransfers',
      weight: 6,
      metadata: {
        from: transfer.fromUserAccount || transfer.fromTokenAccount || null,
        to: transfer.toUserAccount || transfer.toTokenAccount || null,
        amount: transfer.tokenAmount || transfer.amount || transfer.uiTokenAmount || null
      },
      autoVerified: true
    });
  }
  return derived;
}

function extractMintsFromAmmEvent(evt, context) {
  const amm = evt?.events?.amm;
  if (!amm) return [];
  const results = [];
  const tokens = [
    { mint: amm.tokenA?.mint || amm.tokenA?.token?.mint, label: 'tokenA' },
    { mint: amm.tokenB?.mint || amm.tokenB?.token?.mint, label: 'tokenB' }
  ];
  for (const tok of tokens) {
    if (!tok.mint) continue;
    results.push({
      mint: tok.mint,
      programId: context.primaryProgram || null,
      source: 'ammEvent',
      weight: 5,
      metadata: { side: tok.label },
      autoVerified: true
    });
  }
  return results;
}

function extractMintsFromBalanceChanges(evt, context) {
  const meta = evt?.meta || evt?.transactionMeta || {};
  const post = meta?.postTokenBalances || evt?.postTokenBalances || [];
  const pre = meta?.preTokenBalances || evt?.preTokenBalances || [];
  if (!Array.isArray(post) || post.length === 0) return [];
  const preMap = new Map();
  for (const entry of pre) {
    if (!entry?.mint) continue;
    const key = `${entry.mint}:${entry.owner || entry.accountIndex || 'unknown'}`;
    const amount = Number(entry.uiTokenAmount?.amount || 0);
    preMap.set(key, Number.isFinite(amount) ? amount : 0);
  }
  const results = [];
  for (const entry of post) {
    if (!entry?.mint) continue;
    const key = `${entry.mint}:${entry.owner || entry.accountIndex || 'unknown'}`;
    const amount = Number(entry.uiTokenAmount?.amount || 0);
    if (!Number.isFinite(amount)) continue;
    const prev = preMap.get(key) || 0;
    if (amount > prev) {
      results.push({
        mint: entry.mint,
        programId: context.primaryProgram || null,
        source: 'balanceChange',
        weight: 4 + Math.min(2, Math.log10(amount - prev + 1)),
        metadata: {
          delta: amount - prev,
          owner: entry.owner || null,
          accountIndex: entry.accountIndex ?? null
        },
        autoVerified: true
      });
    }
  }
  return results;
}

function extractMintsFromInstructions(evt, context) {
  const list = [];
  const outer = evt?.transaction?.message?.instructions || [];
  const inner = Array.isArray(evt?.meta?.innerInstructions)
    ? evt.meta.innerInstructions.flatMap(x => x?.instructions || [])
    : [];
  const provided = Array.isArray(evt?.instructions) ? evt.instructions : [];
  const combined = [...outer, ...inner, ...provided];
  for (const instruction of combined) {
    if (!instruction) continue;
    const parsed = instruction.parsed || {};
    const info = parsed.info || {};
    const programId = instruction.programId || instruction.programIdIndex || context.primaryProgram || null;
    const typeName = typeof parsed?.type === 'string' ? parsed.type : '';
    const isMintRelated = /mint|liquidity|create|initialize|pool/i.test(typeName || '');
    const possibleMints = [
      info.mint,
      info.tokenMint,
      isMintRelated && typeof info.account === 'string' ? info.account : null,
      info.sourceMint,
      info.destinationMint,
      info.poolMint,
      instruction.mint
    ].filter(Boolean);
    for (const mint of possibleMints) {
      list.push({
        mint,
        programId: typeof programId === 'string' ? programId : context.primaryProgram || null,
        source: 'instruction',
        weight: isMintRelated ? 4 : 2,
        metadata: {
          parsed: parsed?.type || null,
          infoKeys: Object.keys(info || {})
        }
      });
    }
    if (isMintRelated && Array.isArray(instruction.accounts)) {
      for (const acct of instruction.accounts) {
        if (typeof acct === 'string' && isBase58OrSystem(acct)) {
          list.push({
            mint: acct,
            programId: typeof programId === 'string' ? programId : context.primaryProgram || null,
            source: 'instructionAccount',
            weight: 2,
            metadata: { accountIndex: instruction.accounts.indexOf(acct) }
          });
        }
      }
    }
  }
  return list;
}

function extractMintsFromLogsWithPrograms(evt, context) {
  const logs = Array.isArray(context.logs) ? context.logs : [];
  if (!logs.length) return [];
  if (EFFECTIVE_WHITELIST.length) {
    const inWhitelist = logs.some(line => typeof line === 'string' && EFFECTIVE_WHITELIST.some(pid => pid && line.includes(pid)));
    if (!inWhitelist) return [];
  }
  if (PROGRAM_IDS.length) {
    const hasProgram = logs.some(line => typeof line === 'string' && PROGRAM_IDS.some(pid => pid && line.includes(pid)));
    if (!hasProgram) return [];
  }
  const hasMarker = logs.some(line => liquidityMarkers.some(rx => rx.test(String(line))));
  if (!hasMarker) return [];
  const candidates = extractCandidatesFromLogs(logs);
  return candidates.map(c => ({
    mint: c.candidate,
    programId: context.primaryProgram || null,
    source: 'logs',
    weight: 1 + c.score,
    metadata: {
      score: c.score,
      sampleLog: logs.slice(0, 3)
    }
  }));
}

const eventCandidateExtractors = [
  extractMintsFromTokenTransfers,
  extractMintsFromAmmEvent,
  extractMintsFromBalanceChanges,
  extractMintsFromInstructions,
  extractMintsFromLogsWithPrograms
];

function gatherMintCandidates(evt, signature) {
  const logs = Array.isArray(evt?.logs) ? evt.logs : (evt?.meta?.logMessages || []);
  const primaryProgram = findProgramInLogs(logs) || evt?.programId || null;
  const context = { signature, logs, primaryProgram };
  const aggregate = new Map();
  for (const extractor of eventCandidateExtractors) {
    let results = [];
    try {
      results = extractor(evt, context) || [];
    } catch (e) {
      debugLog('Extractor error', extractor.name || 'anonymous', e?.message || e);
      continue;
    }
    for (const item of results) {
      if (!item || !item.mint) continue;
      if (!isBase58OrSystem(item.mint) || item.mint === '11111111111111111111111111111111') continue;
      const key = item.mint;
      const existing = aggregate.get(key) || {
        mint: key,
        programIds: new Set(),
        weight: 0,
        sources: [],
        autoVerified: false
      };
      if (item.programId && typeof item.programId === 'string') existing.programIds.add(item.programId);
      existing.weight += item.weight || 1;
      if (existing.sources.length < 8) {
        existing.sources.push({
          source: item.source,
          metadata: item.metadata || null
        });
      }
      existing.autoVerified = existing.autoVerified || !!item.autoVerified;
      aggregate.set(key, existing);
    }
  }
  const candidates = [];
  for (const entry of aggregate.values()) {
    const programIds = Array.from(entry.programIds);
    const metadata = {
      signature,
      weight: entry.weight,
      sources: entry.sources,
      programIds
    };
    candidates.push({
      mint: entry.mint,
      programId: programIds[0] || primaryProgram || null,
      metadata,
      autoVerified: entry.autoVerified,
      weight: entry.weight
    });
  }
  return { candidates, context };
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
      let foundAny = false;
      for (const row of sigRows) {
        const signature = row?.signature || (typeof row === 'string' ? row : null);
        if (!signature) continue;
        const tx = await getTransactionParsed(signature);
        if (!tx) continue;
        const blockTime = tx.blockTime || null;
        if (ONLY_TODAY && blockTime && !isTodayInTZ(blockTime, USER_TZ)) continue;
        if (!isFreshByAge(blockTime || Math.floor(Date.now() / 1000))) continue;
        const syntheticEvent = {
          signature,
          logs: tx.meta?.logMessages || [],
          meta: tx.meta || {},
          transaction: tx.transaction || null,
          tokenTransfers: Array.isArray(tx.tokenTransfers)
            ? tx.tokenTransfers
            : (tx.meta?.postTokenBalances || []).map(p => ({
              mint: p?.mint,
              toUserAccount: p?.owner || null,
              tokenAmount: p?.uiTokenAmount?.amount || null,
              programId: findProgramInLogs(tx.meta?.logMessages || []) || null
            }))
        };
        const { candidates } = gatherMintCandidates(syntheticEvent, signature);
        if (!candidates.length) continue;
        for (const cand of candidates) {
          const pid = cand.programId || findProgramInLogs(syntheticEvent.logs) || null;
          const enrichedMeta = { ...(cand.metadata || {}), source: 'fallback' };
          await insertTokenRow(cand.mint, signature, pid, cand.autoVerified ? 1 : 0, enrichedMeta);
          if (cand.autoVerified) {
            verifiedCache.set(cand.mint, { verifiedAt: Date.now(), strategy: 'fallback' });
            await markTokenVerified(cand.mint, enrichedMeta);
          }
          await enqueueNotification(cand.mint, signature, pid);
          foundAny = true;
        }
      }
      return foundAny;
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
    const { candidates, context } = gatherMintCandidates(evt, signature);
    if (!candidates.length) return;
    for (const cand of candidates) {
      const pid = cand.programId || context.primaryProgram || findProgramInLogs(context.logs) || null;
      const metadata = {
        ...(cand.metadata || {}),
        detectionWeight: cand.weight,
        detectedAt: Date.now(),
        origin: (cand.metadata && cand.metadata.origin) || 'event'
      };
      await insertTokenRow(cand.mint, signature, pid, cand.autoVerified ? 1 : 0, metadata);
      if (cand.autoVerified) {
        verifiedCache.set(cand.mint, { verifiedAt: Date.now(), strategy: 'event' });
        await markTokenVerified(cand.mint, metadata);
      }
      await enqueueNotification(cand.mint, signature, pid);
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

