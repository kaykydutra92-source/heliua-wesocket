// node-app.js
// Robust Helius WebSocket -> SQLite -> Telegram detector + Express dashboard
// Works on Windows/PowerShell and Docker. Edit .env for keys & toggles.

const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const axios = require('axios');
const express = require('express');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
require('dotenv').config();

// ===== Config from .env =====
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const OWNER_WHITELIST = (process.env.OWNER_WHITELIST || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const HELIUS_API_KEY = process.env.HELIUS_API_KEY || '';
const RPC_URL =
  process.env.RPC_URL ||
  (HELIUS_API_KEY ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : '');

const PORT = Number(process.env.PORT || 3000);
const DELAY_SECONDS = Number(process.env.DELAY_SECONDS || 2) * 1000;

const VERIFY_MINT_BEFORE_NOTIFY = (process.env.VERIFY_MINT_BEFORE_NOTIFY || 'true') === 'true';
const VERIFY_CONCURRENCY = Number(process.env.VERIFY_CONCURRENCY || 5);
const VERIFY_RETRIES = Number(process.env.VERIFY_RETRIES || 3);
const VERIFY_BASE_DELAY_MS = Number(process.env.VERIFY_BASE_DELAY_MS || 500);

const USE_PROGRAM_SUBSCRIBE = (process.env.USE_PROGRAM_SUBSCRIBE || 'false') === 'true';
const USE_LOGS_MENTIONS = (process.env.USE_LOGS_MENTIONS || 'true') === 'true';
const USE_LOGS_ALL = (process.env.USE_LOGS_ALL || 'false') === 'true';

const FALLBACK_SIGNATURE_LIMIT = Number(process.env.FALLBACK_SIGNATURE_LIMIT || 5);
const FALLBACK_CONCURRENCY = Number(process.env.FALLBACK_CONCURRENCY || 2);
const FALLBACK_INSPECT_TTL_MS = Number(process.env.FALLBACK_INSPECT_TTL_MS || 60 * 1000);

const USER_TZ = process.env.USER_TZ || 'UTC';
const ONLY_TODAY = (process.env.ONLY_TODAY || 'false') === 'true';
const MAX_AGE_SECONDS = Number(process.env.MAX_AGE_SECONDS || 0); // 0 = disabled

const DEBUG = (process.env.DEBUG || 'false') === 'true';

// ===== programs.json (monitored programs) =====
const programsFile = path.join(__dirname, 'programs.json');
let PROGRAMS = [];
let PROGRAM_IDS = [];
let EFFECTIVE_WHITELIST = []; // computed below
function isValidBase58(s) {
  return typeof s === 'string' && /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(s);
}

function loadPrograms() {
  if (!fs.existsSync(programsFile)) {
    console.warn('programs.json not found — no program filtering will apply');
    PROGRAMS = [];
    PROGRAM_IDS = [];
    return;
  }
  try {
    const raw = fs.readFileSync(programsFile, 'utf8');
    const parsed = JSON.parse(raw);
    PROGRAMS = Array.isArray(parsed.programs) ? parsed.programs : [];
    const allIds = PROGRAMS.map(p => p.id).filter(Boolean);
    const invalidIds = allIds.filter(id => !isValidBase58(id));
    if (invalidIds.length > 0) {
      console.warn('Ignoring invalid program IDs:', invalidIds.join(', '));
    }
    PROGRAM_IDS = allIds.filter(isValidBase58);
    if (OWNER_WHITELIST.length > 0) {
      EFFECTIVE_WHITELIST = OWNER_WHITELIST.slice();
    } else {
      const set = new Set(PROGRAM_IDS.concat(['TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA']));
      EFFECTIVE_WHITELIST = [...set];
    }
    console.log(`Loaded ${PROGRAMS.length} program entries from programs.json`);
  } catch (e) {
    console.error('Failed to parse programs.json', e);
  }
}
loadPrograms();

// watch programs.json for changes
try {
  fs.watchFile(programsFile, { interval: 2000 }, (curr, prev) => {
    if (curr.mtimeMs !== prev.mtimeMs) {
      console.log('programs.json changed — reloading');
      loadPrograms();
    }
  });
} catch (_e) {
  // ignore if watcher unavailable
}

// ===== DB setup =====
const DB_FILE = path.join(__dirname, 'tokens.db');
const dbPromise = open({ filename: DB_FILE, driver: sqlite3.Database });

(async () => {
  const db = await dbPromise;
  await db.exec(`PRAGMA journal_mode=WAL;`);
  await db.exec(`PRAGMA busy_timeout=3000;`);

  await db.exec(`
    CREATE TABLE IF NOT EXISTS tokens (
      token_address TEXT PRIMARY KEY,
      first_seen INTEGER,
      liquidity_tx TEXT,
      program_id TEXT,
      notified INTEGER DEFAULT 0,
      verified INTEGER DEFAULT 0,
      metadata TEXT
    );
  `);
  // legacy cache table (used by isTokenVerifiedInDB as fallback)
  await db.exec(`
    CREATE TABLE IF NOT EXISTS verified_mints (
      token_address TEXT PRIMARY KEY,
      verified_at INTEGER
    );
  `);
})();

// ===== Utilities =====
const dumpedTxs = new Set();            // short-term dedupe of signatures
const verifiedCache = new Map();        // memory cache for verified mints
const verifyQueue = [];
let activeVerifications = 0;
const notifyQueue = [];
let notifyProcessing = false;

const liquidityMarkers = [
  /mint/i, /addliquidity/i, /createpool/i, /deposit/i, /creat pool/i, /initializepool/i, /addlp/i,
];

function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }
function debugLog(...args) { if (DEBUG) console.debug(new Date().toISOString(), '[DEBUG]', ...args); }
function log(...args) { console.log(new Date().toISOString(), ...args); }
function warn(...args) { console.warn(new Date().toISOString(), '[WARN]', ...args); }
function errLog(...args) { console.error(new Date().toISOString(), '[ERROR]', ...args); }

function sameLocalDay(epochSec, tz) {
  if (!epochSec) return false;
  const dNow = new Date();
  const dEvt = new Date(epochSec * 1000);
  const fmt = (d) => d.toLocaleDateString('en-CA', { timeZone: tz });
  return fmt(dNow) === fmt(dEvt);
}

function isFreshEnough(blockTimeSec) {
  if (!blockTimeSec || Number.isNaN(blockTimeSec)) return true;
  const now = Math.floor(Date.now() / 1000);
  if (ONLY_TODAY) {
    if (!sameLocalDay(blockTimeSec, USER_TZ)) return false;
  }
  if (MAX_AGE_SECONDS > 0) {
    if (now - blockTimeSec > MAX_AGE_SECONDS) return false;
  }
  return true;
}

// ===== Telegram =====
async function sendTelegram(text) {
  if (!BOT_TOKEN || !CHAT_ID) {
    warn('BOT_TOKEN or CHAT_ID missing — skipping Telegram send');
    return null;
  }
  try {
    const res = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: CHAT_ID, text, parse_mode: 'HTML', disable_web_page_preview: true },
      { timeout: 10000 }
    );
    debugLog('Telegram response', res.data);
    return res.data;
  } catch (e) {
    errLog('sendTelegram error', e?.response?.data || e?.message || e);
    return null;
  }
}

// ===== DB helpers =====
async function insertTokenRow(token_address, tx, program_id, verified = 0, metadata = null) {
  const db = await dbPromise;
  try {
    await db.run(
      'INSERT OR IGNORE INTO tokens (token_address, first_seen, liquidity_tx, program_id, verified, metadata) VALUES (?, ?, ?, ?, ?, ?)',
      [
        token_address,
        Math.floor(Date.now() / 1000),
        tx || null,
        program_id || null,
        verified ? 1 : 0,
        metadata ? JSON.stringify(metadata) : null
      ]
    );
    debugLog('insertTokenRow', token_address);
  } catch (e) {
    errLog('insertTokenRow error', e?.message || e);
  }
}
async function markTokenNotified(token_address) {
  const db = await dbPromise;
  await db.run('UPDATE tokens SET notified=1 WHERE token_address=?', [token_address]);
}
async function markTokenVerified(token_address, metadata) {
  const db = await dbPromise;
  await db.run(
    'UPDATE tokens SET verified=1, metadata=? WHERE token_address=?',
    [metadata ? JSON.stringify(metadata) : null, token_address]
  );
  // also record in verified_mints table
  await db.run(
    'INSERT OR REPLACE INTO verified_mints (token_address, verified_at) VALUES (?, ?)',
    [token_address, Math.floor(Date.now() / 1000)]
  );
}
async function isTokenNotified(token_address) {
  const db = await dbPromise;
  const r = await db.get('SELECT notified FROM tokens WHERE token_address=?', [token_address]);
  return !!(r && r.notified);
}
async function isTokenVerifiedInDB(token_address) {
  const db = await dbPromise;
  const r = await db.get('SELECT verified FROM tokens WHERE token_address=?', [token_address]);
  if (r && r.verified) return true;
  const v = await db.get('SELECT verified_at FROM verified_mints WHERE token_address=?', [token_address]).catch(() => null);
  return !!v;
}

// ===== Verification queue (limited concurrency) =====
function scheduleVerify(fn) {
  return new Promise((resolve, reject) => {
    verifyQueue.push({ fn, resolve, reject });
    processVerifyQueue();
  });
}
async function processVerifyQueue() {
  if (activeVerifications >= VERIFY_CONCURRENCY) return;
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

// RPC helpers with retries
async function httpPostWithRetry(url, body, retries = VERIFY_RETRIES) {
  let attempt = 0, lastErr;
  while (attempt <= retries) {
    try {
      const res = await axios.post(url, body, { timeout: 12000 });
      return res.data;
    } catch (e) {
      lastErr = e;
      attempt++;
      const wait = VERIFY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
      debugLog('httpPostWithRetry attempt', attempt, 'waiting', wait, e?.message || e);
      await sleep(wait);
    }
  }
  throw lastErr;
}

// Verify via getTransaction
async function verifyMintViaTransaction(sig, candidate) {
  if (!RPC_URL) return { ok: false };
  try {
    const body = {
      jsonrpc: '2.0',
      id: 1,
      method: 'getTransaction',
      params: [sig, { encoding: 'jsonParsed', commitment: 'finalized' }]
    };
    const data = await httpPostWithRetry(RPC_URL, body);
    const res = data?.result;
    if (!res) return { ok: false };

    const meta = res.meta || {};
    const blockTime = meta.blockTime || null;

    const post = meta.postTokenBalances || [];
    for (const p of post) {
      if (p && p.mint === candidate) {
        return { ok: true, metadata: { source: 'postTokenBalances', info: p, blockTime } };
      }
    }
    const tx = res.transaction || {};
    const instructions = tx.message?.instructions || [];
    for (const ins of instructions) {
      const parsed = ins.parsed || {};
      const info = parsed.info || {};
      if (info.mint === candidate || info.tokenMint === candidate || info.account === candidate) {
        return { ok: true, metadata: { source: 'instruction', info: parsed, blockTime } };
      }
    }
    return { ok: false };
  } catch (e) {
    debugLog('verifyMintViaTransaction error', e?.message || e);
    return { ok: false };
  }
}

// Verify via getAccountInfo
async function verifyMintViaAccount(addr) {
  if (!RPC_URL) return { ok: false };
  try {
    const body = {
      jsonrpc: '2.0',
      id: 1,
      method: 'getAccountInfo',
      params: [addr, { encoding: 'jsonParsed', commitment: 'finalized' }]
    };
    const data = await httpPostWithRetry(RPC_URL, body);
    const val = data?.result?.value;
    if (!val) return { ok: false };
    const parsed = val.data?.parsed;
    if (parsed && parsed.type === 'mint')
      return { ok: true, metadata: { source: 'account', info: parsed, blockTime: null } };
    return { ok: false };
  } catch (e) {
    debugLog('verifyMintViaAccount error', e?.message || e);
    return { ok: false };
  }
}

async function verifyCandidate(sig, candidate) {
  if (verifiedCache.has(candidate)) return { ok: true, source: 'cache' };
  if (await isTokenVerifiedInDB(candidate)) {
    verifiedCache.set(candidate, { verifiedAt: Date.now() });
    return { ok: true, source: 'db' };
  }

  return scheduleVerify(async () => {
    if (sig) {
      for (let i = 0; i < VERIFY_RETRIES; i++) {
        const r = await verifyMintViaTransaction(sig, candidate);
        if (r.ok) {
          verifiedCache.set(candidate, { verifiedAt: Date.now() });
          await markTokenVerified(candidate, r.metadata);
          return { ok: true, method: 'transaction', metadata: r.metadata };
        }
        await sleep(VERIFY_BASE_DELAY_MS * Math.pow(2, i));
      }
    }
    for (let i = 0; i < VERIFY_RETRIES; i++) {
      const r2 = await verifyMintViaAccount(candidate);
      if (r2.ok) {
        verifiedCache.set(candidate, { verifiedAt: Date.now() });
        await markTokenVerified(candidate, r2.metadata);
        return { ok: true, method: 'account', metadata: r2.metadata };
      }
      await sleep(VERIFY_BASE_DELAY_MS * Math.pow(2, i));
    }
    return { ok: false };
  });
}

// ===== Candidate extraction & helpers =====
const reBase58 = /([1-9A-HJ-NP-Za-km-z]{32,44})/g;

function extractCandidatesFromLogs(logs) {
  const scores = new Map();
  for (const raw of logs) {
    if (typeof raw !== 'string') continue;
    if (!/[1-9A-HJ-NP-Za-km-z]/.test(raw)) continue;
    let m;
    while ((m = reBase58.exec(raw)) !== null) {
      const candidate = m[1];
      if (!candidate) continue;
      let score = scores.get(candidate) || 0;
      if (liquidityMarkers.some((rx) => rx.test(raw))) score += 3;
      for (const pid of PROGRAM_IDS) {
        if (pid && raw.includes(pid)) {
          score += 2;
          break;
        }
      }
      if (/Program log:|invoke \[|success/i.test(raw)) score += 1;
      const prev = scores.get(candidate) || 0;
      scores.set(candidate, Math.max(prev, score));
    }
  }
  return [...scores.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([candidate, score]) => ({ candidate, score }));
}

function findProgramInLogs(logs) {
  if (!Array.isArray(logs)) return null;
  for (const line of logs) {
    if (typeof line !== 'string') continue;
    for (const p of PROGRAMS) {
      if (!p || !p.id) continue;
      if (line.includes(p.id)) return p.id;
    }
  }
  return null;
}

// ===== Fallback inspector: programNotification pubkey -> recent signatures -> mint =====
const inspectingPubkeys = new Map();
let activeInspectors = 0;
const inspectQueue = [];

function acquireInspectSlot() {
  return new Promise((resolve) => {
    if (activeInspectors < FALLBACK_CONCURRENCY) {
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

async function getRecentSignaturesForAddress(pubkey, limit = FALLBACK_SIGNATURE_LIMIT) {
  if (!RPC_URL) return [];
  const body = {
    jsonrpc: '2.0',
    id: 1,
    method: 'getSignaturesForAddress',
    params: [pubkey, { limit }]
  };
  try {
    const data = await httpPostWithRetry(RPC_URL, body).catch(() => null);
    return data?.result || [];
  } catch (e) {
    debugLog('getRecentSignaturesForAddress error', e?.message || e);
    return [];
  }
}

async function inspectSignaturesForMint(pubkey, limit = FALLBACK_SIGNATURE_LIMIT) {
  try {
    const last = inspectingPubkeys.get(pubkey);
    if (last && (Date.now() - last) < FALLBACK_INSPECT_TTL_MS) {
      debugLog('inspectSignaturesForMint: recently inspected', pubkey);
      return false;
    }
    inspectingPubkeys.set(pubkey, Date.now());

    await acquireInspectSlot();
    try {
      const sigRows = await getRecentSignaturesForAddress(pubkey, limit);
      if (!Array.isArray(sigRows) || sigRows.length === 0) {
        debugLog('inspectSignaturesForMint: no signatures', pubkey);
        return false;
      }

      for (const row of sigRows) {
        const signature =
          row?.signature || row?.sig || (typeof row === 'string' ? row : null);
        if (!signature) continue;

        try {
          const body = {
            jsonrpc: '2.0', id: 1, method: 'getTransaction',
            params: [signature, { encoding: 'jsonParsed', commitment: 'finalized' }]
          };
          const txData = await httpPostWithRetry(RPC_URL, body).catch(() => null);
          const tx = txData?.result;
          if (!tx) continue;

          const blockTime = tx.meta?.blockTime || null;

          // (1) postTokenBalances best source
          const post = tx.meta?.postTokenBalances || [];
          if (post && post.length > 0) {
            for (const p of post) {
              if (p && p.mint && isFreshEnough(blockTime)) {
                const mint = p.mint;
                const prog =
                  findProgramInLogs(tx.meta?.logMessages || []) ||
                  findProgramInLogs(
                    (tx.transaction?.message?.instructions || [])
                      .map(i => i?.programId?.toString?.() || '')
                      .filter(Boolean)
                  ) || null;
                debugLog('inspectSignaturesForMint: postTokenBalances', mint, signature, 'program', prog);
                await insertTokenRow(mint, signature, prog, 0, { source: 'postTokenBalances', info: p });
                await enqueueNotification(mint, signature, prog, { blockTime });
                return true;
              }
            }
          }

          // (2) logs candidate fallback
          const logs = tx.meta?.logMessages || tx.meta?.logMessage || [];
          if (Array.isArray(logs) && logs.length > 0) {
            const candidates = extractCandidatesFromLogs(logs);
            if (candidates && candidates.length && isFreshEnough(blockTime)) {
              const top = candidates[0].candidate;
              const prog = findProgramInLogs(logs) || null;
              debugLog('inspectSignaturesForMint: logs-candidate', top, signature, 'program', prog);
              await insertTokenRow(top, signature, prog, 0, { source: 'log-candidate', score: candidates[0].score });
              await enqueueNotification(top, signature, prog, { blockTime });
              return true;
            }
          }

          // (3) parse instructions for mint fields
          const instructions = tx.transaction?.message?.instructions || [];
          for (const ins of instructions) {
            const parsed = ins?.parsed || ins?.data || {};
            const info = parsed?.info || {};
            const possible = info?.mint || info?.tokenMint || info?.account;
            if (possible && typeof possible === 'string' && reBase58.test(possible) && isFreshEnough(blockTime)) {
              const prog = findProgramInLogs(tx.meta?.logMessages || []) || null;
              debugLog('inspectSignaturesForMint: instruction field', possible, signature, 'program', prog);
              await insertTokenRow(possible, signature, prog, 0, { source: 'instruction', info: parsed });
              await enqueueNotification(possible, signature, prog, { blockTime });
              return true;
            }
          }
        } catch (txErr) {
          debugLog('inspectSignaturesForMint getTransaction error', txErr?.message || txErr);
        }
      }

      debugLog('inspectSignaturesForMint: no candidate for', pubkey);
      return false;
    } finally {
      releaseInspectSlot();
      inspectingPubkeys.set(pubkey, Date.now());
    }
  } catch (e) {
    debugLog('inspectSignaturesForMint top-level error', e?.message || e);
    return false;
  }
}

// ===== Notification queue =====
async function enqueueNotification(token, signature, program_id, opts = {}) {
  const blockTime = opts.blockTime ?? null;
  if (blockTime && !isFreshEnough(blockTime)) {
    debugLog('enqueueNotification: dropped by time filter', token, signature, blockTime);
    return;
  }
  await insertTokenRow(token, signature, program_id, 0, opts.metadata || null);
  if (!notifyQueue.some(q => q.token === token && q.signature === signature)) {
    notifyQueue.push({ token, signature, program_id, ts: Date.now(), blockTime });
  }
  processNotifyQueue();
}

async function processNotifyQueue() {
  if (notifyProcessing) return;
  notifyProcessing = true;
  try {
    while (notifyQueue.length > 0) {
      const item = notifyQueue.shift();
      const { token, signature, program_id, ts, blockTime } = item;
      const delayLeft = DELAY_SECONDS - (Date.now() - ts);
      if (delayLeft > 0) await sleep(delayLeft);
      if (blockTime && !isFreshEnough(blockTime)) {
        debugLog('processNotifyQueue: dropped by time filter', token, signature, blockTime);
        continue;
      }

      const alreadyNotified = await isTokenNotified(token);
      if (alreadyNotified) { debugLog('already notified', token); continue; }

      if (VERIFY_MINT_BEFORE_NOTIFY) {
        const v = await verifyCandidate(signature, token);
        if (!v.ok) { warn('Verification failed, skipping notify for', token); continue; }
      }

      const programLabel = PROGRAMS.find(p => p.id === program_id)?.name || program_id || 'Unknown';
      const message =
        `🚀 <b>New SOL token listed!</b>\n\n` +
        `DEX / Program: <b>${programLabel}</b>\n` +
        `Mint: <code>${token}</code>\n` +
        `Tx: <a href="https://solscan.io/tx/${signature}">Link</a>`;

      const sent = await sendTelegram(message);
      if (sent && sent.ok) {
        await markTokenNotified(token);
        log('Notification sent for', token);
      } else {
        warn('Telegram failed for', token, '- requeueing');
        notifyQueue.push({ token, signature, program_id, ts: Date.now() + 30 * 1000, blockTime });
        await sleep(1000);
      }
    }
  } catch (e) {
    errLog('processNotifyQueue error', e?.message || e);
  } finally {
    notifyProcessing = false;
  }
}

// ===== processEvent (core) =====
async function processEvent(evt) {
  try {
    if (!evt) return;

    // programSubscribe notifications (account/pubkey only, no signature)
    if (evt.value && evt.value.pubkey && !evt.signature && !evt.txSignature) {
      debugLog('programNotification pubkey received, inspecting…', evt.value.pubkey);
      await inspectSignaturesForMint(evt.value.pubkey).catch(() => {});
      return;
    }

    const signature =
      evt.signature ||
      evt.txSignature ||
      (evt.transaction && evt.transaction.signatures && evt.transaction.signatures[0]);
    if (!signature) return;
    if (dumpedTxs.has(signature)) return;
    dumpedTxs.add(signature);
    setTimeout(() => dumpedTxs.delete(signature), 60 * 1000);

    debugLog('Processing event', signature, 'slot', evt?.context?.slot);

    // Enhanced structured payloads (tokenTransfers / events)
    if (evt.tokenTransfers && Array.isArray(evt.tokenTransfers) && evt.tokenTransfers.length > 0) {
      const mints = new Set();
      for (const t of evt.tokenTransfers) if (t.mint) mints.add(t.mint);
      for (const mint of mints) {
        const prog = findProgramInLogs(evt.logs) || null;
        await insertTokenRow(mint, signature, prog, 0, null);
        await enqueueNotification(mint, signature, prog);
      }
      return;
    }
    if (evt.events && evt.events.amm) {
      try {
        const amm = evt.events.amm;
        const tokenA = amm.tokenA?.mint || amm.tokenA?.token?.mint;
        const tokenB = amm.tokenB?.mint || amm.tokenB?.token?.mint;
        const prog = findProgramInLogs(evt.logs) || null;
        if (tokenA) { await insertTokenRow(tokenA, signature, prog, 0, null); await enqueueNotification(tokenA, signature, prog); }
        if (tokenB) { await insertTokenRow(tokenB, signature, prog, 0, null); await enqueueNotification(tokenB, signature, prog); }
        return;
      } catch (e) {
        debugLog('events.amm parse error', e?.message || e);
      }
    }

    // From here on we need logs
    if (!evt.logs || !Array.isArray(evt.logs)) {
      debugLog('Event has no logs — skipping', signature);
      return;
    }
    // 🔐 OWNER_WHITELIST CHECK (by log substring)
    if (Array.isArray(EFFECTIVE_WHITELIST) && EFFECTIVE_WHITELIST.length > 0) {
      const inWhitelist = evt.logs.some(
        (line) => typeof line === 'string' && EFFECTIVE_WHITELIST.some((pid) => pid && line.includes(pid))
      );
      if (!inWhitelist) {
        debugLog('Event not in EFFECTIVE_WHITELIST — skipping', signature);
        return;
      }
    }

    // Program filter
    if (PROGRAM_IDS.length > 0) {
      const hasProg = evt.logs.some(
        (line) => typeof line === 'string' && PROGRAM_IDS.some(pid => pid && line.includes(pid))
      );
      if (!hasProg) {
        debugLog('Event missing monitored program ids — skipping', signature);
        return;
      }
    }

    // Liquidity markers
    const hasLiquidityMarker = evt.logs.some(l => liquidityMarkers.some(rx => rx.test(String(l))));
    if (!hasLiquidityMarker) {
      debugLog('No liquidity marker — skipping', signature);
      return;
    }
    debugLog('Liquidity marker found for', signature);

    // Extract & score candidates
    const candidates = extractCandidatesFromLogs(evt.logs);
    if (!candidates || candidates.length === 0) {
      debugLog('No candidates extracted', signature);
      return;
    }

    // Try candidates in order, verify, store, notify
    for (const { candidate } of candidates) {
      debugLog('Candidate', candidate, 'for', signature);
      const v = await verifyCandidate(signature, candidate);
      if (v.ok) {
        const bt = v.metadata?.blockTime;
        if (bt && !isFreshEnough(bt)) {
          debugLog('Time filter drop inside processEvent', candidate, signature, bt);
          continue;
        }
        const matchedProgram = findProgramInLogs(evt.logs);
        await insertTokenRow(candidate, signature, matchedProgram, 1, v.metadata || null);
        await enqueueNotification(candidate, signature, matchedProgram, { blockTime: bt, metadata: v.metadata });
        log('Verified & enqueued', candidate, 'sig', signature, 'program', matchedProgram || 'none');
        return;
      } else {
        debugLog('Candidate verification failed for', candidate);
      }
    }

    debugLog('No verified candidate for', signature);
  } catch (e) {
    errLog('processEvent error', e?.message || e);
  }
}

// ===== WebSocket connection & subscription =====
function getWsUrl() {
  if (HELIUS_API_KEY) return `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
  if (RPC_URL) return RPC_URL.replace(/^http/, 'ws');
  return 'wss://mainnet.helius-rpc.com';
}

let wsInstance = null;

function connectWS() {
  const wsUrl = getWsUrl();
  if (!wsUrl) {
    errLog('No WS URL configured');
    return;
  }
  log('Connecting WS to', wsUrl);
  wsInstance = new WebSocket(wsUrl, { handshakeTimeout: 20000 });

  wsInstance.on('open', () => {
    log('WebSocket connected — listening for new listings...');
    try {
      if (USE_PROGRAM_SUBSCRIBE && PROGRAM_IDS.length > 0) {
        // programSubscribe (one per program)
        PROGRAM_IDS.forEach((pid, idx) => {
          const req = {
            jsonrpc: '2.0',
            id: 1000 + idx,
            method: 'programSubscribe',
            params: [pid, { commitment: 'finalized' }]
          };
          wsInstance.send(JSON.stringify(req));
          debugLog('programSubscribe', pid);
        });
      } else if (USE_LOGS_MENTIONS && PROGRAM_IDS.length > 0) {
        // Helius "mentions" supports one address per subscription — fan out
        PROGRAM_IDS.forEach((pid, idx) => {
          const req = {
            jsonrpc: '2.0',
            id: 2000 + idx,
            method: 'logsSubscribe',
            params: [{ mentions: [pid] }, { commitment: 'finalized' }]
          };
          wsInstance.send(JSON.stringify(req));
          debugLog('logsSubscribe mentions', pid);
        });
      } else if (USE_LOGS_ALL) {
        // firehose (very noisy; for debugging)
        wsInstance.send(JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'logsSubscribe',
          params: ['all', { commitment: 'finalized' }]
        }));
        log('logsSubscribe all sent (USE_LOGS_ALL=true)');
      } else {
        // default safe fallback
        if (PROGRAM_IDS.length > 0) {
          PROGRAM_IDS.forEach((pid, idx) => {
            const req = {
              jsonrpc: '2.0',
              id: 3000 + idx,
              method: 'programSubscribe',
              params: [pid, { commitment: 'finalized' }]
            };
            wsInstance.send(JSON.stringify(req));
            debugLog('programSubscribe (smart default)', pid);
          });
        } else {
          wsInstance.send(JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            method: 'logsSubscribe',
            params: ['all', { commitment: 'finalized' }]
          }));
          log('logsSubscribe all sent (no program filters)');
        }
      }
    } catch (e) {
      errLog('subscribe error', e?.message || e);
    }
  });

  wsInstance.on('message', async (raw) => {
    try {
      const s = raw.toString();
      let data;
      try {
        data = JSON.parse(s);
      } catch {
        debugLog('Non-JSON ws message');
        return;
      }
      if (DEBUG) debugLog('WS message', JSON.stringify(data));

      // Direct logs notifications
      if (data?.method === 'logsNotification') {
        const evt = data.params.result;
        if (evt) await processEvent(evt);
        return;
      }

      // Program/account notifications sometimes arrive as { method: 'programNotification', params: { result: { value: {...} } } }
      if (data?.method === 'programNotification' && data?.params?.result?.value) {
        const v = data.params.result.value;
        // Try to recover mint via fallback (pubkey → recent signatures → transaction)
        if (v.pubkey) {
          inspectSignaturesForMint(v.pubkey, FALLBACK_SIGNATURE_LIMIT).catch(() => {});
        }
        return;
      }

      // Some endpoints send structured results in params.result
      const evt = data?.params?.result;
      if (evt && (evt.tokenTransfers || evt.events || evt.logs)) {
        await processEvent(evt);
        return;
      }

      // Some providers send subscription confirmations
      if (data?.result && typeof data.id !== 'undefined') {
        debugLog('WS subscription ack', data.id, data.result);
        return;
      }

      debugLog('WS other', data.method || data.id || 'unknown');
    } catch (e) {
      errLog('WS message handler error', e?.message || e);
    }
  });

  wsInstance.on('close', (code, reason) => {
    warn('WS closed — reconnecting in 3s', code, reason && reason.toString && reason.toString());
    setTimeout(connectWS, 3000);
  });
  wsInstance.on('error', e => {
    errLog('WS error', e?.message || e);
  });
}

// ===== Dashboard (Express + EJS) =====
function startDashboard() {
  const app = express();
  app.set('view engine', 'ejs');
  app.set('views', path.join(__dirname, 'views'));
  app.get('/', async (req, res) => {
    try {
      const db = await dbPromise;
      const tokens = await db.all('SELECT * FROM tokens ORDER BY first_seen DESC LIMIT 500');
      res.render('index', { tokens });
    } catch (e) {
      res.status(500).send('DB error: ' + (e?.message || e));
    }
  });
  const server = app.listen(PORT, () => log(`Dashboard running at http://localhost:${PORT}`));
  return server;
}

// Exports for testing & utilities
module.exports = { sendTelegram, processEvent, connectWS, startDashboard };

// Auto-run when not imported
if (require.main === module) {
  if (!RPC_URL && !HELIUS_API_KEY) {
    errLog('No RPC_URL or HELIUS_API_KEY set. WS will not connect.');
  }
  connectWS();
  const server = startDashboard();
  process.on('SIGINT', () => { log('SIGINT - shutting down'); try { server.close(); } catch (_e) {} process.exit(0); });
  process.on('SIGTERM', () => { log('SIGTERM - shutting down'); try { server.close(); } catch (_e) {} process.exit(0); });
}
