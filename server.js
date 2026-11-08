'use strict';
/*
 * stream-sync-node v3 — комбайн для Render:
 *   Cobalt (внутрішній) + yt-dlp + ffmpeg + Telegram-відправник (/send, /circle, /stickers) + X-вотчер.
 *
 * Що нового порівняно з v2:
 *  • /send ніколи не "мовчить": або відповідає result, або pending (довге завантаження доробляється у фоні),
 *    або чітка помилка з діагностикою по кожному рушію (cobalt / yt-dlp / tikwm / fxtwitter / innertube ...).
 *  • Мульти-рушій: якщо один не відповів — працює наступний. Для TikTok/YouTube/Instagram/X свої ланцюжки.
 *  • Стан X-вотчера (канали, акаунти, вже надіслані пости) зберігається в Telegram і переживає рестарт Render.
 *  • X-вотчер: кожні 15 с, надсилає ТІЛЬКИ нові відео, нічого — якщо нового немає. Тест = лише останні 2.
 *  • Ліміти Cobalt підняті (раніше всі користувачі ділили 20 запитів на хвилину на один IP 127.0.0.1).
 */

const http = require('node:http');
const fs = require('node:fs');
const fsp = fs.promises;
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');

const VERSION = '3.0.0';
const BOOT_AT = Date.now();

// ==================== 0. КОНФІГУРАЦІЯ ====================
const env = (k, d = '') => {
  const v = process.env[k];
  return (v === undefined || v === null || String(v).trim() === '') ? d : String(v).trim();
};
const envBool = (k, d = false) => {
  const v = env(k, '');
  if (v === '') return d;
  return !/^(0|false|no|off)$/i.test(v);
};
const envNum = (k, d) => {
  const n = Number(env(k, ''));
  return Number.isFinite(n) && n > 0 ? n : d;
};

const PORT = envNum('PORT', 10000);
const COBALT_PORT = envNum('COBALT_INTERNAL_PORT', 9000);
const LOCAL_API_PORT = envNum('LOCAL_BOT_API_PORT', 8081);
const API_KEY = env('API_KEY') || env('COBALT_API_KEY') || '8bfef237-aef7-4964-a01d-65cd9c2a7ef2';
const PUBLIC_URL = (env('RENDER_EXTERNAL_URL') || env('API_URL') || ('http://127.0.0.1:' + PORT)).replace(/\/+$/, '');
const TMP_DIR = env('TMP_DIR', '/tmp');
const MAX_JOBS = Math.max(1, Math.min(4, envNum('MAX_JOBS', 2)));
const SEND_WAIT_MS = envNum('SEND_WAIT_MS', 21000);        // скільки /send чекає до відповіді pending (бот має ліміт ~26 с)
const JOB_DEADLINE_MS = envNum('JOB_DEADLINE_MS', 9 * 60 * 1000);
const ADMIN_CHAT_ID = env('ADMIN_CHAT_ID', '1550849366');
const STATE_CHAT_ID = env('STATE_CHAT_ID', ADMIN_CHAT_ID);
const TG_API_BASE = env('TG_API_BASE', 'https://api.telegram.org').replace(/\/+$/, '');

const MB = 1024 * 1024;
const STD_TG_LIMIT = Math.floor(49.2 * MB);
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const YTDLP_BIN = env('YTDLP_BIN', '/usr/local/bin/yt-dlp');
const YT_COOKIES_RAW = env('YT_COOKIES_B64') ? Buffer.from(env('YT_COOKIES_B64'), 'base64').toString('utf8') : env('YT_COOKIES');
const COOKIES_ALL_RAW = env('COOKIES_B64') ? Buffer.from(env('COOKIES_B64'), 'base64').toString('utf8') : '';
const YTDLP_PROXY = env('YTDLP_PROXY');          // проксі для всіх сайтів у yt-dlp
const YT_PROXY = env('YT_PROXY') || env('PROXY_URL'); // проксі тільки для YouTube
const X_BEARER_TOKEN = env('X_BEARER_TOKEN');    // опційно: офіційний X API v2 як найнадійніше джерело

// Telegram Local Bot API
const RAW_API_ID = String(process.env.TELEGRAM_API_ID || '').replace(/['"\s]+/g, '');
const RAW_API_HASH = String(process.env.TELEGRAM_API_HASH || '').replace(/['"\s]+/g, '');
const API_ID_VALID = /^\d{5,12}$/.test(RAW_API_ID);
const API_HASH_VALID = /^[0-9a-fA-F]{32}$/.test(RAW_API_HASH);

let cobaltReady = false;
let cobaltLastError = '';
let cobaltRestarts = 0;
let localApiReady = false;
let localApiLastError = '';
let ytdlpVersion = '';
let ytdlpLastError = '';
let ffmpegOk = true;

// ==================== 1. ЛОГИ (кільцевий буфер для /logs) ====================
const LOG_RING = [];
function log(tag, ...args) {
  const line = new Date().toISOString().slice(11, 19) + ' [' + tag + '] ' + args.map(a => {
    if (a instanceof Error) return a.message;
    if (typeof a === 'string') return a;
    try { return JSON.stringify(a); } catch (e) { return String(a); }
  }).join(' ');
  LOG_RING.push(line);
  if (LOG_RING.length > 400) LOG_RING.shift();
  console.log(line);
}
const redact = s => String(s || '').replace(/\b\d{6,}:[A-Za-z0-9_-]{25,}\b/g, '<token>');

// ==================== 2. ДОПОМІЖНІ ФУНКЦІЇ ====================
const sleep = ms => new Promise(r => setTimeout(r, ms));

function withTimeout(promise, ms, label = 'op') {
  let t;
  const timeout = new Promise((_, rej) => { t = setTimeout(() => rej(new Error('TIMEOUT:' + label + ':' + ms)), ms); });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

// fetch із таймаутом на ВЕСЬ запит (включно з читанням тіла), повертає {status, ok, text, json, headers}
async function fetchJson(url, opts = {}, ms = 15000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(new Error('TIMEOUT ' + ms + 'ms')), ms);
  try {
    const r = await fetch(url, { ...opts, signal: ctrl.signal });
    const text = await r.text();
    let json = null;
    try { json = JSON.parse(text); } catch (e) {}
    return { status: r.status, ok: r.ok, text, json, headers: r.headers, url: r.url };
  } finally {
    clearTimeout(t);
  }
}

function fetchRaw(url, opts = {}, ms = 15000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(new Error('TIMEOUT ' + ms + 'ms')), ms);
  return fetch(url, { ...opts, signal: ctrl.signal }).finally(() => clearTimeout(t));
}

function tmpName(ext) {
  return path.join(TMP_DIR, 'ssn_' + Date.now().toString(36) + '_' + crypto.randomBytes(4).toString('hex') + (ext.startsWith('.') ? ext : '.' + ext));
}
function safeUnlink(f) { if (!f) return; try { fs.unlinkSync(f); } catch (e) {} }
async function safeRm(f) { if (!f) return; try { await fsp.rm(f, { recursive: true, force: true }); } catch (e) {} }
const fileSize = f => { try { return fs.statSync(f).size; } catch (e) { return 0; } };

function runProc(cmd, args, { timeoutMs = 120000, input = null, env: penv = null, onStderr = null, onStdout = null } = {}) {
  return new Promise((resolve, reject) => {
    let out = '';
    let err = '';
    let p;
    try {
      p = spawn(cmd, args, { env: penv || process.env, stdio: ['pipe', 'pipe', 'pipe'] });
    } catch (e) { return reject(e); }
    const timer = setTimeout(() => {
      try { p.kill('SIGKILL'); } catch (e) {}
      reject(Object.assign(new Error('PROC_TIMEOUT:' + path.basename(cmd)), { stdout: out, stderr: err }));
    }, timeoutMs);
    p.stdout.on('data', d => { out += d.toString(); if (out.length > 4 * MB) out = out.slice(-2 * MB); if (onStdout) onStdout(d.toString()); });
    p.stderr.on('data', d => { err += d.toString(); if (err.length > 200000) err = err.slice(-100000); if (onStderr) onStderr(d.toString()); });
    p.on('error', e => { clearTimeout(timer); reject(Object.assign(e, { stdout: out, stderr: err })); });
    p.on('close', code => {
      clearTimeout(timer);
      if (code === 0) resolve({ stdout: out, stderr: err, code });
      else reject(Object.assign(new Error(path.basename(cmd) + ' exit ' + code + ': ' + err.trim().split('\n').slice(-3).join(' | ').slice(0, 400)), { code, stdout: out, stderr: err }));
    });
    if (input !== null) p.stdin.end(input); else p.stdin.end();
  });
}

const esc = s => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

// ==================== 3. СТАН (переживає рестарт Render) ====================
// Render Free не має постійного диска. Тому стан кладемо у закріплений файл у чаті адміна з ботом (Telegram = безкоштовна БД).
// Токен постера зберігається зашифрованим (AES-256-GCM, ключ з API_KEY).

function parseXUsername(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  const m = s.match(/(?:https?:\/\/)?(?:www\.|mobile\.)?(?:x|twitter|fxtwitter|vxtwitter|fixupx|xcancel)\.com\/([A-Za-z0-9_]{1,30})/i) ||
            s.match(/(?:https?:\/\/)?nitter\.[^\/]+\/([A-Za-z0-9_]{1,30})/i);
  if (m && m[1] && !/^(?:home|explore|search|i|intent|share|hashtag|status)$/i.test(m[1])) return m[1];
  const bare = s.replace(/^@+/, '').split(/[\/?#\s]/)[0].trim();
  return /^[A-Za-z0-9_]{1,30}$/.test(bare) ? bare : '';
}

function parseTelegramChat(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  const m = s.match(/(?:https?:\/\/)?(?:www\.)?(?:t\.me|telegram\.me)\/([A-Za-z0-9_]{4,64})/i);
  if (m && m[1] && !/^(?:joinchat|addstickers|share|iv|c)$/i.test(m[1])) return '@' + m[1];
  if (/^-?\d{6,20}$/.test(s)) return s;
  if (s.startsWith('@')) return s;
  if (/^[A-Za-z0-9_]{4,64}$/.test(s)) return '@' + s;
  return '';
}

const parseList = raw => String(raw || '').split(/[,\n;]+/).map(s => s.trim()).filter(Boolean);
const uniq = arr => [...new Set(arr)];
const lc = s => String(s || '').toLowerCase();

const SEEN_CAP = 150;

const state = {
  loadedFromTelegram: false,   // true якщо стан підтягнуто з Telegram або отримано явний push від бота
  x: {
    enabled: envBool('X_ENABLED', true),
    accounts: uniq(parseList(env('X_ACCOUNTS', 'GoalsXtra,TheGoalsZone')).map(parseXUsername).filter(Boolean)),
    targets: uniq(parseList(env('X_TARGET_CHAT', '@Fantasyargoal')).map(parseTelegramChat).filter(Boolean)),
    autoChats: [],
    credit: env('X_CREDIT_TEXT', '- Uploaded by @jiijhwiazbot'),
    token: env('X_POSTER_BOT_TOKEN'),
    holdEnabled: envBool('X_HOLD_ENABLED', true), // За замовчуванням: кидати в відложку на 1 день (24 год) для перевірки на повтори
    holdHours: envNum('X_HOLD_HOURS', 24),
    queue: [],         // Черга відкладених постів (відложка на 24 год): [{ qid, account, id, videoUrl, width, height, duration, text, createdAt, publishAt, targets, dupWarning, adminChatId, adminMsgId, cachedFileId }]
    mediaSeen: [],     // Глобальні ключі вже взятих відео (щоб другий акаунт не дублював те саме відео)
    seen: {},          // lc(account) -> [id, ...] (нові спочатку)
    baseline: {},      // lc(account) -> timestamp останнього стартового поста
    offset: 0,         // getUpdates offset для виявлення каналів
    posted: 0,
    updatedAt: 0
  }
};

const runtime = {
  mainToken: '',         // токен основного бота (приходить із /send) — запасний варіант для сховища стану
  adminChat: ADMIN_CHAT_ID
};

function stateKey() { return crypto.scryptSync(API_KEY, 'ssn-state-v1', 32); }
function encSecret(plain) {
  if (!plain) return '';
  const iv = crypto.randomBytes(12);
  const c = crypto.createCipheriv('aes-256-gcm', stateKey(), iv);
  const ct = Buffer.concat([c.update(String(plain), 'utf8'), c.final()]);
  return 'enc1:' + Buffer.concat([iv, c.getAuthTag(), ct]).toString('base64');
}
function decSecret(s) {
  if (!s) return '';
  if (!String(s).startsWith('enc1:')) return String(s);
  try {
    const buf = Buffer.from(String(s).slice(5), 'base64');
    const d = crypto.createDecipheriv('aes-256-gcm', stateKey(), buf.subarray(0, 12));
    d.setAuthTag(buf.subarray(12, 28));
    return Buffer.concat([d.update(buf.subarray(28)), d.final()]).toString('utf8');
  } catch (e) { return ''; }
}

function serializeState() {
  const x = state.x;
  return JSON.stringify({
    v: 1,
    savedAt: Date.now(),
    x: {
      enabled: x.enabled, accounts: x.accounts, targets: x.targets, autoChats: x.autoChats,
      credit: x.credit, token: encSecret(x.token),
      holdEnabled: x.holdEnabled, holdHours: x.holdHours,
      queue: Array.isArray(x.queue) ? x.queue.slice(0, 120) : [],
      mediaSeen: Array.isArray(x.mediaSeen) ? x.mediaSeen.slice(0, 400) : [],
      seen: x.seen, baseline: x.baseline,
      offset: x.offset, posted: x.posted, updatedAt: x.updatedAt
    }
  });
}

function applyLoadedState(j) {
  if (!j || !j.x) return false;
  const x = j.x;
  const S = state.x;
  if (typeof x.enabled === 'boolean') S.enabled = x.enabled;
  if (Array.isArray(x.accounts) && x.accounts.length) S.accounts = uniq(x.accounts.map(parseXUsername).filter(Boolean));
  if (Array.isArray(x.targets)) S.targets = uniq(x.targets.map(parseTelegramChat).filter(Boolean));
  if (Array.isArray(x.autoChats)) S.autoChats = uniq(x.autoChats.map(String));
  if (x.credit) S.credit = String(x.credit);
  const tok = decSecret(x.token);
  if (tok) S.token = tok;
  if (x.seen && typeof x.seen === 'object') {
    for (const k of Object.keys(x.seen)) {
      const merged = uniq([...(Array.isArray(x.seen[k]) ? x.seen[k] : []), ...(S.seen[k] || [])]);
      merged.sort((a, b) => (String(a).length - String(b).length) || (String(a) < String(b) ? 1 : -1));
      S.seen[k] = merged.slice(0, SEEN_CAP);
    }
  }
  if (typeof x.holdEnabled === 'boolean') S.holdEnabled = x.holdEnabled;
  if (Number.isFinite(x.holdHours) && x.holdHours >= 0) S.holdHours = x.holdHours;
  if (Array.isArray(x.queue)) {
    const existingIds = new Set((S.queue || []).map(q => q.qid));
    for (const q of x.queue) {
      if (q && q.qid && !existingIds.has(q.qid)) S.queue.push(q);
    }
  }
  if (Array.isArray(x.mediaSeen)) {
    S.mediaSeen = uniq([...(S.mediaSeen || []), ...x.mediaSeen]).slice(0, 400);
  }
  if (x.baseline && typeof x.baseline === 'object') S.baseline = { ...S.baseline, ...x.baseline };
  if (Number.isFinite(x.offset)) S.offset = x.offset;
  if (Number.isFinite(x.posted)) S.posted = x.posted;
  S.updatedAt = Number(x.updatedAt) || 0;
  return true;
}

class TgError extends Error {
  constructor(description, code, params) {
    super(description || 'Telegram error');
    this.name = 'TgError';
    this.code = code || 0;
    this.params = params || null;
  }
}

const Store = {
  msgId: 0,
  loaded: false,
  loading: null,
  dirty: false,
  timer: null,
  saving: false,
  lastError: '',
  lastSavedAt: 0,
  lastLoadAt: 0,
  token() {
    return env('STATE_BOT_TOKEN') || env('BOT_TOKEN') || runtime.mainToken || '';
  },
  async load() {
    if (this.loaded) return true;
    if (this.loading) return this.loading;
    this.loading = (async () => {
      const tok = this.token();
      if (!tok) { this.lastError = 'немає токена (задай BOT_TOKEN у Environment на Render)'; return false; }
      try {
        const chat = await tg(tok, 'getChat', { chat_id: STATE_CHAT_ID }, { timeout: 20000 });
        const pm = chat && chat.pinned_message;
        if (pm && pm.document && /xwatch-state/i.test(pm.document.file_name || '')) {
          this.msgId = pm.message_id;
          const f = await tg(tok, 'getFile', { file_id: pm.document.file_id }, { timeout: 20000 });
          const r = await fetchJson(TG_API_BASE + '/file/bot' + tok + '/' + f.file_path, {}, 30000);
          if (r.json && applyLoadedState(r.json)) {
            state.loadedFromTelegram = true;
            log('state', 'стан завантажено з Telegram: акаунтів', state.x.accounts.length, 'каналів', state.x.targets.length + state.x.autoChats.length);
          }
        } else {
          log('state', 'збереженого стану в Telegram ще немає — буде створено при першій зміні');
        }
        this.loaded = true;
        this.lastLoadAt = Date.now();
        this.lastError = '';
        return true;
      } catch (e) {
        this.lastError = redact(e.message).slice(0, 200);
        log('state', 'не вдалося завантажити стан:', this.lastError);
        return false;
      }
    })().finally(() => { this.loading = null; });
    return this.loading;
  },
  schedule() {
    this.dirty = true;
    if (this.timer) return;
    this.timer = setTimeout(() => { this.timer = null; this.saveNow().catch(() => {}); }, 4000);
  },
  async saveNow() {
    if (this.saving) { this.dirty = true; return false; }
    const tok = this.token();
    if (!tok) { this.lastError = 'немає токена для збереження стану'; return false; }
    if (!this.loaded && !(await this.load())) {
      // не перезаписуємо чужий стан, поки не змогли його прочитати
      if (this.token()) this.dirty = true;
      return false;
    }
    this.saving = true;
    this.dirty = false;
    try {
      const body = serializeState();
      const mkForm = (withEdit) => {
        const f = new FormData();
        const blob = new Blob([body], { type: 'application/json' });
        if (withEdit) {
          f.append('chat_id', String(STATE_CHAT_ID));
          f.append('message_id', String(this.msgId));
          f.append('media', JSON.stringify({ type: 'document', media: 'attach://f', caption: '🗄 xwatch-state (службовий файл, не видаляй)' }));
          f.append('f', blob, 'xwatch-state.json');
        } else {
          f.append('chat_id', String(STATE_CHAT_ID));
          f.append('disable_notification', 'true');
          f.append('caption', '🗄 xwatch-state (службовий файл, не видаляй)');
          f.append('document', blob, 'xwatch-state.json');
        }
        return f;
      };
      let done = false;
      if (this.msgId) {
        try {
          await tg(tok, 'editMessageMedia', () => mkForm(true), { timeout: 30000 });
          done = true;
        } catch (e) {
          if (/not modified/i.test(e.message)) done = true;
          else log('state', 'editMessageMedia не вдалось (' + redact(e.message).slice(0, 80) + '), створюю новий файл');
        }
      }
      if (!done) {
        const m = await tg(tok, 'sendDocument', () => mkForm(false), { timeout: 30000 });
        this.msgId = m.message_id;
        await tg(tok, 'pinChatMessage', { chat_id: STATE_CHAT_ID, message_id: m.message_id, disable_notification: true }, { timeout: 15000 }).catch(e => {
          this.lastError = 'не вдалось закріпити файл стану: ' + redact(e.message).slice(0, 100);
        });
      }
      this.lastSavedAt = Date.now();
      if (!this.lastError.startsWith('не вдалось закріпити')) this.lastError = '';
      return true;
    } catch (e) {
      this.lastError = redact(e.message).slice(0, 200);
      this.dirty = true;
      log('state', 'збереження стану не вдалось:', this.lastError);
      return false;
    } finally {
      this.saving = false;
    }
  }
};

// ==================== 4. TELEGRAM API ====================
// payload: об'єкт (JSON) | FormData | функція () => FormData (потрібна для повторів, бо FormData одноразова)
async function tg(token, method, payload, opts = {}) {
  const base = (opts.local && localApiReady) ? ('http://127.0.0.1:' + LOCAL_API_PORT) : TG_API_BASE;
  const url = base + '/bot' + token + '/' + method;
  const timeout = opts.timeout || 60000;
  let lastErr = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    let init;
    if (typeof payload === 'function') init = { method: 'POST', body: payload() };
    else if (typeof FormData !== 'undefined' && payload instanceof FormData) init = { method: 'POST', body: payload };
    else init = { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload || {}) };
    let res;
    try {
      res = await fetchJson(url, init, timeout);
    } catch (e) {
      lastErr = new TgError('NETWORK: ' + redact(e.message), 0);
      if (attempt < 2 && !opts.noRetry) { await sleep(800 * (attempt + 1)); continue; }
      throw lastErr;
    }
    const j = res.json;
    if (j && j.ok) return j.result;
    const desc = (j && j.description) || ('HTTP ' + res.status + (res.text ? ' ' + res.text.slice(0, 120) : ''));
    lastErr = new TgError(desc, (j && j.error_code) || res.status, j && j.parameters);
    if (lastErr.code === 429 && attempt < 2) {
      const wait = Math.min(Number(lastErr.params && lastErr.params.retry_after) || 2, 12);
      await sleep(wait * 1000 + 250);
      continue;
    }
    if (lastErr.code >= 500 && attempt < 2 && !opts.noRetry) { await sleep(1000 * (attempt + 1)); continue; }
    throw lastErr;
  }
  throw lastErr || new TgError('unknown');
}

// Редагування статусного повідомлення з обмеженням частоти (Telegram не любить >1 правки/сек на повідомлення)
const statusMap = new Map(); // chat:msg -> {t, text, dead}
async function editStatus(token, chatId, msgId, text, { force = false, bizConn = '' } = {}) {
  if (!token || !chatId || !msgId) return false;
  const key = chatId + ':' + msgId;
  const st = statusMap.get(key) || { t: 0, text: '', dead: false };
  if (st.dead || st.text === text) return false;
  const now = Date.now();
  if (!force && now - st.t < 2200) return false;
  st.t = now;
  st.text = text;
  statusMap.set(key, st);
  if (statusMap.size > 400) statusMap.delete(statusMap.keys().next().value);
  try {
    await tg(token, 'editMessageText', {
      chat_id: chatId, message_id: msgId, text, parse_mode: 'HTML', link_preview_options: { is_disabled: true },
      ...(bizConn ? { business_connection_id: bizConn } : {})
    }, { timeout: 10000, noRetry: true });
    return true;
  } catch (e) {
    if (/not found|can't be edited|MESSAGE_ID_INVALID/i.test(e.message)) st.dead = true;
    return false;
  }
}

function progressText(pct, stage) {
  const p = Math.max(1, Math.min(99, Math.round(pct)));
  const filled = Math.max(1, Math.min(10, Math.round(p / 10)));
  return '⏳ <b>' + esc(stage || 'Завантаження') + '</b> · <b>' + p + '%</b>\n<code>' + '█'.repeat(filled) + '░'.repeat(10 - filled) + '</code>';
}

// ---- відправка файлу в Telegram (стрімінг з диска, без завантаження у RAM) ----
const SEND_MODES = {
  video:      { method: 'sendVideo',     field: 'video',      mime: 'video/mp4',                 name: 'video.mp4' },
  document:   { method: 'sendDocument',  field: 'document',   mime: 'application/octet-stream',  name: 'file.mp4' },
  audio:      { method: 'sendAudio',     field: 'audio',      mime: 'audio/mpeg',                name: 'audio.mp3' },
  video_note: { method: 'sendVideoNote', field: 'video_note', mime: 'video/mp4',                 name: 'circle.mp4' }
};

async function fileBlob(file, type) {
  if (typeof fs.openAsBlob === 'function') return fs.openAsBlob(file, { type });
  const buf = await fsp.readFile(file); // запасний варіант для старих Node (для файлів ≤ 50 МБ)
  return new Blob([buf], { type });
}

function stripHtml(s) { return String(s || '').replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&'); }

async function sendFileToTelegram(p) {
  const { token, chatId, file, replyTo, bizConn, threadId, meta = {}, thumb } = p;
  const mode = SEND_MODES[p.mode] ? p.mode : 'video';
  const M = SEND_MODES[mode];
  const size = fileSize(file);
  if (!size) throw new Error('EMPTY_FILE');
  let caption = mode === 'video_note' ? '' : String(p.caption || '');
  let parseMode = caption ? 'HTML' : '';
  const markup = p.markup ? (typeof p.markup === 'string' ? p.markup : JSON.stringify(p.markup)) : '';
  const fileName = p.fileName || M.name;
  const mime = p.mime || M.mime;
  const blob = await fileBlob(file, mime);
  const thumbBlob = thumb && fileSize(thumb) ? await fileBlob(thumb, 'image/jpeg') : null;

  const mkForm = () => {
    const f = new FormData();
    f.append('chat_id', String(chatId));
    if (threadId) f.append('message_thread_id', String(threadId));
    if (bizConn) f.append('business_connection_id', String(bizConn));
    if (replyTo) f.append('reply_parameters', JSON.stringify({ message_id: Number(replyTo), allow_sending_without_reply: true }));
    if (caption) { f.append('caption', caption); if (parseMode) f.append('parse_mode', parseMode); }
    if (markup) f.append('reply_markup', markup);
    if (mode === 'video') {
      f.append('supports_streaming', 'true');
      f.append('show_caption_above_media', 'true');
      if (meta.width) f.append('width', String(meta.width));
      if (meta.height) f.append('height', String(meta.height));
      if (meta.duration) f.append('duration', String(Math.round(meta.duration)));
    } else if (mode === 'video_note') {
      f.append('length', '384');
      if (meta.duration) f.append('duration', String(Math.round(meta.duration)));
    } else if (mode === 'audio') {
      if (meta.duration) f.append('duration', String(Math.round(meta.duration)));
      if (meta.title) f.append('title', String(meta.title).slice(0, 120));
      if (meta.performer) f.append('performer', String(meta.performer).slice(0, 120));
    } else if (mode === 'document') {
      f.append('disable_content_type_detection', 'true');
    }
    f.append(M.field, blob, fileName);
    if (thumbBlob) f.append('thumbnail', thumbBlob, 'thumb.jpg');
    return f;
  };

  const mkJsonLocal = () => {
    const o = { chat_id: chatId, [M.field]: 'file://' + file };
    if (threadId) o.message_thread_id = Number(threadId);
    if (bizConn) o.business_connection_id = String(bizConn);
    if (replyTo) o.reply_parameters = { message_id: Number(replyTo), allow_sending_without_reply: true };
    if (caption) { o.caption = caption; if (parseMode) o.parse_mode = parseMode; }
    if (markup) o.reply_markup = JSON.parse(markup);
    if (mode === 'video') {
      o.supports_streaming = true; o.show_caption_above_media = true;
      if (meta.width) o.width = meta.width;
      if (meta.height) o.height = meta.height;
      if (meta.duration) o.duration = Math.round(meta.duration);
    } else if (mode === 'video_note') { o.length = 384; if (meta.duration) o.duration = Math.round(meta.duration); }
    else if (mode === 'audio') {
      if (meta.duration) o.duration = Math.round(meta.duration);
      if (meta.title) o.title = String(meta.title).slice(0, 120);
      if (meta.performer) o.performer = String(meta.performer).slice(0, 120);
    } else if (mode === 'document') o.disable_content_type_detection = true;
    if (thumb && fileSize(thumb)) o.thumbnail = 'file://' + thumb;
    return o;
  };

  const upTimeout = Math.min(15 * 60 * 1000, Math.max(60000, Math.ceil(size / (120 * 1024)) * 1000));
  const attempts = [];
  const big = size > STD_TG_LIMIT;
  if (!big) attempts.push({ name: 'cloud-multipart', run: () => tg(token, M.method, mkForm, { timeout: upTimeout }) });
  if (localApiReady) {
    attempts.push({ name: 'local-file', run: () => tg(token, M.method, mkJsonLocal(), { local: true, timeout: upTimeout, noRetry: true }) });
    attempts.push({ name: 'local-multipart', run: () => tg(token, M.method, mkForm, { local: true, timeout: upTimeout }) });
  }
  if (!attempts.length) throw Object.assign(new Error('TOO_BIG_NO_LOCAL_API'), { tooBig: true, size });

  const errors = [];
  for (const a of attempts) {
    for (let fix = 0; fix < 3; fix++) {
      try {
        const res = await a.run();
        return res;
      } catch (e) {
        const msg = String(e.message || e);
        errors.push(a.name + ': ' + redact(msg).slice(0, 160));
        // автовиправлення типових відмов Telegram
        if (/can't parse entities|unsupported start tag|can't find end/i.test(msg) && parseMode) { caption = stripHtml(caption); parseMode = ''; continue; }
        if (/caption is too long/i.test(msg) && caption) { caption = stripHtml(caption).slice(0, 1000); parseMode = ''; continue; }
        if (/chat not found|bot was blocked|bot is not a member|have no rights|not enough rights|forbidden|user is deactivated|CHAT_WRITE_FORBIDDEN|CHAT_ADMIN_REQUIRED/i.test(msg)) {
          throw Object.assign(new Error('TG_REJECTED: ' + msg), { fatal: true, tgDesc: msg });
        }
        break; // перейти до наступного способу
      }
    }
  }
  const tooBig = big || errors.some(e => /too large|entity too large|file is too big/i.test(e));
  throw Object.assign(new Error((tooBig ? 'TG_TOO_BIG: ' : 'TG_UPLOAD_FAILED: ') + errors.join(' || ').slice(0, 500)), { tooBig, size });
}

// ==================== 5. МЕДІА-ІНСТРУМЕНТИ ====================
async function probe(file) {
  const r = await runProc('ffprobe', ['-v', 'error', '-print_format', 'json', '-show_format', '-show_streams', file], { timeoutMs: 40000 });
  const j = JSON.parse(r.stdout || '{}');
  const streams = j.streams || [];
  const v = streams.find(s => s.codec_type === 'video' && !(s.disposition && s.disposition.attached_pic));
  const a = streams.find(s => s.codec_type === 'audio');
  let width = v ? Number(v.width) || 0 : 0;
  let height = v ? Number(v.height) || 0 : 0;
  if (v) {
    let rot = 0;
    const sd = (v.side_data_list || []).find(x => x && x.rotation !== undefined);
    if (sd) rot = Math.abs(Number(sd.rotation) || 0);
    else if (v.tags && v.tags.rotate) rot = Math.abs(Number(v.tags.rotate) || 0);
    if (rot === 90 || rot === 270) { const t = width; width = height; height = t; }
  }
  const fmt = j.format || {};
  const duration = Number(fmt.duration) || (v && Number(v.duration)) || (a && Number(a.duration)) || 0;
  return {
    duration, width, height,
    vcodec: v ? v.codec_name : '', acodec: a ? a.codec_name : '',
    hasVideo: !!v, hasAudio: !!a,
    container: String(fmt.format_name || ''), size: Number(fmt.size) || fileSize(file),
    bitrate: Number(fmt.bit_rate) || 0
  };
}

async function makeThumb(file, info) {
  const out = tmpName('.jpg');
  const at = Math.max(0, Math.min(1.5, (info && info.duration ? info.duration / 3 : 0.5)));
  try {
    await runProc('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-ss', String(at), '-i', file, '-frames:v', '1',
      '-vf', "scale='min(320,iw)':'min(320,ih)':force_original_aspect_ratio=decrease", '-q:v', '6', '-y', out], { timeoutMs: 30000 });
    if (fileSize(out) > 190 * 1024) {
      const out2 = tmpName('.jpg');
      await runProc('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-i', out, '-q:v', '14', '-y', out2], { timeoutMs: 15000 });
      safeUnlink(out);
      return fileSize(out2) ? out2 : null;
    }
    return fileSize(out) ? out : null;
  } catch (e) { safeUnlink(out); return null; }
}

// Робить файл сумісним з Telegram: mp4 + faststart; vp9/av1/webm → h264 (якщо файл невеликий), інакше віддамо документом.
async function normalizeForTelegram(file, info, { allowTranscode = true } = {}) {
  const out = tmpName('.mp4');
  const okV = ['h264', 'hevc'].includes(info.vcodec);
  const okA = !info.hasAudio || ['aac', 'mp3'].includes(info.acodec);
  const isMp4 = /mp4|mov|m4a/.test(info.container);
  if (okV && okA && isMp4) {
    try {
      await runProc('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-i', file, '-map', '0:v:0', '-map', '0:a:0?', '-c', 'copy', '-movflags', '+faststart', '-y', out], { timeoutMs: 180000 });
      if (fileSize(out) > 1000) return { file: out, converted: false };
    } catch (e) { safeUnlink(out); }
    return { file, converted: false };
  }
  if (!allowTranscode) { safeUnlink(out); return { file, converted: false, incompatible: true }; }
  // сумісні відео, але неправильний аудіокодек (opus у mp4 тощо) — перекодуємо тільки звук
  if (okV && !okA) {
    try {
      await runProc('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-i', file, '-map', '0:v:0', '-map', '0:a:0?', '-c:v', 'copy', '-c:a', 'aac', '-b:a', '128k', '-movflags', '+faststart', '-y', out], { timeoutMs: 240000 });
      if (fileSize(out) > 1000) return { file: out, converted: true };
    } catch (e) { safeUnlink(out); }
    return { file, converted: false, incompatible: true };
  }
  // несумісний відеокодек: перекодування дороге на безкоштовному Render, робимо тільки для коротких/малих файлів
  if ((info.duration && info.duration > 150) || info.size > 120 * MB) { safeUnlink(out); return { file, converted: false, incompatible: true }; }
  try {
    await runProc('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-i', file, '-map', '0:v:0', '-map', '0:a:0?', '-vf', "scale='min(1280,iw)':-2",
      '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '26', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '128k', '-movflags', '+faststart', '-y', out], { timeoutMs: 280000 });
    if (fileSize(out) > 1000) return { file: out, converted: true };
  } catch (e) { safeUnlink(out); }
  return { file, converted: false, incompatible: true };
}

async function muxVideoAudio(vPath, aPath, out) {
  const aInfo = await probe(aPath).catch(() => ({ acodec: 'aac' }));
  const aCodec = ['aac', 'mp3'].includes(aInfo.acodec) ? 'copy' : 'aac';
  await runProc('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-i', vPath, '-i', aPath, '-map', '0:v:0', '-map', '1:a:0',
    '-c:v', 'copy', '-c:a', aCodec, ...(aCodec === 'aac' ? ['-b:a', '160k'] : []), '-shortest', '-movflags', '+faststart', '-y', out], { timeoutMs: 240000 });
}

async function hlsToMp4(url, out, { ua = UA, referer = '' } = {}) {
  const args = ['-hide_banner', '-loglevel', 'error', '-protocol_whitelist', 'file,http,https,tcp,tls,crypto', '-user_agent', ua];
  if (referer) args.push('-headers', 'Referer: ' + referer + '\r\n');
  args.push('-i', url, '-c', 'copy', '-bsf:a', 'aac_adtstoasc', '-movflags', '+faststart', '-y', out);
  await runProc('ffmpeg', args, { timeoutMs: 8 * 60 * 1000 });
}

const MEDIA_CT = /^(video|audio|image)\/|application\/(octet-stream|x-mpegurl|vnd\.apple\.mpegurl|mp4|binary)|binary\/octet-stream/i;

// Завантаження файлу з перевіркою, що це справді медіа (а не HTML-сторінка блокування), докачкою та сторожем на "завис"
async function downloadToFile(url, dest, opts = {}) {
  const { referer = '', ua = UA, headers = {}, onProgress = null, maxBytes = 1900 * MB, stallMs = 25000, retries = 2, label = '' } = opts;
  let written = 0;
  let total = 0;
  let lastErr = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ctrl = new AbortController();
    let stall = setTimeout(() => ctrl.abort(new Error('STALL')), stallMs);
    const poke = () => { clearTimeout(stall); stall = setTimeout(() => ctrl.abort(new Error('STALL')), stallMs); };
    try {
      const h = { 'User-Agent': ua, Accept: '*/*', 'Accept-Language': 'en-US,en;q=0.9', ...headers };
      if (referer) h.Referer = referer;
      if (written > 0) h.Range = 'bytes=' + written + '-';
      const r = await fetch(url, { headers: h, redirect: 'follow', signal: ctrl.signal });
      poke();
      if (!(r.ok || r.status === 206)) {
        const err = new Error('HTTP_' + r.status);
        err.status = r.status;
        throw err;
      }
      if (written > 0 && r.status !== 206) { written = 0; await fsp.rm(dest, { force: true }); } // сервер не підтримує докачку
      const ct = String(r.headers.get('content-type') || '');
      const cl = Number(r.headers.get('content-length') || 0);
      if (/text\/html|application\/json|text\/plain/i.test(ct) && cl < 3 * MB) {
        const snippet = (await r.text()).replace(/\s+/g, ' ').slice(0, 140);
        throw Object.assign(new Error('NOT_MEDIA:' + snippet), { fatal: true });
      }
      if (!total) total = (r.status === 206 ? written : 0) + cl;
      if (total && total > maxBytes) throw Object.assign(new Error('TOO_BIG:' + total), { fatal: true });
      const ws = fs.createWriteStream(dest, { flags: written > 0 ? 'a' : 'w' });
      let lastPct = -1;
      const src = Readable.fromWeb(r.body);
      src.on('data', chunk => {
        written += chunk.length;
        poke();
        if (written > maxBytes) src.destroy(Object.assign(new Error('TOO_BIG:' + written), { fatal: true }));
        if (onProgress && total > 0) {
          const pct = Math.round((written / total) * 100);
          if (pct !== lastPct) { lastPct = pct; onProgress(Math.min(100, pct)); }
        }
      });
      await pipeline(src, ws);
      clearTimeout(stall);
      if (written < 512) throw new Error('EMPTY_FILE');
      if (total && written < total * 0.995 && !/chunked/i.test(r.headers.get('transfer-encoding') || '')) throw new Error('TRUNCATED:' + written + '/' + total);
      return written;
    } catch (e) {
      clearTimeout(stall);
      lastErr = e;
      const msg = String((e && e.message) || e);
      if (e && (e.fatal || /^HTTP_(401|403|404|410|451)/.test(msg)) && !/STALL/.test(msg)) break;
      if (attempt < retries) { await sleep(700 * (attempt + 1)); continue; }
    }
  }
  const msg = String((lastErr && lastErr.message) || lastErr);
  throw Object.assign(new Error(msg), { status: lastErr && lastErr.status, fatal: lastErr && lastErr.fatal });
}

// ==================== 6. ПЛАТФОРМИ ТА РУШІЇ ЗАВАНТАЖЕННЯ ====================
function detectPlatform(url) {
  const u = String(url || '').toLowerCase();
  if (/(^|\/\/|\.)(youtube\.com|youtu\.be|youtube-nocookie\.com)\b/.test(u)) return 'youtube';
  if (/(^|\/\/|\.)(tiktok\.com|douyin\.com|iesdouyin\.com)\b/.test(u)) return 'tiktok';
  if (/(^|\/\/|\.)(instagram\.com|instagr\.am)\b/.test(u)) return 'instagram';
  if (/(^|\/\/|\.)(x\.com|twitter\.com|fxtwitter\.com|vxtwitter\.com|fixupx\.com|t\.co)\b/.test(u)) return 'twitter';
  if (/(^|\/\/|\.)(facebook\.com|fb\.watch)\b/.test(u)) return 'facebook';
  if (/(^|\/\/|\.)(reddit\.com|redd\.it)\b/.test(u)) return 'reddit';
  return 'other';
}

function extractYtId(url) {
  const m = String(url || '').match(/(?:youtube\.com\/(?:watch\?(?:[^\s]*&)?v=|shorts\/|embed\/|live\/|v\/|clip\/)|youtu\.be\/)([A-Za-z0-9_-]{6,})/i);
  return m ? m[1] : '';
}

const toLocalCobalt = u => String(u || '').replace(PUBLIC_URL, 'http://127.0.0.1:' + COBALT_PORT);

// ---- куки/проксі для yt-dlp ----
const COOKIE_FILES = {};
function setupCookies() {
  try {
    if (YT_COOKIES_RAW) { COOKIE_FILES.youtube = path.join(TMP_DIR, 'cookies_youtube.txt'); fs.writeFileSync(COOKIE_FILES.youtube, YT_COOKIES_RAW, { mode: 0o600 }); }
    if (COOKIES_ALL_RAW) { COOKIE_FILES.all = path.join(TMP_DIR, 'cookies_all.txt'); fs.writeFileSync(COOKIE_FILES.all, COOKIES_ALL_RAW, { mode: 0o600 }); }
    for (const site of ['TIKTOK', 'INSTAGRAM', 'TWITTER']) {
      const b = env('COOKIES_' + site + '_B64');
      if (b) { COOKIE_FILES[site.toLowerCase()] = path.join(TMP_DIR, 'cookies_' + site.toLowerCase() + '.txt'); fs.writeFileSync(COOKIE_FILES[site.toLowerCase()], Buffer.from(b, 'base64').toString('utf8'), { mode: 0o600 }); }
    }
  } catch (e) { log('cookies', 'помилка запису кук:', e.message); }
}
function cookieFor(platform) {
  const f = COOKIE_FILES[platform] || COOKIE_FILES.all || '';
  if (!f) return '';
  // копія, бо yt-dlp перезаписує файл кук
  const copy = tmpName('.txt');
  try { fs.copyFileSync(f, copy); return copy; } catch (e) { return ''; }
}

// ---- yt-dlp ----
let ytdlpCanImpersonate = false;
let ytdlpInit = null;   // promise початкової перевірки yt-dlp (щоб перші запити не бачили його «відсутнім»)

async function ensureYtDlp() {
  try {
    if (!fs.existsSync(YTDLP_BIN)) {
      log('ytdlp', 'бінарник не знайдено, завантажую з GitHub…');
      const r = await fetchRaw('https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_musllinux', { redirect: 'follow' }, 120000);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const tmp = YTDLP_BIN + '.new';
      await fsp.mkdir(path.dirname(YTDLP_BIN), { recursive: true });
      await pipeline(Readable.fromWeb(r.body), fs.createWriteStream(tmp, { mode: 0o755 }));
      await fsp.chmod(tmp, 0o755);
      await fsp.rename(tmp, YTDLP_BIN);
    }
    const v = await runProc(YTDLP_BIN, ['--version'], { timeoutMs: 40000 });
    ytdlpVersion = v.stdout.trim();
    ytdlpLastError = '';
    try {
      const t = await runProc(YTDLP_BIN, ['--list-impersonate-targets'], { timeoutMs: 40000 });
      ytdlpCanImpersonate = /chrome/i.test(t.stdout) && !/chrome[^\n]*unavailable/i.test(t.stdout);
    } catch (e) { ytdlpCanImpersonate = false; }
    log('ytdlp', 'версія', ytdlpVersion, ytdlpCanImpersonate ? '(impersonate: так)' : '(impersonate: ні)');
  } catch (e) {
    ytdlpVersion = '';
    ytdlpLastError = redact(e.message).slice(0, 200);
    log('ytdlp', 'недоступний:', ytdlpLastError);
  }
}

async function updateYtDlp() {
  if (!ytdlpVersion) return;
  try {
    const r = await runProc(YTDLP_BIN, ['-U'], { timeoutMs: 120000 });
    log('ytdlp', 'оновлення:', r.stdout.trim().split('\n').slice(-1)[0] || 'ok');
    const v = await runProc(YTDLP_BIN, ['--version'], { timeoutMs: 40000 });
    ytdlpVersion = v.stdout.trim();
  } catch (e) { log('ytdlp', 'оновлення не вдалось:', redact(e.message).slice(0, 150)); }
}

function classifyYtdlpError(stderr) {
  const s = String(stderr || '');
  if (/Sign in to confirm|not a bot|LOGIN_REQUIRED/i.test(s)) return 'YT_BOT_CHECK: YouTube блокує IP сервера (потрібні куки YT_COOKIES_B64 або проксі YT_PROXY)';
  if (/Private video|This video is private/i.test(s)) return 'PRIVATE';
  if (/Video unavailable|has been removed|no longer available|Unavailable/i.test(s)) return 'UNAVAILABLE';
  if (/age.restricted|Sign in to confirm your age/i.test(s)) return 'AGE_RESTRICTED';
  if (/HTTP Error 403/i.test(s)) return 'HTTP_403';
  if (/HTTP Error 429|Too Many Requests/i.test(s)) return 'HTTP_429';
  if (/Requested format is not available/i.test(s)) return 'FORMAT_UNAVAILABLE';
  if (/Unsupported URL/i.test(s)) return 'UNSUPPORTED_URL';
  if (/login|cookies|rate-limit reached/i.test(s)) return 'LOGIN_REQUIRED';
  return '';
}

function buildFormat(q, dub, audioOnly) {
  if (audioOnly) return dub ? ('ba[language^=' + dub + ']/ba/b') : 'ba/b';
  const H = '[height<=' + q + ']';
  const a1 = dub ? ('ba[language^=' + dub + ']') : 'ba[ext=m4a]';
  const parts = [
    'bv*' + H + '[vcodec^=avc1]+' + a1,
    'bv*' + H + '[vcodec^=avc1]+ba[ext=m4a]',
    'b' + H + '[ext=mp4]',
    'bv*' + H + '+ba',
    'b' + H,
    'b'
  ];
  return parts.join('/');
}

async function ytdlpDownload(job, req, attempt) {
  const platform = req.platform;
  const base = path.join(TMP_DIR, 'ssn_yd_' + Date.now().toString(36) + '_' + crypto.randomBytes(3).toString('hex'));
  job.track(base + '*');
  const q = Math.min(1080, Number(req.quality) || 1080);
  const args = ['--ignore-config', '--no-playlist', '--no-warnings', '--no-cache-dir', '--newline', '--socket-timeout', '20',
    '--retries', '3', '--fragment-retries', '3', '--concurrent-fragments', '4',
    '-f', buildFormat(q, req.dubLang, req.audioOnly), '-o', base + '.%(ext)s'];
  if (req.audioOnly) args.push('-x', '--audio-format', 'mp3', '--audio-quality', '128K');
  else args.push('--merge-output-format', 'mp4');
  const proxy = (platform === 'youtube' && YT_PROXY) || YTDLP_PROXY;
  if (proxy) args.push('--proxy', proxy);
  if (platform === 'youtube') {
    args.push('--js-runtimes', 'node', '--remote-components', 'ejs:github');
    if (attempt.clients) args.push('--extractor-args', 'youtube:player_client=' + attempt.clients);
  }
  if (attempt.impersonate && ytdlpCanImpersonate) args.push('--impersonate', 'chrome');
  const cookies = cookieFor(platform);
  if (cookies) { args.push('--cookies', cookies); job.track(cookies); }
  args.push('--', req.sourceUrl);

  let lastPct = 0;
  const onOut = chunk => {
    const m = chunk.match(/\[download\]\s+([\d.]+)%/g);
    if (m) {
      const pct = parseFloat(m[m.length - 1].replace(/[^\d.]/g, ''));
      if (pct > lastPct) { lastPct = pct; job.progress(15 + pct * 0.65, 'Завантаження'); }
    }
  };
  try {
    await runProc(YTDLP_BIN, args, { timeoutMs: 6 * 60 * 1000, onStdout: onOut });
  } catch (e) {
    const cls = classifyYtdlpError(e.stderr || e.message);
    throw new Error(cls || ('yt-dlp: ' + String(e.message).slice(0, 220)));
  }
  const dir = path.dirname(base);
  const prefix = path.basename(base);
  const files = (await fsp.readdir(dir)).filter(f => f.startsWith(prefix) && !/\.(part|ytdl|temp)$/i.test(f)).map(f => path.join(dir, f));
  if (!files.length) throw new Error('yt-dlp: файл не створено');
  files.sort((a, b) => fileSize(b) - fileSize(a));
  return { file: files[0] };
}

async function engineYtdlp(job, req) {
  if (!ytdlpVersion && ytdlpInit) await Promise.race([ytdlpInit, sleep(25000)]);
  if (!ytdlpVersion) throw new Error('YTDLP_MISSING: ' + (ytdlpLastError || 'не встановлено'));
  const platform = req.platform;
  let attempts;
  if (platform === 'youtube') {
    attempts = YT_COOKIES_RAW
      ? [{ clients: '' }, { clients: 'tv,mweb' }, { clients: 'android_vr,web_embedded' }]
      : [{ clients: 'android_vr,web_embedded' }, { clients: 'tv,mweb' }, { clients: '' }];
  } else if (platform === 'tiktok' || platform === 'instagram' || platform === 'twitter') {
    attempts = [{ impersonate: true }, { impersonate: false }];
  } else attempts = [{ impersonate: false }];
  if (req.dry) {
    const r = await runProc(YTDLP_BIN, ['--ignore-config', '--no-warnings', '--no-playlist', '--skip-download', '-J',
      ...(platform === 'youtube' ? ['--js-runtimes', 'node', '--remote-components', 'ejs:github', '--extractor-args', 'youtube:player_client=' + (attempts[0].clients || 'default')] : []),
      ...((platform === 'youtube' && YT_PROXY) || YTDLP_PROXY ? ['--proxy', (platform === 'youtube' && YT_PROXY) || YTDLP_PROXY] : []),
      '--', req.sourceUrl], { timeoutMs: 90000 }).catch(e => { throw new Error(classifyYtdlpError(e.stderr || e.message) || ('yt-dlp: ' + e.message.slice(0, 160))); });
    const j = JSON.parse(r.stdout);
    return { dry: true, info: (j.title || '') + ' | formats=' + ((j.formats || []).length) };
  }
  const errs = [];
  for (const a of attempts) {
    if (Date.now() > job.deadline) break;
    try {
      return await ytdlpDownload(job, req, a);
    } catch (e) {
      errs.push(e.message);
      if (/^(PRIVATE|UNAVAILABLE|AGE_RESTRICTED|UNSUPPORTED_URL)/.test(e.message)) break;
    }
  }
  throw new Error(uniq(errs).join(' | ').slice(0, 300));
}

// ---- Cobalt ----
async function cobaltResolve(req) {
  if (!cobaltReady) throw new Error('COBALT_DOWN' + (cobaltLastError ? ': ' + cobaltLastError : ''));
  const q = Math.min(1080, Number(req.quality) || 1080);
  const body = {
    url: req.sourceUrl,
    videoQuality: String(q),
    downloadMode: req.audioOnly ? 'audio' : 'auto',
    filenameStyle: 'basic',
    allowH265: false,
    tiktokFullAudio: true,
    ...(req.platform === 'youtube' ? { youtubeVideoCodec: 'h264' } : {}),
    ...(req.platform === 'youtube' && req.dubLang && req.dubLang !== 'orig' ? { youtubeDubLang: req.dubLang } : {}),
    ...(req.audioOnly ? { audioFormat: 'mp3', audioBitrate: '128' } : {})
  };
  const r = await fetchJson('http://127.0.0.1:' + COBALT_PORT + '/', {
    method: 'POST', headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'User-Agent': UA }, body: JSON.stringify(body)
  }, 25000);
  const j = r.json;
  if (!j) throw new Error('cobalt HTTP ' + r.status);
  if (j.status === 'error') throw new Error('cobalt:' + ((j.error && j.error.code) || 'error').replace(/^error\.api\./, ''));
  if (j.status === 'picker') {
    if (req.audioOnly && j.audio) return { url: toLocalCobalt(j.audio) };
    const pick = (j.picker || []).find(p => p && /video|gif/i.test(p.type || '')) || (j.picker || [])[0];
    if (!pick || !pick.url) throw new Error('cobalt:picker-empty');
    if (!/video|gif/i.test(pick.type || 'video') && !req.audioOnly) throw new Error('SLIDESHOW');
    return { url: toLocalCobalt(pick.url) };
  }
  if (j.status === 'local-processing' && Array.isArray(j.tunnel) && j.tunnel.length) {
    return { url: toLocalCobalt(j.tunnel[0]), audioUrl: j.tunnel[1] ? toLocalCobalt(j.tunnel[1]) : '' };
  }
  const u = typeof j.url === 'string' ? j.url : (typeof j.tunnel === 'string' ? j.tunnel : '');
  if (!u) throw new Error('cobalt:no-url(' + j.status + ')');
  return { url: toLocalCobalt(u) };
}

async function engineCobalt(job, req) {
  const r = await cobaltResolve(req);
  if (req.dry) return { dry: true, info: r.url.slice(0, 80) };
  return downloadPlan(job, { ...r, ua: UA, referer: '' }, req);
}

// ---- TikWM (TikTok) ----
let tikwmChain = Promise.resolve();
function tikwmThrottle(fn) {
  const run = tikwmChain.then(() => sleep(1100)).then(fn, fn);
  tikwmChain = run.catch(() => {});
  return run;
}

async function engineTikwm(job, req) {
  if (req.platform !== 'tiktok') throw new Error('N/A');
  const r = await tikwmThrottle(() => fetchJson('https://www.tikwm.com/api/?hd=1&url=' + encodeURIComponent(req.sourceUrl), { headers: { 'User-Agent': UA, Accept: 'application/json' } }, 15000));
  const j = r.json;
  if (!j || j.code !== 0 || !j.data) throw new Error('tikwm:' + ((j && j.msg) || ('HTTP ' + r.status)).toString().slice(0, 80));
  const d = j.data;
  if (Array.isArray(d.images) && d.images.length && !d.play && !d.hdplay) throw new Error('SLIDESHOW');
  const abs = u => !u ? '' : (/^https?:/i.test(u) ? u : 'https://www.tikwm.com' + (u.startsWith('/') ? '' : '/') + u);
  const vurl = req.audioOnly ? abs(d.music) : abs(d.hdplay || d.play || d.wmplay);
  if (!vurl) throw new Error('tikwm:no-url');
  if (req.dry) return { dry: true, info: vurl.slice(0, 80) };
  const out = await downloadPlan(job, { url: vurl, referer: 'https://www.tikwm.com/', ua: UA }, req);
  return { ...out, title: String(d.title || '').slice(0, 200), author: d.author && (d.author.unique_id || d.author.nickname) };
}

// ---- X / Twitter через fxtwitter ----
function parseTweetUrl(u) {
  const m = String(u || '').match(/(?:x|twitter|fxtwitter|vxtwitter|fixupx)\.com\/([A-Za-z0-9_]+)\/status(?:es)?\/(\d{10,22})/i) ||
            String(u || '').match(/(?:x|twitter)\.com\/i\/(?:web\/)?status\/(\d{10,22})/i);
  if (!m) return null;
  return m.length === 3 ? { user: m[1], id: m[2] } : { user: 'i', id: m[1] };
}

async function engineFxtwitter(job, req) {
  if (req.platform !== 'twitter') throw new Error('N/A');
  const t = parseTweetUrl(req.sourceUrl);
  if (!t) throw new Error('fx:no-status-id');
  const media = await resolveTweetVideo(t.user, t.id);
  if (!media || !media.videoUrl) throw new Error('fx:no-video');
  if (req.dry) return { dry: true, info: media.videoUrl.slice(0, 80) };
  const out = await downloadPlan(job, { url: media.videoUrl, referer: 'https://x.com/', ua: UA }, req);
  return { ...out, title: media.text };
}

// ---- YouTube Innertube напряму з IP Render (резерв, якщо yt-dlp недоступний) ----
const YT_CLIENTS = [
  { n: 28, name: 'ANDROID_VR', ver: '1.60.19', extra: { androidSdkVersion: 32, osName: 'Android', osVersion: '12L', deviceMake: 'Oculus', deviceModel: 'Quest 3' }, ua: 'com.google.android.apps.youtube.vr.oculus/1.60.19 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip' },
  { n: 85, name: 'TVHTML5_SIMPLY_EMBEDDED_PLAYER', ver: '2.0', extra: {}, thirdParty: { embedUrl: 'https://www.youtube.com/' }, ua: 'Mozilla/5.0 (PlayStation; PlayStation 5/2.26) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15' },
  { n: 5, name: 'IOS', ver: '19.45.4', extra: { deviceMake: 'Apple', deviceModel: 'iPhone16,2', osName: 'iPhone', osVersion: '18.1.0.22B83' }, ua: 'com.google.ios.youtube/19.45.4 (iPhone16,2; U; CPU iOS 18_1 like Mac OS X)' },
  { n: 3, name: 'ANDROID', ver: '19.44.38', extra: { androidSdkVersion: 34, osName: 'Android', osVersion: '14' }, ua: 'com.google.android.youtube/19.44.38 (Linux; U; Android 14) gzip' }
];

const DUB_FLAGS = { uk: '🇺🇦 UK', ua: '🇺🇦 UK', en: '🇬🇧 EN', ru: 'RU', pl: '🇵🇱 PL', de: '🇩🇪 DE', es: '🇪🇸 ES', fr: '🇫🇷 FR', it: '🇮🇹 IT', pt: '🇧🇷 PT', tr: '🇹🇷 TR', ar: '🇸🇦 AR', hi: '🇮🇳 HI', id: '🇮🇩 ID', kk: '🇰🇿 KK', ja: '🇯🇵 JA', ko: '🇰🇷 KO', zh: '🇨🇳 ZH' };
function normDubCode(rawIdOrLocale, rawName = '') {
  const s = String(rawIdOrLocale || '').toLowerCase().split(/[._-]/)[0];
  if (s && s.length >= 2 && s.length <= 3) return s === 'ua' ? 'uk' : s;
  const n = String(rawName || '').toLowerCase();
  if (/укр|ukrain/.test(n)) return 'uk';
  if (/рус|russian/.test(n)) return 'ru';
  if (/англ|english/.test(n)) return 'en';
  return s || 'orig';
}
function formatDubLabel(code, rawName = '') {
  if (DUB_FLAGS[code]) return DUB_FLAGS[code];
  const clean = String(rawName || '').replace(/\s*\(.*?\)\s*/g, '').trim();
  return (clean || code).toUpperCase().slice(0, 10);
}

async function queryYtInnertube(id) {
  const reasons = [];
  const settled = await Promise.allSettled(YT_CLIENTS.map(async c => {
    const r = await fetchJson('https://www.youtube.com/youtubei/v1/player?prettyPrint=false', {
      method: 'POST',
      headers: { 'User-Agent': c.ua, 'Content-Type': 'application/json', 'X-YouTube-Client-Name': String(c.n), 'X-YouTube-Client-Version': c.ver, Origin: 'https://www.youtube.com' },
      body: JSON.stringify({
        videoId: id, contentCheckOk: true, racyCheckOk: true,
        context: { client: { clientName: c.name, clientVersion: c.ver, hl: 'uk', gl: 'UA', ...c.extra }, ...(c.thirdParty ? { thirdParty: c.thirdParty } : {}) }
      })
    }, 9000);
    const j = r.json;
    const ps = j && j.playabilityStatus;
    if (!ps || ps.status !== 'OK') throw new Error(c.name + ':' + ((ps && ps.status) || ('HTTP' + r.status)) + (ps && ps.reason ? '(' + String(ps.reason).slice(0, 60) + ')' : ''));
    return { client: c, data: j };
  }));
  const ok = [];
  for (const s of settled) { if (s.status === 'fulfilled') ok.push(s.value); else reasons.push(String(s.reason && s.reason.message || s.reason)); }
  if (!ok.length) throw new Error('INNERTUBE_ALL_FAILED: ' + reasons.join(' | ').slice(0, 300));

  const audioTracksMap = new Map();
  const vStreams = [];
  const progStreams = [];
  let hlsUrl = '', hlsUa = UA, title = '', author = '';
  for (const { client: c, data: j } of ok) {
    const vd = j.videoDetails || {};
    if (!title && vd.title) title = String(vd.title);
    if (!author && vd.author) author = String(vd.author);
    const sd = j.streamingData || {};
    if (!hlsUrl && sd.hlsManifestUrl) { hlsUrl = sd.hlsManifestUrl; hlsUa = c.ua; }
    for (const f of (sd.adaptiveFormats || [])) {
      if (!f || !f.url) continue;
      const mime = String(f.mimeType || '');
      if (/audio\/mp4/i.test(mime)) {
        const at = f.audioTrack || {};
        const code = normDubCode(at.id, at.displayName);
        const br = Number(f.bitrate || 0);
        const prev = audioTracksMap.get(code);
        if (!prev || br > prev.bitrate) audioTracksMap.set(code, { lang: code, label: formatDubLabel(code, at.displayName), url: f.url, customUa: c.ua, bitrate: br, size: Number(f.contentLength) || 0, isDefault: !!at.audioIsDefault });
      } else if (/video\/mp4.*avc1/i.test(mime)) {
        const q = Number((String(f.qualityLabel || '').match(/(\d{3,4})/) || [])[1] || f.height || 0);
        if (q > 0 && !vStreams.some(x => x.q === q)) vStreams.push({ url: f.url, q, customUa: c.ua, size: Number(f.contentLength) || 0 });
      }
    }
    for (const f of (sd.formats || [])) {
      if (!f || !f.url || !/video\/mp4/i.test(String(f.mimeType || ''))) continue;
      const q = Number((String(f.qualityLabel || '').match(/(\d{3,4})/) || [])[1] || f.height || 0);
      const at = f.audioTrack || {};
      const dubLang = at.id ? normDubCode(at.id, at.displayName) : '';
      if (q > 0 && !progStreams.some(x => x.q === q && x.dubLang === dubLang)) progStreams.push({ url: f.url, q, dubLang, customUa: c.ua, size: Number(f.contentLength) || 0 });
    }
  }
  return { audioTracks: [...audioTracksMap.values()], vStreams, progStreams, hlsUrl, hlsUa, title, author, thumbnail: 'https://i.ytimg.com/vi/' + id + '/hqdefault.jpg' };
}

async function engineInnertube(job, req) {
  if (req.platform !== 'youtube') throw new Error('N/A');
  const id = extractYtId(req.sourceUrl);
  if (!id) throw new Error('NO_YT_ID');
  const inner = await queryYtInnertube(id);
  if (req.dry) return { dry: true, info: 'audioTracks=' + inner.audioTracks.length + ' v=' + inner.vStreams.length + ' prog=' + inner.progStreams.length };
  const qNum = Math.min(1080, Number(req.quality) || 1080);
  const dub = req.dubLang && req.dubLang !== 'orig' ? normDubCode(req.dubLang, req.dubLang) : '';
  const tracks = inner.audioTracks;
  let audio = dub ? tracks.find(t => t.lang === dub) : null;
  if (!audio) audio = tracks.find(t => t.isDefault) || [...tracks].sort((a, b) => b.bitrate - a.bitrate)[0] || null;
  const ref = 'https://www.youtube.com/';
  if (req.audioOnly) {
    if (!audio) throw new Error('innertube:no-audio');
    return downloadPlan(job, { url: audio.url, ua: audio.customUa, referer: ref }, req);
  }
  const adapt = inner.vStreams.find(v => v.q === qNum) || inner.vStreams.filter(v => v.q <= qNum).sort((a, b) => b.q - a.q)[0];
  if (adapt && audio) return downloadPlan(job, { url: adapt.url, audioUrl: audio.url, ua: adapt.customUa, audioUa: audio.customUa, referer: ref }, req);
  if (!dub || tracks.length <= 1) {
    const prog = inner.progStreams.find(v => v.q === qNum) || [...inner.progStreams].sort((a, b) => b.q - a.q)[0];
    if (prog) return downloadPlan(job, { url: prog.url, ua: prog.customUa, referer: ref }, req);
  }
  if (inner.hlsUrl) return downloadPlan(job, { url: inner.hlsUrl, ua: inner.hlsUa, referer: ref }, req);
  throw new Error('innertube:no-streams');
}

// ---- сторонні YouTube-сервіси (останній резерв, можуть бути нестабільними) ----
async function engineYtThirdParty(job, req) {
  if (req.platform !== 'youtube') throw new Error('N/A');
  const id = extractYtId(req.sourceUrl);
  if (!id) throw new Error('NO_YT_ID');
  const canon = 'https://www.youtube.com/watch?v=' + id;
  const qNum = Math.min(1080, Number(req.quality) || 720);
  const errs = [];
  const found = [];
  // SaveTube
  for (const base of ['https://cdn59.savetube.su', 'https://cdn54.savetube.me', 'https://cdn61.savetube.su']) {
    try {
      const r = await fetchJson(base + '/info?url=' + encodeURIComponent(canon), { headers: { 'User-Agent': UA, Accept: 'application/json', Origin: 'https://yt.savetube.me', Referer: 'https://yt.savetube.me/' } }, 6000);
      const d = r.json && r.json.data;
      if (!d) continue;
      if (req.audioOnly) { const a = (d.audio_formats || []).find(x => x && x.url); if (a) { found.push({ url: a.url, referer: 'https://yt.savetube.me/' }); break; } }
      const vids = (d.video_formats || []).filter(v => v && v.url && !(/googlevideo\.com/i.test(v.url) && /[?&]ip=/i.test(v.url)));
      const pick = vids.find(v => Number(v.height || v.quality) === qNum) || vids[0];
      if (pick) { found.push({ url: pick.url, referer: 'https://yt.savetube.me/' }); break; }
    } catch (e) { errs.push('savetube:' + e.message.slice(0, 40)); }
  }
  // Invidious
  if (!found.length && !req.audioOnly) {
    for (const base of ['https://inv.nadeko.net', 'https://invidious.nerdvpn.de', 'https://yewtu.be']) {
      try {
        const r = await fetchJson(base + '/api/v1/videos/' + encodeURIComponent(id), { headers: { 'User-Agent': UA, Accept: 'application/json' } }, 6000);
        const streams = ((r.json && r.json.formatStreams) || []).filter(f => f && f.itag);
        if (streams.length) {
          const pick = streams.find(f => String(f.qualityLabel || '').includes(String(qNum))) || streams[streams.length - 1];
          found.push({ url: base + '/latest_version?id=' + encodeURIComponent(id) + '&itag=' + pick.itag + '&local=true', referer: base + '/' });
          break;
        }
      } catch (e) { errs.push('invidious:' + e.message.slice(0, 40)); }
    }
  }
  if (!found.length) throw new Error('thirdparty: ' + (errs.join(',') || 'нічого не знайдено'));
  if (req.dry) return { dry: true, info: found[0].url.slice(0, 80) };
  return downloadPlan(job, { url: found[0].url, ua: UA, referer: found[0].referer }, req);
}

// ---- URL, який передав бот (тільки якщо він ще "живий") ----
async function engineProvided(job, req) {
  if (!req.providedUrl) throw new Error('N/A');
  if (req.tunnelUsed) throw new Error('N/A:tunnel-used');
  if (req.platform === 'youtube') throw new Error('N/A:yt');
  if (req.dry) return { dry: true, info: req.providedUrl.slice(0, 80) };
  return downloadPlan(job, { url: toLocalCobalt(req.providedUrl), audioUrl: req.providedAudio ? toLocalCobalt(req.providedAudio) : '', ua: req.ua || UA, referer: req.referer || '' }, req);
}

// ---- спільне: завантажити (video+audio / HLS / файл) за готовим планом ----
async function downloadPlan(job, plan, req) {
  const isHls = /\.m3u8(\?|$)/i.test(plan.url);
  const outV = job.tmp('.mp4');
  if (isHls) {
    job.progress(30, 'Завантаження потоку');
    await hlsToMp4(plan.url, outV, { ua: plan.ua || UA, referer: plan.referer || '' });
    return { file: outV };
  }
  if (plan.audioUrl && !req.audioOnly) {
    const outA = job.tmp('.m4a');
    await Promise.all([
      downloadToFile(plan.url, outV, { referer: plan.referer, ua: plan.ua || UA, onProgress: p => job.progress(15 + p * 0.55, 'Завантаження відео') }),
      downloadToFile(plan.audioUrl, outA, { referer: plan.referer, ua: plan.audioUa || plan.ua || UA })
    ]);
    job.progress(78, 'Збірка MP4');
    const merged = job.tmp('.mp4');
    await muxVideoAudio(outV, outA, merged);
    return { file: merged };
  }
  const ext = req.audioOnly ? '.mp3' : '.mp4';
  const out = job.tmp(ext);
  await downloadToFile(plan.url, out, { referer: plan.referer, ua: plan.ua || UA, onProgress: p => job.progress(15 + p * 0.65, 'Завантаження') });
  return { file: out };
}

const ENGINE_TIMEOUT = { provided: 120000, cobalt: 200000, ytdlp: 8 * 60 * 1000, tikwm: 150000, fxtwitter: 150000, innertube: 220000, thirdparty: 150000 };
const ENGINES = { provided: engineProvided, cobalt: engineCobalt, ytdlp: engineYtdlp, tikwm: engineTikwm, fxtwitter: engineFxtwitter, innertube: engineInnertube, thirdparty: engineYtThirdParty };

function enginesFor(req) {
  const first = req.providedUrl && !req.tunnelUsed && req.platform !== 'youtube' ? ['provided'] : [];
  switch (req.platform) {
    case 'youtube':   return ['ytdlp', 'innertube', 'cobalt', 'thirdparty'];
    case 'tiktok':    return [...first, 'cobalt', 'tikwm', 'ytdlp'];
    case 'twitter':   return [...first, 'fxtwitter', 'cobalt', 'ytdlp'];
    case 'instagram': return [...first, 'cobalt', 'ytdlp'];
    default:          return [...first, 'cobalt', 'ytdlp'];
  }
}

// Послідовно пробує рушії. У dry-режимі пробує ВСІ (для діагностики) і нічого не качає.
async function acquire(job, req) {
  const names = enginesFor(req);
  const results = [];
  for (const name of names) {
    if (!req.dry && Date.now() > job.deadline) { job.diag.push({ engine: name, ok: false, ms: 0, err: 'DEADLINE' }); break; }
    const t0 = Date.now();
    try {
      const r = await withTimeout(ENGINES[name](job, req), req.dry ? 100000 : (ENGINE_TIMEOUT[name] || 150000), name);
      job.diag.push({ engine: name, ok: true, ms: Date.now() - t0, info: r.info || '' });
      if (!req.dry) return { ...r, engine: name };
      results.push(name);
    } catch (e) {
      const msg = redact(e && e.message).slice(0, 220);
      if (!/^N\/A/.test(msg)) job.diag.push({ engine: name, ok: false, ms: Date.now() - t0, err: msg });
      await job.cleanupPartial();
      if (/^SLIDESHOW/.test(msg) && !req.dry) throw Object.assign(new Error('SLIDESHOW'), { code: 'SLIDESHOW' });
    }
  }
  if (req.dry) return { dry: true, working: results };
  const err = new Error('ENGINES_FAILED');
  err.detail = job.diag.filter(d => !d.ok).map(d => d.engine + ': ' + d.err).join(' | ').slice(0, 700);
  throw err;
}

// ==================== 7. ЗАВДАННЯ (черга, прогрес, гібридна відповідь) ====================
const jobs = new Map();       // id -> інфо для /job/:id
let activeJobs = 0;
const jobQueue = [];

function acquireSlot() {
  return new Promise(resolve => {
    const go = () => { activeJobs++; resolve(() => { activeJobs--; const n = jobQueue.shift(); if (n) n(); }); };
    if (activeJobs < MAX_JOBS) go(); else jobQueue.push(go);
  });
}

function createJob(kind, body = {}) {
  const job = {
    id: crypto.randomBytes(5).toString('hex'),
    kind,
    startedAt: Date.now(),
    deadline: Date.now() + JOB_DEADLINE_MS,
    diag: [],
    pct: 0,
    stage: '',
    detached: false,
    statusMsgId: 0,
    paths: [],
    token: body.botToken,
    chatId: body.chatId,
    progMsgId: body.progressMsgId || 0,
    bizConn: body.businessConnectionId || '',
    replyTo: body.replyToMsgId || body.replyToMessageId || 0,
    threadId: body.messageThreadId || 0,
    tmp(ext) { const f = tmpName(ext); this.paths.push(f); return f; },
    track(p) { this.paths.push(p); },
    progress(pct, stage) {
      if (pct > this.pct) this.pct = Math.min(99, pct);
      if (stage) this.stage = stage;
      const target = this.detached ? this.statusMsgId : this.progMsgId;
      if (target && this.token) editStatus(this.token, this.chatId, target, progressText(this.pct, this.stage), { bizConn: this.bizConn }).catch(() => {});
    },
    async cleanupPartial() {
      // видаляємо тимчасові файли, створені невдалим рушієм
      const list = this.paths.splice(0);
      for (const p of list) {
        if (p.endsWith('*')) {
          const dir = path.dirname(p); const prefix = path.basename(p).slice(0, -1);
          try { for (const f of await fsp.readdir(dir)) if (f.startsWith(prefix)) safeUnlink(path.join(dir, f)); } catch (e) {}
        } else safeUnlink(p);
      }
    },
    async cleanupAll() { await this.cleanupPartial(); }
  };
  jobs.set(job.id, { id: job.id, kind, startedAt: job.startedAt, status: 'running', info: null });
  if (jobs.size > 200) jobs.delete(jobs.keys().next().value);
  return job;
}

function finishJobInfo(job, status, info) {
  const j = jobs.get(job.id);
  if (j) { j.status = status; j.info = info; j.finishedAt = Date.now(); }
}

// Виконує роботу; якщо вона триває довше waitMs — відповідає pending, а роботу доробляє у фоні.
async function runHybrid(job, body, work, failMarkup) {
  const waitMs = Math.max(3000, Math.min(Number(body.waitMs) || SEND_WAIT_MS, 40000));
  const release = await acquireSlot();
  const started = Date.now();
  const promise = (async () => {
    try {
      const res = await work();
      finishJobInfo(job, 'done', { ms: Date.now() - started, diag: job.diag });
      if (job.detached) await onDetachedDone(job, true);
      return res;
    } catch (e) {
      const out = describeFailure(e, job);
      finishJobInfo(job, 'failed', out);
      if (job.detached) await onDetachedDone(job, false, out, body, failMarkup);
      return { ok: false, ...out };
    } finally {
      await job.cleanupAll();
      release();
    }
  })();
  const raced = await Promise.race([promise.then(r => ({ done: true, r })), sleep(waitMs).then(() => ({ done: false }))]);
  if (raced.done) return raced.r;
  // довге завантаження: відповідаємо боту, що все прийнято, і створюємо власне статусне повідомлення
  job.detached = true;
  try {
    const m = await tg(job.token, 'sendMessage', {
      chat_id: job.chatId,
      text: progressText(Math.max(job.pct, 5), job.stage || 'Завантаження') + '\n<i>Великий файл — це може зайняти кілька хвилин.</i>',
      parse_mode: 'HTML',
      ...(job.bizConn ? { business_connection_id: job.bizConn } : {}),
      ...(job.replyTo ? { reply_parameters: { message_id: Number(job.replyTo), allow_sending_without_reply: true } } : {}),
      ...(job.threadId ? { message_thread_id: Number(job.threadId) } : {})
    }, { timeout: 10000, noRetry: true });
    job.statusMsgId = m.message_id;
  } catch (e) { log('job', job.id, 'не вдалось створити статус-повідомлення:', redact(e.message).slice(0, 100)); }
  return { ok: true, pending: true, jobId: job.id };
}

async function onDetachedDone(job, ok, out, body, failMarkup) {
  try {
    const cleanIds = [job.statusMsgId].filter(Boolean);
    if (ok) {
      for (const id of cleanIds) await tg(job.token, 'deleteMessage', { chat_id: job.chatId, message_id: id, ...(job.bizConn ? {} : {}) }, { timeout: 8000, noRetry: true }).catch(() => {});
      return;
    }
    const text = '😕 ' + esc(body.failText || 'Не вдалося завантажити відео. Спробуй ще раз пізніше або інше посилання.') +
      (body.debug && out && out.detail ? '\n<code>' + esc(String(out.detail).slice(0, 500)) + '</code>' : '');
    const markup = failMarkup || (body.sourceUrl ? { inline_keyboard: [[{ text: '🔗 ' + (body.linkText || 'Open'), url: body.sourceUrl }]] } : undefined);
    if (job.statusMsgId) {
      await tg(job.token, 'editMessageText', { chat_id: job.chatId, message_id: job.statusMsgId, text, parse_mode: 'HTML', ...(markup ? { reply_markup: markup } : {}), link_preview_options: { is_disabled: true } }, { timeout: 10000, noRetry: true })
        .catch(() => tg(job.token, 'sendMessage', { chat_id: job.chatId, text, parse_mode: 'HTML', ...(markup ? { reply_markup: markup } : {}) }, { timeout: 10000, noRetry: true }).catch(() => {}));
    } else {
      await tg(job.token, 'sendMessage', { chat_id: job.chatId, text, parse_mode: 'HTML', ...(markup ? { reply_markup: markup } : {}) }, { timeout: 10000, noRetry: true }).catch(() => {});
    }
  } catch (e) { log('job', job.id, 'onDetachedDone:', e.message); }
}

function describeFailure(e, job) {
  const msg = String((e && e.message) || e);
  let error = 'FAILED';
  if (msg === 'ENGINES_FAILED') error = 'ENGINES_FAILED';
  else if (msg === 'SLIDESHOW') error = 'SLIDESHOW';
  else if (/^TG_REJECTED/.test(msg)) error = 'TG_REJECTED';
  else if (/^TG_TOO_BIG|TOO_BIG/.test(msg)) error = 'TOO_BIG';
  else if (/^TG_UPLOAD_FAILED/.test(msg)) error = 'TG_UPLOAD_FAILED';
  else if (/^NOT_VIDEO|^BAD_MEDIA/.test(msg)) error = 'BAD_MEDIA';
  const detail = (e && e.detail) || redact(msg).slice(0, 600);
  log('job', job.id, job.kind, 'ПОМИЛКА', error, String(detail).slice(0, 300));
  return { error, detail: String(detail).slice(0, 700), diag: job.diag };
}

// ==================== 8. /send ====================
async function handleSend(body) {
  const b = body || {};
  const { botToken, chatId } = b;
  const sourceUrl = String(b.sourceUrl || '').trim();
  const providedUrl = String(b.url || '').trim();
  if (!/^\d{5,}:[A-Za-z0-9_-]{20,}$/.test(String(botToken || '')) || !chatId || (!sourceUrl && !providedUrl)) return { ok: false, error: 'MISSING_PARAMS' };
  if (!runtime.mainToken && !env('BOT_TOKEN') && !env('STATE_BOT_TOKEN')) { runtime.mainToken = botToken; Store.load().then(() => { if (Store.dirty) Store.saveNow(); }).catch(() => {}); }

  const mode = SEND_MODES[b.mode] ? b.mode : 'video';
  const job = createJob('send', b);
  const req = {
    sourceUrl: sourceUrl || providedUrl,
    platform: detectPlatform(sourceUrl || providedUrl),
    quality: Math.min(1080, Number(b.quality === 'max' ? 1080 : b.quality) || 1080),
    dubLang: String(b.dubLang || ''),
    audioOnly: mode === 'audio',
    providedUrl, providedAudio: String(b.audioUrl || ''),
    tunnelUsed: !!b.tunnelUsed,
    ua: String(b.customUa || ''), referer: String(b.referer || '')
  };
  job.progress(6, 'Пошук джерела');

  const work = async () => {
    const qualities = req.audioOnly ? [req.quality] : uniq([req.quality, 720, 480, 360].filter(q => q <= req.quality));
    let lastErr = null;
    for (let qi = 0; qi < qualities.length && qi < 3; qi++) {
      const rq = { ...req, quality: qualities[qi] };
      const got = await acquire(job, rq);
      job.progress(86, 'Перевірка файлу');
      let file = got.file;
      let info;
      try { info = await probe(file); } catch (e) { throw new Error('BAD_MEDIA: ' + String(e.message).slice(0, 120)); }
      if (rq.audioOnly ? !(info.hasAudio) : !info.hasVideo) throw new Error('BAD_MEDIA: у файлі немає ' + (rq.audioOnly ? 'аудіо' : 'відео'));
      let sendMode = mode;
      let thumb = null;
      if (mode === 'video') {
        const norm = await normalizeForTelegram(file, info);
        if (norm.file !== file) { job.paths.push(norm.file); file = norm.file; info = await probe(file).catch(() => info); }
        if (norm.incompatible) sendMode = 'document';
        else thumb = await makeThumb(file, info);
        if (thumb) job.paths.push(thumb);
      } else if (mode === 'document') {
        // як файл: віддаємо як є, але з faststart для зручного перегляду
      }
      job.progress(93, 'Відправка в Telegram');
      try {
        const result = await sendFileToTelegram({
          token: botToken, chatId, mode: sendMode, file, caption: b.caption, markup: b.replyMarkup,
          replyTo: job.replyTo, bizConn: job.bizConn, threadId: job.threadId, thumb,
          fileName: sendMode === 'document' ? ('video_' + Date.now().toString(36) + '.mp4') : undefined,
          meta: { width: info.width, height: info.height, duration: info.duration, title: got.title, performer: got.author }
        });
        job.progress(99, 'Готово');
        log('job', job.id, 'OK', req.platform, got.engine, Math.round(fileSize(file) / 1024) + 'KB', Math.round((Date.now() - job.startedAt) / 1000) + 's');
        return { ok: true, result, engine: got.engine };
      } catch (e) {
        lastErr = e;
        if (e.tooBig && !req.audioOnly && qi < qualities.length - 1) {
          log('job', job.id, 'файл завеликий (' + Math.round(fileSize(file) / MB) + ' МБ), пробую нижчу якість');
          await job.cleanupPartial();
          continue;
        }
        throw e;
      }
    }
    throw lastErr || new Error('FAILED');
  };
  return runHybrid(job, b, work);
}

// ==================== 9. /circle (відео → кружечок) ====================
async function handleCircle(body) {
  const b = body || {};
  const { botToken, chatId, fileId = '', url = '' } = b;
  if (!/^\d{5,}:[A-Za-z0-9_-]{20,}$/.test(String(botToken || '')) || !chatId || (!fileId && !url)) return { ok: false, error: 'MISSING_PARAMS' };
  const job = createJob('circle', b);
  job.progress(8, 'Завантаження відео');
  const work = async () => {
    const inPath = job.tmp('_in.mp4');
    const outPath = job.tmp('_circle.mp4');
    let input = inPath;
    if (url) {
      await downloadToFile(toLocalCobalt(url), inPath, { referer: String(b.referer || '') });
    } else {
      let fp = '';
      try { const f = await tg(botToken, 'getFile', { file_id: fileId }, { timeout: 20000, noRetry: true }); fp = f.file_path; if (fp) await downloadToFile(TG_API_BASE + '/file/bot' + botToken + '/' + fp, inPath); }
      catch (e) { fp = ''; }
      if (!fileSize(inPath)) {
        if (!localApiReady) throw new Error('GET_FILE_FAILED: файл більше 20 МБ, а Local Bot API не запущено');
        const f2 = await tg(botToken, 'getFile', { file_id: fileId }, { local: true, timeout: 30000, noRetry: true });
        const p2 = f2 && f2.file_path;
        if (p2 && fs.existsSync(p2)) input = p2;
        else if (p2) await downloadToFile('http://127.0.0.1:' + LOCAL_API_PORT + '/file/bot' + botToken + '/' + String(p2).replace(/^\/+/, ''), inPath);
        else throw new Error('GET_FILE_FAILED');
      }
    }
    job.progress(55, 'Обрізка у кружечок');
    await runProc('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-threads', '0', '-t', '60', '-i', input,
      '-vf', "crop='min(iw,ih)':'min(iw,ih)',scale=384:384,setsar=1", '-r', '30', '-c:v', 'libx264', '-preset', 'ultrafast', '-profile:v', 'baseline', '-level', '3.0',
      '-pix_fmt', 'yuv420p', '-crf', '25', '-c:a', 'aac', '-ac', '1', '-ar', '44100', '-b:a', '96k', '-movflags', '+faststart', '-y', outPath], { timeoutMs: 120000 });
    job.progress(92, 'Відправка кружечка');
    const info = await probe(outPath).catch(() => ({}));
    const result = await sendFileToTelegram({
      token: botToken, chatId, mode: 'video_note', file: outPath, markup: b.replyMarkup,
      replyTo: job.replyTo, bizConn: job.bizConn, threadId: job.threadId, meta: { duration: info.duration }
    });
    return { ok: true, result };
  };
  return runHybrid(job, b, work);
}

// ==================== 10. /stickers (нарізка відео на квадрати) ====================
async function handleStickers(body) {
  const b = body || {};
  const { botToken, chatId, url = '', sourceUrl = '', fileId = '' } = b;
  if (!/^\d{5,}:[A-Za-z0-9_-]{20,}$/.test(String(botToken || '')) || !chatId || (!url && !sourceUrl && !fileId)) return { ok: false, error: 'MISSING_PARAMS' };
  const cols = Math.max(1, Math.min(5, Number(b.cols) || 3));
  const rows = Math.max(1, Math.min(5, Number(b.rows) || 3));
  const job = createJob('stickers', b);
  job.progress(8, 'Завантаження оригіналу');
  const work = async () => {
    let inPath = job.tmp('_grid_in.mp4');
    if (url) await downloadToFile(toLocalCobalt(url), inPath);
    else if (fileId) {
      const f = await tg(botToken, 'getFile', { file_id: fileId }, { timeout: 20000, noRetry: true });
      if (!f.file_path) throw new Error('GET_FILE_FAILED');
      await downloadToFile(TG_API_BASE + '/file/bot' + botToken + '/' + f.file_path, inPath);
    } else {
      const got = await acquire(job, { sourceUrl, platform: detectPlatform(sourceUrl), quality: 1080, dubLang: '', audioOnly: false });
      inPath = got.file;
    }
    const { width, height, duration } = await probe(inPath);
    const tile = Math.max(64, Math.min(Math.floor(width / cols), Math.floor(height / rows)));
    const gridW = tile * cols; const gridH = tile * rows;
    const sx = Math.max(0, Math.floor((width - gridW) / 2)); const sy = Math.max(0, Math.floor((height - gridH) / 2));
    const inset = Math.max(2, Math.round(tile * 0.02));
    const crop = Math.max(32, tile - inset * 2);
    const clipDur = Math.min(duration || 3, 2.95).toFixed(2);
    const total = cols * rows;
    const sent = [];
    for (let r = 0; r < rows; r++) {
      for (let c = 0; c < cols; c++) {
        const idx = r * cols + c + 1;
        job.progress(15 + (idx / total) * 70, 'Нарізка ' + idx + '/' + total);
        const tilePath = job.tmp('_sticker_' + idx + '.mp4');
        await runProc('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-i', inPath, '-t', clipDur,
          '-vf', 'crop=' + crop + ':' + crop + ':' + (sx + c * tile + inset) + ':' + (sy + r * tile + inset) + ',scale=512:512,setsar=1',
          '-c:v', 'libx264', '-preset', 'fast', '-crf', '18', '-pix_fmt', 'yuv420p', '-an', '-movflags', '+faststart', '-y', tilePath], { timeoutMs: 90000 });
        const res = await sendFileToTelegram({
          token: botToken, chatId, mode: 'document', file: tilePath, fileName: 'sticker_' + (r + 1) + '_' + (c + 1) + '_(' + idx + ').mp4',
          caption: idx === 1 ? '🧩 <b>Нарізка ' + cols + '×' + rows + ' (' + total + ' ч. · 512×512)</b>' : '',
          replyTo: idx === 1 ? job.replyTo : 0, bizConn: job.bizConn, threadId: job.threadId
        });
        sent.push(res);
        safeUnlink(tilePath);
      }
    }
    return { ok: true, count: sent.length, result: sent[0] || true };
  };
  return runHybrid(job, b, work);
}


// ==================== 10b. /stt (розпізнавання голосових, кружечків, відео та аудіо без ключів через ffmpeg + Whisper/Qwen) ====================
async function handleStt(body) {
  const b = body || {};
  const { botToken, fileId = '', url = '' } = b;
  if (!fileId && !url) return { ok: false, error: 'MISSING_PARAMS' };
  const inPath = tmpName('_stt_in');
  const outMp3 = tmpName('_stt.mp3');
  try {
    if (url) {
      await downloadToFile(toLocalCobalt(url), inPath, { maxBytes: 95 * MB, stallMs: 20000 });
    } else if (botToken && fileId) {
      let ok = false;
      try {
        const f = await tg(botToken, 'getFile', { file_id: fileId }, { timeout: 15000, noRetry: true });
        if (f && f.file_path) {
          await downloadToFile(TG_API_BASE + '/file/bot' + botToken + '/' + f.file_path, inPath, { maxBytes: 50 * MB });
          ok = fileSize(inPath) > 100;
        }
      } catch (e) {}
      if (!ok && localApiReady) {
        const f2 = await tg(botToken, 'getFile', { file_id: fileId }, { local: true, timeout: 25000, noRetry: true });
        const p2 = f2 && f2.file_path;
        if (p2 && fs.existsSync(p2)) fs.copyFileSync(p2, inPath);
        else if (p2) await downloadToFile('http://127.0.0.1:' + LOCAL_API_PORT + '/file/bot' + botToken + '/' + String(p2).replace(/^\/+/, ''), inPath);
      }
    }
    if (!fileSize(inPath)) return { ok: false, error: 'DOWNLOAD_FAILED' };

    await runProc('ffmpeg', [
      '-hide_banner', '-loglevel', 'error', '-t', '900', '-i', inPath,
      '-vn', '-ac', '1', '-ar', '16000', '-c:a', 'libmp3lame', '-b:a', '48k', '-y', outMp3
    ], { timeoutMs: 60000 });

    if (!fileSize(outMp3)) return { ok: false, error: 'FFMPEG_AUDIO_EMPTY' };
    const audioBuf = await fsp.readFile(outMp3);
    const audioBlob = new Blob([audioBuf], { type: 'audio/mpeg' });

    const tryGradioSpace = async (baseUrl, fnIndex, dataBuild) => {
      const upForm = new FormData();
      upForm.append('files', audioBlob, 'audio.mp3');
      const rUp = await fetchJson(baseUrl + '/gradio_api/upload', { method: 'POST', body: upForm, headers: { 'User-Agent': UA } }, 15000);
      const upPath = Array.isArray(rUp.json) && rUp.json[0];
      if (!upPath) throw new Error('upload-failed');
      const fileData = { path: upPath, orig_name: 'audio.mp3', mime_type: 'audio/mpeg', meta: { _type: 'gradio.FileData' } };
      const sessionHash = crypto.randomBytes(6).toString('hex');
      const rJoin = await fetchJson(baseUrl + '/gradio_api/queue/join', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'User-Agent': UA },
        body: JSON.stringify({ data: dataBuild(fileData), fn_index: fnIndex, session_hash: sessionHash })
      }, 15000);
      const eventId = rJoin.json && rJoin.json.event_id;
      if (!eventId) throw new Error('no-event-id');
      const rData = await fetchJson(baseUrl + '/gradio_api/queue/data?session_hash=' + sessionHash, {
        headers: { Accept: 'text/event-stream', 'User-Agent': UA }
      }, 45000);
      for (const line of String(rData.text || '').split('\n')) {
        if (!line.startsWith('data:')) continue;
        try {
          const msg = JSON.parse(line.slice(5).trim());
          if (msg.msg === 'process_completed' && msg.output && Array.isArray(msg.output.data)) {
            const txt = String(msg.output.data[0] || '').trim();
            if (txt) return txt;
          }
        } catch (e) {}
      }
      throw new Error('empty-transcript');
    };

    const spaces = [
      () => tryGradioSpace('https://hf-audio-whisper-large-v3-turbo.hf.space', 0, fd => [fd, 'transcribe']),
      () => tryGradioSpace('https://openai-whisper.hf.space', 0, fd => [fd, 'transcribe', false]),
      () => tryGradioSpace('https://qwen-qwen3-asr-demo.hf.space', 0, fd => [fd, '', 'auto', true])
    ];

    try {
      const text = await Promise.any(spaces.map(fn => fn()));
      if (text) return { ok: true, text };
    } catch (e) {}

    return { ok: false, error: 'STT_ALL_FAILED' };
  } finally {
    safeUnlink(inPath);
    safeUnlink(outMp3);
  }
}

// ==================== 11. /yt-info (список якостей і доріжок озвучення) ====================
async function ytInfo(rawUrl) {
  const id = extractYtId(rawUrl);
  if (!id) return { ok: false, error: 'NO_YT_ID' };
  let innerErr = '';
  try {
    const inner = await queryYtInnertube(id);
    return { ok: true, source: 'innertube', ...inner };
  } catch (e) { innerErr = e.message; }
  // Innertube із цього IP заблокований — беремо список форматів через yt-dlp (URL-адрес не віддаємо, завантаження піде через /send за sourceUrl)
  if (ytdlpVersion) {
    try {
      const url = 'https://www.youtube.com/watch?v=' + id;
      const proxy = YT_PROXY || YTDLP_PROXY;
      const cookies = cookieFor('youtube');
      const args = ['--ignore-config', '--no-warnings', '--no-playlist', '--skip-download', '-J', '--js-runtimes', 'node', '--remote-components', 'ejs:github',
        '--extractor-args', 'youtube:player_client=' + (cookies ? 'tv,mweb' : 'android_vr,web_embedded'),
        ...(proxy ? ['--proxy', proxy] : []), ...(cookies ? ['--cookies', cookies] : []), '--', url];
      const r = await runProc(YTDLP_BIN, args, { timeoutMs: 12000 }).finally(() => safeUnlink(cookies));
      const j = JSON.parse(r.stdout);
      const fmts = j.formats || [];
      const heights = uniq(fmts.filter(f => f.vcodec && f.vcodec !== 'none' && f.height).map(f => f.height)).filter(h => h <= 1080).sort((a, b) => b - a);
      const langs = new Map();
      for (const f of fmts) {
        if (!f.acodec || f.acodec === 'none' || (f.vcodec && f.vcodec !== 'none')) continue;
        const code = normDubCode(f.language || '', f.format_note || '');
        const isDefault = /default/i.test(f.format_note || '') || (f.language_preference || 0) >= 10;
        const prev = langs.get(code);
        if (!prev || (f.abr || 0) > prev.bitrate) langs.set(code, { lang: code, label: formatDubLabel(code, f.format_note || ''), url: 'ytdlp://audio/' + id + '/' + code, placeholder: true, bitrate: f.abr || 0, size: f.filesize || 0, isDefault: prev ? prev.isDefault || isDefault : isDefault });
      }
      const audioTracks = [...langs.values()];
      if (audioTracks.length && !audioTracks.some(t => t.isDefault)) audioTracks[0].isDefault = true;
      return {
        ok: true, source: 'ytdlp',
        audioTracks: audioTracks.length ? audioTracks : [{ lang: 'orig', label: 'ORIG', url: 'ytdlp://audio/' + id + '/orig', placeholder: true, bitrate: 128, size: 0, isDefault: true }],
        vStreams: heights.map(h => ({ url: 'ytdlp://video/' + id + '/' + h, q: h, placeholder: true, size: 0 })),
        progStreams: [], hlsUrl: '', hlsUa: UA,
        title: j.title || '', author: j.uploader || j.channel || '', thumbnail: j.thumbnail || ('https://i.ytimg.com/vi/' + id + '/hqdefault.jpg')
      };
    } catch (e) {
      return { ok: false, error: 'YT_INFO_FAILED', detail: (innerErr + ' || ' + (classifyYtdlpError(e.stderr || e.message) || e.message)).slice(0, 500) };
    }
  }
  return { ok: false, error: 'YT_INFO_FAILED', detail: innerErr.slice(0, 400) };
}

// ==================== 12. /diag (перевірка всіх рушіїв для посилання) ====================
async function handleDiag(rawUrl, quality = 720) {
  const url = String(rawUrl || '').trim();
  if (!/^https?:\/\//i.test(url)) return { ok: false, error: 'BAD_URL' };
  const job = createJob('diag');
  const req = { sourceUrl: url, platform: detectPlatform(url), quality: Number(quality) || 720, dubLang: '', audioOnly: false, dry: true };
  const t0 = Date.now();
  await acquire(job, req).catch(() => {});
  return { ok: true, platform: req.platform, ms: Date.now() - t0, engines: job.diag, ytdlp: ytdlpVersion || null, cobalt: cobaltReady };
}

// ==================== 13. X-ВОТЧЕР (відео-пости акаунтів → Telegram-канали + Відложка на 1 день) ====================
const X = state.x;
const X_INTERVAL_SEC = Math.max(envNum('X_MIN_INTERVAL_SEC', 5), envNum('X_INTERVAL_SEC', 8));
const X_MAX_AGE_MS = envNum('X_MAX_AGE_HOURS', 18) * 3600 * 1000;
const FX_UA = 'TelegramBot (like TwitterBot)';
const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const xr = {                       // стан виконання (не зберігається)
  lastTickAt: 0, lastOkAt: 0, lastError: '', phase: 'init', ticks: 0, rrIndex: 0,
  perAccount: {}, targetStatus: {}, discoverError: '', lastDiscoverAt: 0, lastVerifyAt: 0, botId: 0, botName: '',
  tries: {}, noVideoTries: {}, resolvedCache: new Map(), recentPostsMeta: [], test: null, diag: null
};
const srcStats = {};       // глобальна статистика по джерелах для панелі
const accSrcCooldown = {}; // окремий кулдаун acc:src, щоб збій на 1-му акаунті НІКОЛИ не блокував 2-й акаунт!

const TW_EPOCH = 1288834974657n;
const isSnowflake = id => /^\d{15,21}$/.test(String(id || ''));
const snowTs = id => { try { return Number((BigInt(id) >> 22n) + TW_EPOCH); } catch (e) { return 0; } };
const cmpId = (a, b) => { a = String(a); b = String(b); return a.length !== b.length ? a.length - b.length : (a < b ? -1 : a > b ? 1 : 0); };
const allTargets = () => uniq([...(X.targets || []), ...(X.autoChats || [])].map(parseTelegramChat).filter(Boolean));

function stat(name) { return (srcStats[name] = srcStats[name] || { ok: 0, fail: 0, consec: 0, lastErr: '', lastOkAt: 0, cooldownUntil: 0, lastItems: 0, lastMs: 0 }); }
function accCooldownKey(acc, name) { return lc(acc) + ':' + name; }
function isAccSrcCooling(acc, name) {
  const until = accSrcCooldown[accCooldownKey(acc, name)] || 0;
  return Date.now() < until;
}
function srcOk(name, n, ms, acc = '') {
  const s = stat(name);
  s.ok++; s.consec = 0; s.lastOkAt = Date.now(); s.lastItems = n; s.lastMs = ms; s.cooldownUntil = 0; s.lastErr = '';
  if (acc) accSrcCooldown[accCooldownKey(acc, name)] = 0;
}
function srcFail(name, e, acc = '') {
  const s = stat(name);
  s.fail++; s.consec++;
  s.lastErr = redact(String((e && e.message) || e)).slice(0, 140);
  const cd = Date.now() + Math.min(45000, 5000 * Math.pow(1.5, Math.min(s.consec - 1, 4)));
  s.cooldownUntil = cd;
  if (acc) accSrcCooldown[accCooldownKey(acc, name)] = cd;
}

// Виділяє унікальний глобальний ключ відео з URL або ID донора (щоб 2 акаунти не постили одне й те саме відео двічі)
function videoMediaKey(url, donorId = '') {
  if (donorId && isSnowflake(donorId)) return 'donor:' + donorId;
  const s = String(url || '');
  const m = s.match(/(?:amplify_video|ext_tw_video|tweet_video)\/(\d{10,22})/i) ||
            s.match(/\/([\w-]{12,64})\.mp4/i);
  return m ? ('vid:' + m[1]) : ('url:' + s.split('?')[0].slice(-80));
}

function hasGlobalMediaSeen(mKey) {
  if (!mKey) return false;
  if (Array.isArray(X.mediaSeen) && X.mediaSeen.includes(mKey)) return true;
  if (Array.isArray(X.queue) && X.queue.some(q => q && q.mediaKey === mKey)) return true;
  return false;
}

function markGlobalMediaSeen(mKey) {
  if (!mKey) return;
  if (!Array.isArray(X.mediaSeen)) X.mediaSeen = [];
  if (!X.mediaSeen.includes(mKey)) {
    X.mediaSeen.unshift(mKey);
    if (X.mediaSeen.length > 400) X.mediaSeen = X.mediaSeen.slice(0, 400);
  }
}

// Розумна перевірка на схожість тексту/матчу (щоб попереджати людину у відложці про можливий повтор гола з іншого ракурсу)
function tokenizeMatchText(text) {
  const clean = String(text || '')
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, ' ')
    .replace(/pic\.twitter\.com\/\S+/g, ' ')
    .replace(/[^\p{L}\p{N}\s-]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const stop = new Set(['goal', 'gol', 'golazo', 'what', 'the', 'and', 'for', 'from', 'with', 'assist', 'match', 'live', 'video', 'uploaded', 'by']);
  return uniq(clean.split(' ').filter(w => w.length >= 3 && !stop.has(w)));
}

function findSimilarRecentPost(text, currentAcc, currentId) {
  const wordsA = tokenizeMatchText(text);
  if (wordsA.length < 2) return null;
  const setA = new Set(wordsA);
  const pool = [
    ...(Array.isArray(X.queue) ? X.queue.map(q => ({ id: q.id, acc: q.account, text: q.text, at: q.createdAt, inQueue: true })) : []),
    ...xr.recentPostsMeta
  ];
  for (const item of pool) {
    if (!item || String(item.id) === String(currentId)) continue;
    if (Date.now() - (item.at || 0) > 6 * 3600 * 1000) continue;
    const wordsB = tokenizeMatchText(item.text);
    if (wordsB.length < 2) continue;
    let common = 0;
    for (const w of wordsB) if (setA.has(w)) common++;
    const ratio = common / Math.min(wordsA.length, wordsB.length);
    if (common >= 3 || (common >= 2 && ratio >= 0.65)) {
      return { acc: item.acc, id: item.id, inQueue: !!item.inQueue, text: String(item.text || '').slice(0, 80) };
    }
  }
  return null;
}

function rememberPostMeta(acc, id, text) {
  xr.recentPostsMeta.unshift({ acc, id, text: String(text || '').slice(0, 200), at: Date.now() });
  if (xr.recentPostsMeta.length > 150) xr.recentPostsMeta.length = 150;
}

// ---- вибір найкращого MP4 з відповідей різних сервісів ----
function pickFxVideo(v) {
  if (!v || typeof v !== 'object') return null;
  const rawFormats = Array.isArray(v.formats) ? v.formats : (Array.isArray(v.variants) ? v.variants : []);
  const fm = rawFormats.filter(f => f && f.url && (f.container === 'mp4' || /video\/mp4/i.test(String(f.content_type || f.type || '')) || /\.mp4(\?|$)/i.test(f.url)) && !/\.m3u8(\?|$)/i.test(f.url));
  const h264 = fm.filter(f => !f.codec || /h264|avc1/i.test(String(f.codec)));
  const pool = (h264.length ? h264 : fm).sort((a, b) => (Number(b.bitrate || b.bit_rate) || 0) - (Number(a.bitrate || a.bit_rate) || 0));
  const fastDirect = pool.find(f => (!f.size || f.size <= 19.5 * MB) && (!f.height || (f.height >= 480 && f.height <= 1080)));
  const good = fastDirect || pool.find(f => (!f.size || f.size <= 48 * MB) && (!f.height || f.height <= 1080));
  const best = good || pool[0] || pool[pool.length - 1];
  if (best) return { url: best.url, width: best.width || v.width, height: best.height || v.height, duration: v.duration || (v.duration_ms ? v.duration_ms / 1000 : 0) };
  if (v.url && !/\.m3u8(\?|$)/i.test(v.url)) return { url: v.url, width: v.width, height: v.height, duration: v.duration || (v.duration_ms ? v.duration_ms / 1000 : 0) };
  return null;
}

function extractFxTweetDirectVideo(o) {
  if (!o || typeof o !== 'object' || o.type === 'tombstone') return null;
  const m = o.media || {};
  if (Array.isArray(m.videos)) {
    for (const v of m.videos) {
      if (v && (v.type === 'video' || v.type === 'gif' || !v.type)) {
        const picked = pickFxVideo(v);
        if (picked) return picked;
      }
    }
  }
  if (Array.isArray(m.all)) {
    for (const item of m.all) {
      if (item && (item.type === 'video' || item.type === 'gif' || item.type === 'animated_gif')) {
        const picked = pickFxVideo(item);
        if (picked) return picked;
      }
    }
  }
  if (m.external && m.external.url && /\.mp4(\?|$)/i.test(m.external.url)) {
    return { url: m.external.url, width: m.external.width, height: m.external.height, duration: m.external.duration };
  }
  return null;
}

function videoFromFxTweet(t) {
  if (!t || typeof t !== 'object') return null;
  return extractFxTweetDirectVideo(t) ||
         extractFxTweetDirectVideo(t.quote) ||
         extractFxTweetDirectVideo(t.retweet) ||
         extractFxTweetDirectVideo(t.retweeted_status) ||
         extractFxTweetDirectVideo(t.quoted_status);
}

// Збирає вбудовані посилання на донорські твіти (/status/ID, /video/1) та t.co з об'єкта твіту
function extractTweetEmbeddedLinks(t, selfId = '') {
  const statusIds = [];
  const tcoUrls = [];
  const addStr = s => {
    const str = String(s || '');
    if (!str) return;
    for (const m of str.matchAll(/\/status(?:es)?\/(\d{15,21})/gi)) {
      if (isSnowflake(m[1]) && m[1] !== String(selfId) && !statusIds.includes(m[1])) statusIds.push(m[1]);
    }
    for (const m of str.matchAll(/https?:\/\/t\.co\/[A-Za-z0-9]{6,14}/gi)) {
      if (!tcoUrls.includes(m[0])) tcoUrls.push(m[0]);
    }
  };
  if (!t || typeof t !== 'object') return { statusIds, tcoUrls };
  addStr(t.text);
  addStr(t.full_text);
  if (t.raw_text && typeof t.raw_text === 'object') {
    addStr(t.raw_text.text);
    for (const f of (Array.isArray(t.raw_text.facets) ? t.raw_text.facets : [])) {
      addStr(f && (f.original || f.replacement || f.url));
    }
  }
  addStr(t.qrtURL);
  addStr(t.retweetURL);
  if (t.quote && isSnowflake(t.quote.id) && String(t.quote.id) !== String(selfId)) statusIds.push(String(t.quote.id));
  if (t.retweet && isSnowflake(t.retweet.id) && String(t.retweet.id) !== String(selfId)) statusIds.push(String(t.retweet.id));
  if (t.quoted_status && isSnowflake(t.quoted_status.id_str || t.quoted_status.id)) statusIds.push(String(t.quoted_status.id_str || t.quoted_status.id));
  if (t.retweeted_status && isSnowflake(t.retweeted_status.id_str || t.retweeted_status.id)) statusIds.push(String(t.retweeted_status.id_str || t.retweeted_status.id));
  if (t.entities && typeof t.entities === 'object') {
    for (const u of [].concat(t.entities.urls || [], t.entities.media || [])) {
      addStr(u && (u.expanded_url || u.url || u.display_url));
    }
  }
  return { statusIds: uniq(statusIds), tcoUrls: uniq(tcoUrls) };
}

function itemsFromFxV2(j, includeCandidates = true) {
  const out = [];
  const results = (j && Array.isArray(j.results)) ? j.results : ((j && j.data && Array.isArray(j.data)) ? j.data : []);
  for (const r of results) {
    if (!r || !isSnowflake(r.id)) continue;
    const id = String(r.id);
    const ts = r.created_timestamp ? r.created_timestamp * 1000 : snowTs(id);
    const text = String(r.text || (r.raw_text && r.raw_text.text) || '');
    const pv = videoFromFxTweet(r);
    if (pv && pv.url) {
      out.push({ id, text, videoUrl: pv.url, width: pv.width, height: pv.height, duration: pv.duration, ts });
      continue;
    }
    if (!includeCandidates) continue;
    const links = extractTweetEmbeddedLinks(r, id);
    const hasMediaHint = links.statusIds.length > 0 || links.tcoUrls.length > 0 ||
                         !!(r.quote || r.retweet || (r.media && (r.media.all || r.media.videos || r.media.external))) ||
                         /pic\.twitter\.com|\/video\/\d|https?:\/\/t\.co\//i.test(text);
    if (hasMediaHint) {
      out.push({ id, text, videoUrl: '', needsResolve: true, hintIds: links.statusIds, hintTcos: links.tcoUrls, ts });
    }
  }
  return out;
}

function pickVxVideo(v) {
  if (!v || typeof v !== 'object') return null;
  for (const m of (Array.isArray(v.media_extended) ? v.media_extended : [])) {
    if (m && m.url && (m.type === 'video' || m.type === 'gif' || /\.mp4|video\.twimg\.com/i.test(m.url)) && !/\.m3u8(\?|$)/i.test(m.url)) {
      return { url: m.url, width: m.size && m.size.width, height: m.size && m.size.height, duration: m.duration_millis ? m.duration_millis / 1000 : 0 };
    }
  }
  for (const u of (Array.isArray(v.mediaURLs) ? v.mediaURLs : [])) {
    if (/\.mp4|video\.twimg\.com/i.test(String(u)) && !/\.m3u8/i.test(String(u))) return { url: String(u) };
  }
  return null;
}

function pickSynVideo(t) {
  if (!t || typeof t !== 'object') return null;
  const media = [].concat((t.extended_entities && t.extended_entities.media) || [], t.mediaDetails || []);
  for (const md of media) {
    if (md && (md.type === 'video' || md.type === 'animated_gif') && md.video_info && Array.isArray(md.video_info.variants)) {
      const mp4s = md.video_info.variants
        .filter(v => v && v.url && (/video\/mp4/i.test(String(v.content_type || '')) || /\.mp4/i.test(v.url)) && !/\.m3u8/i.test(v.url))
        .sort((a, b) => (Number(b.bitrate) || 0) - (Number(a.bitrate) || 0));
      if (mp4s.length) {
        return {
          url: mp4s[0].url,
          width: md.original_info && md.original_info.width,
          height: md.original_info && md.original_info.height,
          duration: md.video_info.duration_millis ? md.video_info.duration_millis / 1000 : 0
        };
      }
    }
  }
  if (t.video && Array.isArray(t.video.variants)) {
    const mp4s = t.video.variants
      .filter(v => v && (v.src || v.url) && (/video\/mp4/i.test(String(v.type || v.content_type || '')) || /\.mp4/i.test(String(v.src || v.url))) && !/\.m3u8/i.test(String(v.src || v.url)));
    if (mp4s.length) {
      const best = mp4s[mp4s.length - 1];
      return { url: best.src || best.url };
    }
  }
  return null;
}

// Розкриття коротких посилань https://t.co/... до ID донорського твіту або прямого відео
const tcoCache = new Map();
async function resolveTcoLink(tcoUrl) {
  const cleanUrl = String(tcoUrl || '').trim();
  if (!/^https?:\/\/t\.co\/[A-Za-z0-9]+/i.test(cleanUrl)) return { statusId: '', url: '' };
  if (tcoCache.has(cleanUrl)) return tcoCache.get(cleanUrl);
  try {
    const r = await fetchRaw(cleanUrl, {
      redirect: 'manual',
      headers: { 'User-Agent': FX_UA, Accept: 'text/html,*/*' }
    }, 4500);
    const loc = String(r.headers.get('location') || '');
    let target = loc;
    if (!target) {
      const txt = await r.text().catch(() => '');
      const mUrl = txt.match(/https?:\/\/[^\s"'<>]+/i);
      if (mUrl) target = mUrl[0];
    }
    const mStatus = target.match(/\/status(?:es)?\/(\d{15,21})/i);
    const out = {
      statusId: (mStatus && isSnowflake(mStatus[1])) ? mStatus[1] : '',
      url: target
    };
    if (out.statusId || out.url) {
      tcoCache.set(cleanUrl, out);
      if (tcoCache.size > 500) tcoCache.delete(tcoCache.keys().next().value);
    }
    return out;
  } catch (e) {
    return { statusId: '', url: '' };
  }
}

// ---- Глибоке розкриття відео з твіту (включно з Quote, Retweet, донорськими /video/1 та t.co) ----
// ВАЖЛИВО: запитуємо як /${user}/status/${sid}, так і /i/status/${sid}, щоб ретвіти та чужі ID (як у 2-го акаунта) ЗАВЖДИ розкривались!
async function resolveTweetVideo(user, id, depth = 0, hints = {}) {
  const sid = String(id || '').trim();
  if (!isSnowflake(sid)) return null;

  const cached = xr.resolvedCache.get(sid);
  if (cached && cached.final && Date.now() - cached.at < 30 * 60 * 1000) {
    return cached.result;
  }

  const u = parseXUsername(user) || 'i';
  let text = String(hints.text || '');
  const donorIds = [...(Array.isArray(hints.hintIds) ? hints.hintIds : [])];
  const tcoList = [...(Array.isArray(hints.hintTcos) ? hints.hintTcos : [])];

  const collectFromObj = obj => {
    const extracted = extractTweetEmbeddedLinks(obj, sid);
    for (const dId of extracted.statusIds) if (!donorIds.includes(dId)) donorIds.push(dId);
    for (const tc of extracted.tcoUrls) if (!tcoList.includes(tc)) tcoList.push(tc);
  };

  if (text) collectFromObj({ text });

  // 1) Паралельний запит до FxTwitter та FixupX (використовуємо '/i/status/' — працює для будь-якого автора чи ретвіту!)
  const fxPromises = [
    'https://api.fxtwitter.com/i/status/' + sid,
    'https://api.fixupx.com/i/status/' + sid,
    ...(u !== 'i' ? ['https://api.fxtwitter.com/' + encodeURIComponent(u) + '/status/' + sid] : [])
  ].map(async url => {
    const r = await fetchJson(url, {
      headers: { 'User-Agent': FX_UA, Accept: 'application/json', 'Cache-Control': 'no-cache' }
    }, 6500);
    const t = r.json && r.json.tweet;
    if (!t) throw new Error('no-tweet');
    return t;
  });

  try {
    const fxSettled = await Promise.allSettled(fxPromises);
    for (const s of fxSettled) {
      if (s.status !== 'fulfilled' || !s.value) continue;
      const t = s.value;
      if (!text && t.text) text = String(t.text);
      collectFromObj(t);
      const pv = videoFromFxTweet(t);
      if (pv && pv.url) {
        const res = {
          videoUrl: pv.url, width: pv.width, height: pv.height, duration: pv.duration,
          text: text || String((t.quote && t.quote.text) || (t.retweet && t.retweet.text) || ''),
          author: (t.author && t.author.screen_name) || u,
          donorId: (t.quote && t.quote.id) || (t.retweet && t.retweet.id) || ''
        };
        xr.resolvedCache.set(sid, { result: res, at: Date.now(), final: true });
        return res;
      }
    }
  } catch (e) {}

  // 2) Паралельний запит до VxTwitter, FixVX та Twitter CDN Syndication
  const secondaryPromises = [
    ...['https://api.vxtwitter.com/i/status/' + sid, 'https://api.fixvx.com/i/status/' + sid].map(async url => {
      const r = await fetchJson(url, {
        headers: { 'User-Agent': FX_UA, Accept: 'application/json', 'Cache-Control': 'no-cache' }
      }, 6500);
      const j = r.json;
      if (!j) throw new Error('no-vx');
      if (!text && j.text) text = String(j.text);
      collectFromObj(j);
      const pv = pickVxVideo(j) || pickVxVideo(j.qrt) || pickVxVideo(j.retweet);
      if (pv && pv.url) {
        return { videoUrl: pv.url, width: pv.width, height: pv.height, duration: pv.duration, text, author: j.user_screen_name || u };
      }
      throw new Error('no-vx-video');
    }),
    (async () => {
      const token = ((Number(sid) / 1e15) * Math.PI).toString(36).replace(/(0+|\.)/g, '');
      const r = await fetchJson('https://cdn.syndication.twimg.com/tweet-result?id=' + sid + '&token=' + token + '&lang=en', {
        headers: { 'User-Agent': BROWSER_UA, Accept: 'application/json', 'Cache-Control': 'no-cache' }
      }, 6500);
      const j = r.json;
      if (!j) throw new Error('no-syn');
      if (!text && (j.text || j.full_text)) text = String(j.text || j.full_text);
      collectFromObj(j);
      const pv = pickSynVideo(j) || pickSynVideo(j.quoted_tweet) || pickSynVideo(j.parent) || pickSynVideo(j.retweeted_status);
      if (pv && pv.url) {
        return { videoUrl: pv.url, width: pv.width, height: pv.height, duration: pv.duration, text, author: (j.user && j.user.screen_name) || u };
      }
      throw new Error('no-syn-video');
    })()
  ];

  try {
    const secSettled = await Promise.allSettled(secondaryPromises);
    for (const s of secSettled) {
      if (s.status === 'fulfilled' && s.value && s.value.videoUrl) {
        xr.resolvedCache.set(sid, { result: s.value, at: Date.now(), final: true });
        return s.value;
      }
    }
  } catch (e) {}

  // 3) Розкриття вбудованих донорських твітів (/status/ID/video/1 або https://t.co/...) — рекурсія до глибини 2
  if (depth < 2) {
    if (tcoList.length > 0) {
      const resolvedTcos = await Promise.all(uniq(tcoList).slice(0, 5).map(resolveTcoLink));
      for (const rt of resolvedTcos) {
        if (rt.statusId && rt.statusId !== sid && !donorIds.includes(rt.statusId)) {
          donorIds.push(rt.statusId);
        } else if (rt.url && /\.mp4(\?|$)/i.test(rt.url) && !/\.m3u8/i.test(rt.url)) {
          const res = { videoUrl: rt.url, text, author: u };
          xr.resolvedCache.set(sid, { result: res, at: Date.now(), final: true });
          return res;
        }
      }
    }
    for (const dId of uniq(donorIds).slice(0, 4)) {
      if (dId === sid) continue;
      const sub = await resolveTweetVideo('i', dId, depth + 1);
      if (sub && sub.videoUrl) {
        const res = {
          videoUrl: sub.videoUrl,
          width: sub.width,
          height: sub.height,
          duration: sub.duration,
          text: text || sub.text,
          author: u,
          donorId: dId
        };
        xr.resolvedCache.set(sid, { result: res, at: Date.now(), final: true });
        return res;
      }
    }
  }

  // 4) Резерв через внутрішній Cobalt
  if (depth === 0 && cobaltReady) {
    try {
      const cob = await cobaltResolve({
        sourceUrl: 'https://x.com/' + encodeURIComponent(u) + '/status/' + sid,
        platform: 'twitter',
        quality: 1080,
        audioOnly: false
      });
      if (cob && cob.url && !/\.m3u8(\?|$)/i.test(cob.url)) {
        const res = { videoUrl: cob.url, text, author: u };
        xr.resolvedCache.set(sid, { result: res, at: Date.now(), final: true });
        return res;
      }
    } catch (e) {}
  }

  const ageMs = Date.now() - snowTs(sid);
  if (ageMs > 90 * 1000) {
    xr.resolvedCache.set(sid, { result: null, at: Date.now(), final: true });
  }
  if (xr.resolvedCache.size > 600) xr.resolvedCache.delete(xr.resolvedCache.keys().next().value);
  return null;
}

// ---- 6 НЕЗАЛЕЖНИХ ДЖЕРЕЛ СТРІЧКИ ПОСТІВ АКАУНТА ----

// 1. FxTwitter / FixupX Statuses (хронологічна стрічка всіх постів, ретвітів та цитат)
async function srcFxStatuses(acc) {
  const errs = [];
  for (const base of ['https://api.fxtwitter.com', 'https://api.fixupx.com']) {
    try {
      const r = await fetchJson(base + '/2/profile/' + encodeURIComponent(acc) + '/statuses?count=40', {
        headers: { 'User-Agent': FX_UA, Accept: 'application/json', 'Cache-Control': 'no-cache', Pragma: 'no-cache' }
      }, 8500);
      if (!r.ok || !r.json) { errs.push(base.replace(/^https?:\/\//, '') + ':' + r.status); continue; }
      const items = itemsFromFxV2(r.json, true);
      if (items.length) return { items };
      errs.push(base.replace(/^https?:\/\//, '') + ':0');
    } catch (e) { errs.push(base.replace(/^https?:\/\//, '') + ':' + String(e.message).slice(0, 25)); }
  }
  throw new Error('fx-statuses: ' + errs.join(','));
}

// 2. FxTwitter / FixupX Media (медіа-вкладка профілю)
async function srcFxMedia(acc) {
  const errs = [];
  for (const base of ['https://api.fxtwitter.com', 'https://api.fixupx.com']) {
    try {
      const r = await fetchJson(base + '/2/profile/' + encodeURIComponent(acc) + '/media?count=40', {
        headers: { 'User-Agent': FX_UA, Accept: 'application/json', 'Cache-Control': 'no-cache', Pragma: 'no-cache' }
      }, 8500);
      if (!r.ok || !r.json) { errs.push(base.replace(/^https?:\/\//, '') + ':' + r.status); continue; }
      const items = itemsFromFxV2(r.json, true);
      if (items.length) return { items };
      errs.push(base.replace(/^https?:\/\//, '') + ':0');
    } catch (e) { errs.push(base.replace(/^https?:\/\//, '') + ':' + String(e.message).slice(0, 25)); }
  }
  throw new Error('fx-media: ' + errs.join(','));
}

// 3. Офіційний X API v2 (якщо заданий X_BEARER_TOKEN)
const xApiUserIds = {};
async function srcXApi(acc) {
  if (!X_BEARER_TOKEN) throw new Error('N/A');
  const H = { Authorization: 'Bearer ' + X_BEARER_TOKEN, 'User-Agent': FX_UA };
  let uid = xApiUserIds[lc(acc)];
  if (!uid) {
    const r = await fetchJson('https://api.twitter.com/2/users/by/username/' + encodeURIComponent(acc), { headers: H }, 9000);
    uid = r.json && r.json.data && r.json.data.id;
    if (!uid) throw new Error('x-api user HTTP ' + r.status);
    xApiUserIds[lc(acc)] = uid;
  }
  const r2 = await fetchJson('https://api.twitter.com/2/users/' + uid + '/tweets?max_results=25&exclude=replies&expansions=attachments.media_keys,referenced_tweets.id&media.fields=type,variants,duration_ms,width,height&tweet.fields=created_at,text,entities', { headers: H }, 10000);
  if (!r2.ok || !r2.json) throw new Error('x-api tweets HTTP ' + r2.status);
  const media = {};
  for (const m of ((r2.json.includes && r2.json.includes.media) || [])) media[m.media_key] = m;
  const items = [];
  for (const t of (r2.json.data || [])) {
    const id = String(t.id);
    const text = String(t.text || '');
    const keys = (t.attachments && t.attachments.media_keys) || [];
    let added = false;
    for (const k of keys) {
      const m = media[k];
      if (!m || !(m.type === 'video' || m.type === 'animated_gif')) continue;
      const mp4 = (m.variants || []).filter(v => /video\/mp4/i.test(v.content_type || '')).sort((a, b) => (b.bit_rate || 0) - (a.bit_rate || 0))[0];
      if (mp4) {
        items.push({ id, text, videoUrl: mp4.url, width: m.width, height: m.height, duration: (m.duration_ms || 0) / 1000, ts: snowTs(id) });
        added = true;
        break;
      }
    }
    if (!added) {
      const links = extractTweetEmbeddedLinks(t, id);
      if (links.statusIds.length || links.tcoUrls.length || (t.referenced_tweets && t.referenced_tweets.length)) {
        items.push({ id, text, videoUrl: '', needsResolve: true, hintIds: links.statusIds, hintTcos: links.tcoUrls, ts: snowTs(id) });
      }
    }
  }
  return { items };
}

// 4. Twitter Syndication (підтримує як JSON timeline, так і прямий парсинг свіжих entry_id)
async function srcSyndication(acc) {
  const maxAge = Math.max(X_MAX_AGE_MS || 0, 36 * 3600 * 1000);
  for (const showReplies of ['false', 'true']) {
    const r = await fetchJson('https://syndication.twitter.com/srv/timeline-profile/screen-name/' + encodeURIComponent(acc) + '?dnt=true&showReplies=' + showReplies, {
      headers: { 'User-Agent': BROWSER_UA, Accept: 'text/html,*/*', 'Cache-Control': 'no-cache' }
    }, 8500);
    if (!r.ok) continue;
    const items = [];
    const seenIds = new Set();
    const m = r.text.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
    if (m) {
      try {
        const j = JSON.parse(m[1]);
        const entries = (((j.props || {}).pageProps || {}).timeline || {}).entries || [];
        for (const e of entries) {
          const t = e && e.content && e.content.tweet;
          if (!t) continue;
          const id = String(t.id_str || t.id || '');
          if (!isSnowflake(id) || seenIds.has(id)) continue;
          const ts = snowTs(id);
          if (Date.now() - ts > maxAge) continue;
          seenIds.add(id);
          const text = String(t.full_text || t.text || '');
          const pv = pickSynVideo(t) || pickSynVideo(t.retweeted_status) || pickSynVideo(t.quoted_status);
          if (pv && pv.url) {
            items.push({ id, text, videoUrl: pv.url, width: pv.width, height: pv.height, duration: pv.duration, ts });
          } else {
            const links = extractTweetEmbeddedLinks(t, id);
            if (links.statusIds.length || links.tcoUrls.length || /pic\.twitter\.com|\/video\/|https?:\/\/t\.co\//i.test(text)) {
              items.push({ id, text, videoUrl: '', needsResolve: true, hintIds: links.statusIds, hintTcos: links.tcoUrls, ts });
            }
          }
        }
      } catch (e) {}
    }
    for (const hm of r.text.matchAll(/"entry_id"\s*:\s*"tweet-(\d{15,21})"/g)) {
      const id = hm[1];
      if (isSnowflake(id) && !seenIds.has(id) && (Date.now() - snowTs(id) <= maxAge)) {
        seenIds.add(id);
        items.push({ id, text: '', videoUrl: '', needsResolve: true, ts: snowTs(id) });
      }
    }
    if (items.length) return { items };
  }
  throw new Error('syndication: немає свіжих постів');
}

// 5. Nitter / XCancel (розширений пул інстансів: RSS + HTML + /media)
const NITTER_STATIC = [
  'https://xcancel.com',
  'https://nitter.poast.org',
  'https://nitter.privacyredirect.com',
  'https://nitter.tiekoetter.com',
  'https://nitter.space',
  'https://lightbrd.com',
  'https://nitter.lucabased.xyz',
  'https://nitter.jaydenha.uk',
  'https://nitter.net'
];
const nitterCache = { at: 0, hosts: [] };
async function nitterHosts() {
  if (Date.now() - nitterCache.at < 20 * 60 * 1000 && nitterCache.hosts.length) return nitterCache.hosts;
  let live = [];
  try {
    const r = await fetchJson('https://status.d420.de/api/v1/instances', { headers: { 'User-Agent': BROWSER_UA, Accept: 'application/json' } }, 4500);
    const arr = Array.isArray(r.json) ? r.json : ((r.json && (r.json.hosts || r.json.instances)) || []);
    for (const h of arr) {
      if (!h || typeof h !== 'object') continue;
      if (h.healthy === false || h.is_healthy === false || h.is_bad_host === true) continue;
      const u = h.url || (h.domain && ('https://' + h.domain)) || (h.host && ('https://' + h.host));
      if (u && /^https?:\/\//i.test(u)) live.push(String(u).replace(/\/+$/, ''));
    }
  } catch (e) {}
  nitterCache.hosts = uniq([...NITTER_STATIC, ...live.slice(0, 6)]).slice(0, 12);
  nitterCache.at = Date.now();
  return nitterCache.hosts;
}

function parseNitterRssItems(xml) {
  const items = [];
  const seenIds = new Set();
  const blocks = String(xml || '').match(/<item>[\s\S]*?<\/item>/gi) || [];
  for (const blk of blocks) {
    const linkMatch = blk.match(/<link>([^<]+)<\/link>/i) || blk.match(/<guid[^>]*>([^<]+)<\/guid>/i);
    const idMatch = (linkMatch && linkMatch[1].match(/\/status(?:es)?\/(\d{15,21})/i)) || blk.match(/\/status(?:es)?\/(\d{15,21})/i);
    if (!idMatch || !isSnowflake(idMatch[1])) continue;
    const id = idMatch[1];
    if (seenIds.has(id)) continue;
    seenIds.add(id);

    const descMatch = blk.match(/<description>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/description>/i);
    const titleMatch = blk.match(/<title>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/title>/i);
    const rawDesc = (descMatch ? descMatch[1] : '') || (titleMatch ? titleMatch[1] : '');
    const cleanText = stripHtml(rawDesc).replace(/^RT by @[A-Za-z0-9_]+:\s*/i, '').replace(/^R to @[A-Za-z0-9_]+:\s*/i, '').trim();

    const hintIds = [];
    for (const m of blk.matchAll(/\/status(?:es)?\/(\d{15,21})/gi)) {
      if (isSnowflake(m[1]) && m[1] !== id && !hintIds.includes(m[1])) hintIds.push(m[1]);
    }
    const hintTcos = [];
    for (const m of blk.matchAll(/https?:\/\/t\.co\/[A-Za-z0-9]{6,14}/gi)) {
      if (!hintTcos.includes(m[0])) hintTcos.push(m[0]);
    }
    items.push({ id, text: cleanText, videoUrl: '', needsResolve: true, hintIds, hintTcos, ts: snowTs(id) });
  }
  if (!items.length) {
    for (const m of String(xml || '').matchAll(/\/status(?:es)?\/(\d{15,21})/g)) {
      if (isSnowflake(m[1]) && !seenIds.has(m[1])) {
        seenIds.add(m[1]);
        items.push({ id: m[1], text: '', videoUrl: '', needsResolve: true, ts: snowTs(m[1]) });
      }
    }
  }
  return items;
}

function parseNitterHtmlItems(html) {
  const items = [];
  const seenIds = new Set();
  const str = String(html || '');
  // Розбиваємо по timeline-item, якщо є, щоб витягнути і основний ID, і донорські посилання всередині поста
  const chunks = str.split(/class="timeline-item/i);
  if (chunks.length > 1) {
    for (let i = 1; i < chunks.length; i++) {
      const ch = chunks[i];
      const allIds = [...ch.matchAll(/\/status(?:es)?\/(\d{15,21})/gi)].map(m => m[1]).filter(isSnowflake);
      if (!allIds.length) continue;
      const id = allIds[0];
      if (seenIds.has(id)) continue;
      seenIds.add(id);
      const hintIds = uniq(allIds.slice(1).filter(x => x !== id));
      const hintTcos = uniq([...ch.matchAll(/https?:\/\/t\.co\/[A-Za-z0-9]{6,14}/gi)].map(m => m[0]));
      const contentMatch = ch.match(/class="tweet-content[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
      const text = contentMatch ? stripHtml(contentMatch[1]).trim() : '';
      items.push({ id, text, videoUrl: '', needsResolve: true, hintIds, hintTcos, ts: snowTs(id) });
    }
  }
  if (!items.length) {
    for (const m of str.matchAll(/\/status(?:es)?\/(\d{15,21})/gi)) {
      if (isSnowflake(m[1]) && !seenIds.has(m[1])) {
        seenIds.add(m[1]);
        items.push({ id: m[1], text: '', videoUrl: '', needsResolve: true, ts: snowTs(m[1]) });
      }
    }
  }
  return items;
}

async function srcNitter(acc) {
  const hosts = await nitterHosts();
  const tryHost = async host => {
    // 1) RSS
    try {
      const r = await fetchJson(host + '/' + encodeURIComponent(acc) + '/rss', {
        headers: { 'User-Agent': BROWSER_UA, Accept: 'application/rss+xml,text/xml,text/html,*/*', 'Cache-Control': 'no-cache' }
      }, 6000);
      if (r.ok && /<item>/i.test(r.text)) {
        const items = parseNitterRssItems(r.text);
        if (items.length) return { items, host };
      }
    } catch (e) {}
    // 2) HTML профіль
    for (const suffix of ['', '/media']) {
      try {
        const rHtml = await fetchJson(host + '/' + encodeURIComponent(acc) + suffix, {
          headers: { 'User-Agent': BROWSER_UA, Accept: 'text/html,*/*', 'Cache-Control': 'no-cache' }
        }, 6500);
        if (rHtml.ok && /timeline-item|\/status\//i.test(rHtml.text)) {
          const items = parseNitterHtmlItems(rHtml.text);
          if (items.length) return { items, host };
        }
      } catch (e) {}
    }
    throw new Error(host.replace(/^https?:\/\//, '') + ':fail');
  };

  for (let i = 0; i < hosts.length; i += 3) {
    const batch = hosts.slice(i, i + 3);
    try {
      return await Promise.any(batch.map(tryHost));
    } catch (e) {}
  }
  throw new Error('nitter: інстанси недоступні');
}

// 6. Jina Web Reader Mirror (обходить Cloudflare для x.com та xcancel.com — надзвичайно надійний резерв для будь-якого 2-го/3-го акаунта!)
async function srcJinaMirror(acc) {
  const urls = [
    'https://r.jina.ai/https://xcancel.com/' + encodeURIComponent(acc),
    'https://r.jina.ai/https://x.com/' + encodeURIComponent(acc)
  ];
  for (const u of urls) {
    try {
      const r = await fetchJson(u, {
        headers: { 'User-Agent': BROWSER_UA, Accept: 'text/plain,text/html,*/*', 'X-No-Cache': 'true' }
      }, 9000);
      if (!r.ok || !r.text) continue;
      const seenIds = new Set();
      const items = [];
      const maxAge = Math.max(X_MAX_AGE_MS || 0, 48 * 3600 * 1000);
      for (const m of r.text.matchAll(/\/status(?:es)?\/(\d{15,21})/gi)) {
        const id = m[1];
        if (!isSnowflake(id) || seenIds.has(id)) continue;
        if (Date.now() - snowTs(id) > maxAge) continue;
        seenIds.add(id);
        items.push({ id, text: '', videoUrl: '', needsResolve: true, ts: snowTs(id) });
      }
      if (items.length) return { items };
    } catch (e) {}
  }
  throw new Error('jina: немає постів');
}

const X_SOURCES = [
  ['fxStatuses', srcFxStatuses],
  ['fxMedia', srcFxMedia],
  ['nitter', srcNitter],
  ['syndication', srcSyndication],
  ['jina', srcJinaMirror],
  ['xapi', srcXApi]
];

// Паралельне агрегування постів з усіх джерел для конкретного акаунта (з індивідуальними кулдаунами на кожен акаунт!)
async function fetchXPosts(acc, { forceAll = false } = {}) {
  const activeSources = X_SOURCES.filter(([name]) => {
    if (name === 'xapi' && !X_BEARER_TOKEN) return false;
    if (forceAll) return true;
    return !isAccSrcCooling(acc, name);
  });

  const toRun = activeSources.length ? activeSources : X_SOURCES.filter(([n]) => n !== 'xapi');

  const settled = await Promise.allSettled(toRun.map(async ([name, fn]) => {
    const t0 = Date.now();
    try {
      const r = await withTimeout(fn(acc), 11000, name);
      const list = r.items || [];
      srcOk(name, list.length, Date.now() - t0, acc);
      return { name, items: list, ms: Date.now() - t0 };
    } catch (e) {
      srcFail(name, e, acc);
      throw new Error(name + ':' + String(e.message).slice(0, 45));
    }
  }));

  const mergedMap = new Map();
  const okSources = [];
  const perSourceCounts = {};
  const reasons = [];

  for (const s of settled) {
    if (s.status === 'fulfilled' && s.value) {
      perSourceCounts[s.value.name] = { ok: true, count: s.value.items.length, ms: s.value.ms };
      if (s.value.items.length > 0) okSources.push(s.value.name);
      for (const it of s.value.items) {
        if (!it || !isSnowflake(it.id)) continue;
        const prev = mergedMap.get(it.id);
        if (!prev) {
          mergedMap.set(it.id, { ...it });
        } else {
          mergedMap.set(it.id, {
            ...prev,
            ...it,
            text: prev.text || it.text || '',
            videoUrl: prev.videoUrl || it.videoUrl || '',
            width: prev.width || it.width,
            height: prev.height || it.height,
            duration: prev.duration || it.duration,
            needsResolve: !(prev.videoUrl || it.videoUrl),
            hintIds: uniq([...(prev.hintIds || []), ...(it.hintIds || [])]),
            hintTcos: uniq([...(prev.hintTcos || []), ...(it.hintTcos || [])])
          });
        }
      }
    } else if (s.status === 'rejected') {
      const msg = String(s.reason && s.reason.message || s.reason);
      const srcName = msg.split(':')[0] || 'src';
      perSourceCounts[srcName] = { ok: false, err: msg };
      reasons.push(msg);
    }
  }

  const items = [...mergedMap.values()].sort((a, b) => cmpId(b.id, a.id));
  if (!items.length && !okSources.length && reasons.length) {
    throw new Error('ALL_SOURCES_FAILED ' + reasons.join(' | '));
  }

  return { items, source: okSources.join('+') || 'empty', perSourceCounts };
}

// ---- відправка в канали (коректна робота з 1-м, 2-м і будь-якою кількістю каналів!) ----
function xCaption(text) {
  const clean = String(text || '')
    .replace(/https?:\/\/\S+/g, '')
    .replace(/pic\.twitter\.com\/\S+/gi, '')
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
    .slice(0, 850);
  return clean ? (clean + '\n\n' + X.credit) : X.credit;
}

async function sendXVideoToChat(token, chat, item, caption, cache) {
  const base = { chat_id: chat };
  // Якщо вже є кешований cloud fileId для цього токена — пробуємо миттєву відправку
  if (cache.cloudFileId && cache.token === token) {
    try {
      return await tg(token, 'sendVideo', { ...base, video: cache.cloudFileId, caption, supports_streaming: true }, { timeout: 25000, noRetry: true });
    } catch (e) {
      if (/chat not found|not enough rights|have no rights|bot was kicked|not a member|forbidden|CHAT_WRITE_FORBIDDEN|CHAT_ADMIN_REQUIRED/i.test(e.message)) throw e;
    }
  }
  // Спроба №1: пряма відправка URL через Telegram Cloud API
  try {
    const res = await tg(token, 'sendVideo', {
      ...base, video: item.videoUrl, caption, supports_streaming: true,
      ...(item.width ? { width: item.width } : {}), ...(item.height ? { height: item.height } : {}), ...(item.duration ? { duration: Math.round(item.duration) } : {})
    }, { timeout: 35000, noRetry: true });
    if (res && res.video && res.video.file_id) {
      cache.cloudFileId = res.video.file_id;
      cache.token = token;
    }
    return res;
  } catch (e) {
    if (/chat not found|not enough rights|have no rights|bot was kicked|not a member|forbidden|CHAT_WRITE_FORBIDDEN|CHAT_ADMIN_REQUIRED/i.test(e.message)) throw e;
  }
  // Спроба №2: скачуємо файл на диск один раз і роздаємо в усі канали (і 1-й, і 2-й, і наступні!)
  if (!cache.file || !fileSize(cache.file)) {
    cache.file = tmpName('.mp4');
    await downloadToFile(item.videoUrl, cache.file, { referer: 'https://x.com/' });
    cache.info = await probe(cache.file).catch(() => ({}));
    const norm = await normalizeForTelegram(cache.file, cache.info, { allowTranscode: false });
    if (norm.file !== cache.file) { safeUnlink(cache.file); cache.file = norm.file; cache.info = await probe(cache.file).catch(() => cache.info); }
  }
  const res = await sendFileToTelegram({
    token, chatId: chat, mode: 'video', file: cache.file, caption: esc(caption),
    meta: { width: cache.info.width || item.width, height: cache.info.height || item.height, duration: cache.info.duration || item.duration }
  });
  if (res && res.video && res.video.file_id && !localApiReady) {
    cache.cloudFileId = res.video.file_id;
    cache.token = token;
  }
  return res;
}

async function postToTargets(item, targets, tokenOverride = '') {
  const token = tokenOverride || X.token || Store.token();
  const targetList = uniq((Array.isArray(targets) && targets.length ? targets : allTargets()).map(parseTelegramChat).filter(Boolean));
  const caption = xCaption(item.text);
  const cache = { cloudFileId: item.cachedFileId && item.cachedToken === token ? item.cachedFileId : '', token };
  const sent = []; const failed = [];
  try {
    for (const chat of targetList) {
      try {
        await sendXVideoToChat(token, chat, item, caption, cache);
        sent.push(chat);
        xr.targetStatus[chat] = { ...(xr.targetStatus[chat] || {}), lastPostAt: Date.now(), lastErr: '', ok: true };
      } catch (e) {
        failed.push({ chat, err: redact(e.message).slice(0, 140) });
        xr.targetStatus[chat] = { ...(xr.targetStatus[chat] || {}), lastErr: redact(e.message).slice(0, 140), ok: false };
        log('xwatch', 'не вдалось надіслати в', chat, redact(e.message).slice(0, 120));
      }
    }
  } finally { if (cache.file) safeUnlink(cache.file); }
  if (sent.length) {
    X.posted += 1;
    rememberPostMeta(item.account || '', item.id || '', item.text || '');
    Store.schedule();
  }
  return { sent, failed };
}

// ==================== СИСТЕМА ВІДЛОЖКИ НА 1 ДЕНЬ (24 ГОД) ДЛЯ ПЕРЕВІРКИ ПОВТОРІВ ====================
function formatHoldTime(ts) {
  try {
    return new Date(ts).toISOString().slice(0, 16).replace('T', ' ') + ' UTC';
  } catch (e) { return '—'; }
}

function buildHoldPreviewCaption(qItem) {
  const hours = Math.max(1, Math.round((qItem.publishAt - qItem.createdAt) / 3600000));
  const remMin = Math.max(0, Math.round((qItem.publishAt - Date.now()) / 60000));
  const remLabel = remMin >= 60 ? (Math.floor(remMin / 60) + ' ч ' + (remMin % 60) + ' мин') : (remMin + ' мин');
  const header = [
    '📅 <b>В ОТЛОЖКЕ НА ' + hours + ' Ч (проверка на повторы)</b>',
    '⏰ Автовыход в канал: через <b>' + remLabel + '</b> (<code>' + formatHoldTime(qItem.publishAt) + '</code>)',
    '📡 Источник: <b>@' + esc(qItem.account) + '</b> → <code>' + esc((qItem.targets || allTargets()).join(', ')) + '</code>',
    ...(qItem.dupWarning ? ['⚠️ <b>' + esc(qItem.dupWarning) + '</b>'] : []),
    '──────────────',
    esc(xCaption(qItem.text))
  ];
  return header.join('\n').slice(0, 1020);
}

function buildHoldPreviewKb(qid) {
  return {
    inline_keyboard: [
      [
        { text: '✅ Выложить сейчас', callback_data: 'xw:qpub:' + qid },
        { text: '🗑 Удалить (повтор)', callback_data: 'xw:qdel:' + qid }
      ],
      [
        { text: '⏳ Через 1 ч', callback_data: 'xw:q1h:' + qid },
        { text: '📋 Вся отложка', callback_data: 'xw:queue' }
      ]
    ]
  };
}

// Додає нове відео у відложку на 1 день (24 год) і надсилає прев'ю-картку адміну з кнопками перевірки
async function enqueueHoldVideo(acc, item, targets, dupSim = null) {
  if (!Array.isArray(X.queue)) X.queue = [];
  const holdMs = Math.max(1, Number(X.holdHours) || 24) * 3600 * 1000;
  const now = Date.now();
  const qid = Date.now().toString(36).slice(-4) + crypto.randomBytes(2).toString('hex');
  const mKey = videoMediaKey(item.videoUrl, item.donorId);
  const dupWarning = dupSim ? ('Возможный повтор! Похожий пост уже был от @' + dupSim.acc + (dupSim.inQueue ? ' (в отложке)' : '')) : '';

  const qItem = {
    qid,
    account: acc,
    id: String(item.id),
    videoUrl: item.videoUrl,
    width: item.width,
    height: item.height,
    duration: item.duration,
    text: item.text || '',
    mediaKey: mKey,
    createdAt: now,
    publishAt: now + holdMs,
    targets: uniq((targets && targets.length ? targets : allTargets()).map(parseTelegramChat).filter(Boolean)),
    dupWarning,
    adminChatId: String(runtime.adminChat || ADMIN_CHAT_ID),
    adminMsgId: 0,
    adminToken: '',
    cachedFileId: ''
  };

  // Надсилаємо прев'ю відео в адмін-чат через основного бота (щоб інлайн-кнопки працювали миттєво через webhook!)
  const previewTok = Store.token() || X.token;
  if (previewTok && qItem.adminChatId) {
    const cache = {};
    try {
      const cap = buildHoldPreviewCaption(qItem);
      const kb = buildHoldPreviewKb(qid);
      let sentMsg = null;
      try {
        sentMsg = await tg(previewTok, 'sendVideo', {
          chat_id: qItem.adminChatId,
          video: qItem.videoUrl,
          caption: cap,
          parse_mode: 'HTML',
          supports_streaming: true,
          reply_markup: kb,
          ...(qItem.width ? { width: qItem.width } : {}),
          ...(qItem.height ? { height: qItem.height } : {}),
          ...(qItem.duration ? { duration: Math.round(qItem.duration) } : {})
        }, { timeout: 30000, noRetry: true });
      } catch (e) {
        // Якщо за URL не пішло — качаємо файл і відправляємо з кнопками
        cache.file = tmpName('.mp4');
        await downloadToFile(qItem.videoUrl, cache.file, { referer: 'https://x.com/' });
        cache.info = await probe(cache.file).catch(() => ({}));
        sentMsg = await sendFileToTelegram({
          token: previewTok,
          chatId: qItem.adminChatId,
          mode: 'video',
          file: cache.file,
          caption: cap,
          markup: kb,
          meta: { width: cache.info.width || qItem.width, height: cache.info.height || qItem.height, duration: cache.info.duration || qItem.duration }
        });
      }
      if (sentMsg && sentMsg.message_id) {
        qItem.adminMsgId = sentMsg.message_id;
        qItem.adminToken = previewTok;
        if (sentMsg.video && sentMsg.video.file_id && previewTok === X.token) {
          qItem.cachedFileId = sentMsg.video.file_id;
          qItem.cachedToken = previewTok;
        }
      }
    } catch (e) {
      log('xwatch', 'не вдалось надіслати прев\'ю відложки адміну:', redact(e.message).slice(0, 120));
    } finally {
      if (cache.file) safeUnlink(cache.file);
    }
  }

  X.queue.push(qItem);
  markGlobalMediaSeen(mKey);
  rememberPostMeta(acc, item.id, item.text);
  Store.schedule();
  log('xwatch', 'додано у відложку на', X.holdHours || 24, 'год:', acc, item.id, 'qid=' + qid);
  return qItem;
}

async function updateHoldAdminCard(qItem, statusLine) {
  if (!qItem || !qItem.adminMsgId || !qItem.adminChatId) return;
  const tok = qItem.adminToken || Store.token() || X.token;
  if (!tok) return;
  const newCap = statusLine + '\n📡 <b>@' + esc(qItem.account) + '</b> → <code>' + esc((qItem.targets || allTargets()).join(', ')) + '</code>\n──────────────\n' + esc(xCaption(qItem.text)).slice(0, 750);
  await tg(tok, 'editMessageCaption', {
    chat_id: qItem.adminChatId,
    message_id: qItem.adminMsgId,
    caption: newCap,
    parse_mode: 'HTML',
    reply_markup: { inline_keyboard: [] }
  }, { timeout: 10000, noRetry: true }).catch(() => {});
}

// Перевіряє чергу відложки і публікує ті відео, яким вже виповнився 1 день (або час виходу настав)
async function processHoldQueue() {
  if (!Array.isArray(X.queue) || !X.queue.length) return 0;
  const now = Date.now();
  const due = X.queue.filter(q => q && now >= (q.publishAt || 0));
  if (!due.length) return 0;
  let published = 0;
  for (const qItem of due.slice(0, 5)) {
    const r = await postToTargets(qItem, qItem.targets && qItem.targets.length ? qItem.targets : allTargets());
    if (r.sent.length) {
      X.queue = X.queue.filter(x => x.qid !== qItem.qid);
      published++;
      xr.lastOkAt = Date.now();
      await updateHoldAdminCard(qItem, '✅ <b>ОПУБЛИКОВАНО В КАНАЛ (по таймеру отложки)</b>');
      log('xwatch', 'відложка опублікована:', qItem.account, qItem.id, '→', r.sent.join(','));
    } else {
      // Відкладаємо повторну спробу на 2 хвилини
      qItem.publishAt = Date.now() + 120000;
    }
  }
  if (published) Store.schedule();
  return published;
}

// Ручні дії з відложкою (викласти зараз / видалити повтор / змінити час / очистити)
async function handleQueueAction(b) {
  if (!Array.isArray(X.queue)) X.queue = [];
  const act = String(b.queueAction || '').trim();
  const qid = String(b.qid || '').trim();

  if (act === 'pub' && qid) {
    const qItem = X.queue.find(x => x.qid === qid);
    if (!qItem) return { ok: false, msg: 'Пост вже опублікований або видалений з відложки' };
    const r = await postToTargets(qItem, qItem.targets && qItem.targets.length ? qItem.targets : allTargets());
    if (r.sent.length) {
      X.queue = X.queue.filter(x => x.qid !== qid);
      Store.schedule();
      await updateHoldAdminCard(qItem, '✅ <b>ОПУБЛИКОВАНО В КАНАЛ ВРУЧНУЮ</b>');
      return { ok: true, msg: '✅ Опубліковано в ' + r.sent.join(', ') };
    }
    return { ok: false, msg: '❌ Помилка публікації: ' + ((r.failed[0] && r.failed[0].err) || 'не вдалось') };
  }

  if (act === 'del' && qid) {
    const qItem = X.queue.find(x => x.qid === qid);
    if (!qItem) return { ok: false, msg: 'Вже видалено з відложки' };
    X.queue = X.queue.filter(x => x.qid !== qid);
    Store.schedule();
    await updateHoldAdminCard(qItem, '🗑 <b>УДАЛЕНО ИЗ ОТЛОЖКИ (ПОВТОР)</b>');
    return { ok: true, msg: '🗑 Видалено з відложки (повтор скасовано)' };
  }

  if (act === '1h' && qid) {
    const qItem = X.queue.find(x => x.qid === qid);
    if (!qItem) return { ok: false, msg: 'Пост не знайдено у відложці' };
    qItem.publishAt = Date.now() + 3600 * 1000;
    Store.schedule();
    const tok = qItem.adminToken || Store.token() || X.token;
    if (tok && qItem.adminMsgId && qItem.adminChatId) {
      await tg(tok, 'editMessageCaption', {
        chat_id: qItem.adminChatId,
        message_id: qItem.adminMsgId,
        caption: buildHoldPreviewCaption(qItem),
        parse_mode: 'HTML',
        reply_markup: buildHoldPreviewKb(qItem.qid)
      }, { timeout: 10000, noRetry: true }).catch(() => {});
    }
    return { ok: true, msg: '⏳ Час виходу перенесено на +1 годину' };
  }

  if (act === 'pubnext') {
    const sorted = [...X.queue].sort((a, b) => (a.publishAt || 0) - (b.publishAt || 0));
    const qItem = sorted[0];
    if (!qItem) return { ok: false, msg: 'Відложка порожня' };
    return handleQueueAction({ queueAction: 'pub', qid: qItem.qid });
  }

  if (act === 'puball') {
    let count = 0;
    for (const qItem of [...X.queue]) {
      const r = await postToTargets(qItem, qItem.targets && qItem.targets.length ? qItem.targets : allTargets());
      if (r.sent.length) {
        X.queue = X.queue.filter(x => x.qid !== qItem.qid);
        count++;
        await updateHoldAdminCard(qItem, '✅ <b>ОПУБЛИКОВАНО В КАНАЛ ВРУЧНУЮ</b>');
      }
    }
    Store.schedule();
    return { ok: true, msg: '✅ Опубліковано з відложки: ' + count };
  }

  if (act === 'clear') {
    const n = X.queue.length;
    for (const qItem of X.queue.slice(0, 15)) {
      await updateHoldAdminCard(qItem, '🗑 <b>УДАЛЕНО (ОЧИСТКА ОТЛОЖКИ)</b>');
    }
    X.queue = [];
    Store.schedule();
    return { ok: true, msg: '🗑 Очищено постів із відложки: ' + n };
  }

  return { ok: false, msg: 'Невідома дія' };
}

// ---- виявлення каналів + обробка кнопок відложки, якщо картка була надіслана через X.token ----
async function discoverChats() {
  xr.lastDiscoverAt = Date.now();
  const tok = X.token;
  if (!tok) return;
  try {
    const upd = await tg(tok, 'getUpdates', { offset: X.offset ? X.offset + 1 : undefined, limit: 100, timeout: 0, allowed_updates: ['my_chat_member', 'channel_post', 'callback_query'] }, { timeout: 15000, noRetry: true });
    let changed = false;
    for (const u of upd) {
      if (u.update_id > X.offset) { X.offset = u.update_id; changed = true; }
      if (u.callback_query && u.callback_query.data && String(u.callback_query.data).startsWith('xw:q')) {
        const parts = String(u.callback_query.data).split(':');
        const sub = parts[1].slice(1); // qpub -> pub, qdel -> del, q1h -> 1h
        const qid = parts[2] || '';
        const res = await handleQueueAction({ queueAction: sub, qid });
        await tg(tok, 'answerCallbackQuery', { callback_query_id: u.callback_query.id, text: res.msg || 'Готово' }, { timeout: 8000, noRetry: true }).catch(() => {});
        continue;
      }
      const mcm = u.my_chat_member;
      const chat = (mcm && mcm.chat) || (u.channel_post && u.channel_post.chat);
      if (!chat || !['channel', 'supergroup', 'group'].includes(chat.type)) continue;
      const cid = chat.username ? ('@' + chat.username) : String(chat.id);
      const status = mcm && mcm.new_chat_member && mcm.new_chat_member.status;
      if (status === 'left' || status === 'kicked') {
        const before = X.autoChats.length;
        X.autoChats = X.autoChats.filter(c => lc(c) !== lc(cid) && c !== String(chat.id));
        if (X.autoChats.length !== before) changed = true;
      } else {
        const canPost = chat.type !== 'channel' || !mcm || (mcm.new_chat_member.status === 'administrator' && mcm.new_chat_member.can_post_messages !== false);
        if (canPost && !allTargets().some(c => lc(c) === lc(cid) || c === String(chat.id))) { X.autoChats.push(cid); changed = true; log('xwatch', 'виявлено новий канал:', cid); }
      }
    }
    xr.discoverError = '';
    if (changed) { Store.schedule(); verifyTargets().catch(() => {}); }
  } catch (e) {
    xr.discoverError = /409|webhook|terminated by other/i.test(e.message)
      ? 'у бота-постера є webhook/інший getUpdates — авто-виявлення вимкнене, додавай канали командою /xwatch chat'
      : redact(e.message).slice(0, 120);
  }
}

async function verifyTargets() {
  const tok = X.token || Store.token();
  xr.lastVerifyAt = Date.now();
  if (!tok) return;
  try {
    if (!xr.botId) { const me = await tg(tok, 'getMe', {}, { timeout: 10000, noRetry: true }); xr.botId = me.id; xr.botName = me.username || ''; }
  } catch (e) { return; }
  for (const chat of allTargets()) {
    const st = xr.targetStatus[chat] = xr.targetStatus[chat] || {};
    try {
      const info = await tg(tok, 'getChat', { chat_id: chat }, { timeout: 10000, noRetry: true });
      st.title = info.title || info.username || '';
      st.type = info.type;
      const mem = await tg(tok, 'getChatMember', { chat_id: chat, user_id: xr.botId }, { timeout: 10000, noRetry: true });
      st.memberStatus = mem.status;
      st.canPost = info.type === 'channel' ? (mem.status === 'administrator' && mem.can_post_messages !== false) : !['left', 'kicked'].includes(mem.status);
      st.ok = st.canPost;
      st.err = st.canPost ? '' : ('бот @' + xr.botName + ' не має права публікувати (статус: ' + mem.status + ') — додай його адміністратором каналу з правом "Публікація повідомлень"');
    } catch (e) {
      st.ok = false; st.canPost = false;
      st.err = redact(e.message).slice(0, 140);
    }
    st.checkedAt = Date.now();
  }
}

// ---- основний цикл опитування КОЖНОГО акаунта ----
const trimSeen = key => { if ((X.seen[key] || []).length > SEEN_CAP) X.seen[key] = X.seen[key].slice(0, SEEN_CAP); };
const pushSeen = (key, id) => { const a = X.seen[key] = X.seen[key] || []; if (!a.includes(id)) { a.unshift(id); a.sort((x, y) => cmpId(y, x)); } trimSeen(key); };

async function pollAccount(acc, targets) {
  const key = lc(acc);
  const pa = xr.perAccount[key] = xr.perAccount[key] || {};
  let res;
  try { res = await fetchXPosts(acc); }
  catch (e) { pa.error = redact(e.message).slice(0, 300); pa.at = Date.now(); xr.lastError = acc + ': ' + pa.error; return false; }
  pa.error = ''; pa.source = res.source; pa.at = Date.now(); pa.found = res.items.length;
  const seen = new Set(X.seen[key] || []);

  // ВАЖЛИВО: Якщо для 2-го чи нового акаунта ще немає базової лінії, встановлюємо її ТІЛЬКИ коли отримано хоча б 1 пост,
  // а як базовий час беремо час найновішого поточного поста (а не Date.now()), щоб жоден новий пост не загубився!
  if (!X.baseline[key]) {
    if (!res.items.length) {
      pa.note = 'очікую пости від джерел (стрічка порожня)';
      return true;
    }
    for (const it of res.items) pushSeen(key, it.id);
    const newestTs = Math.max(...res.items.map(i => i.ts || snowTs(i.id) || 0));
    X.baseline[key] = newestTs > 0 ? newestTs : Date.now();
    pa.note = 'базова лінія: запам\'ятано ' + res.items.length + ' постів, ловлю нові';
    Store.schedule();
    return true;
  }

  const bAt = Number(X.baseline[key]) || 0;
  const fresh = res.items.filter(i => !seen.has(i.id)).sort((a, b) => cmpId(a.id, b.id));
  if (!fresh.length) {
    pa.note = 'онлайн · перевірено ' + res.items.length + ' постів (нових немає)';
    return true;
  }

  const postable = [];
  let skippedOld = 0;
  for (const it of fresh) {
    const ts = it.ts || snowTs(it.id);
    if ((bAt > 1000000000000 && ts < bAt - 180000) || (X_MAX_AGE_MS && Date.now() - ts > X_MAX_AGE_MS)) {
      pushSeen(key, it.id);
      skippedOld++;
    } else {
      postable.push(it);
    }
  }

  if (!postable.length) {
    pa.note = 'нових немає (старих пропущено: ' + skippedOld + ')';
    Store.schedule();
    return true;
  }

  const toPost = postable.slice(0, 10);
  let handledCount = 0;
  let pendingRetry = 0;

  for (const it of toPost) {
    let item = { ...it, account: acc };
    if (!item.videoUrl) {
      const v = await resolveTweetVideo(acc, it.id, 0, it);
      if (!v || !v.videoUrl) {
        const ageMs = Date.now() - (it.ts || snowTs(it.id));
        const nvTries = (xr.noVideoTries[it.id] = (xr.noVideoTries[it.id] || 0) + 1);
        if (ageMs < 90000 && nvTries <= 3) {
          pendingRetry++;
          continue;
        }
        pushSeen(key, it.id);
        continue;
      }
      item = { ...item, videoUrl: v.videoUrl, width: v.width, height: v.height, duration: v.duration, text: v.text || it.text, donorId: v.donorId || '' };
    }

    // Глобальний захист від точних дублів відео між 1-м і 2-м акаунтами
    const mKey = videoMediaKey(item.videoUrl, item.donorId);
    if (hasGlobalMediaSeen(mKey)) {
      pushSeen(key, it.id);
      log('xwatch', acc, it.id, 'точний дубль відео (' + mKey + ') вже взято з іншого акаунта, пропускаю');
      continue;
    }

    const dupSim = findSimilarRecentPost(item.text, acc, item.id);

    // Якщо увімкнено режим "Відложка на 1 день (24 год)" — кладемо у відложку з прев'ю для перевірки людиною!
    if (X.holdEnabled) {
      await enqueueHoldVideo(acc, item, targets, dupSim);
      pushSeen(key, it.id);
      handledCount++;
      xr.lastOkAt = Date.now();
      continue;
    }

    // Інакше публікуємо одразу в усі цільові канали
    const r = await postToTargets(item, targets);
    if (r.sent.length) {
      pushSeen(key, it.id);
      markGlobalMediaSeen(mKey);
      handledCount++;
      xr.lastOkAt = Date.now();
      log('xwatch', 'надіслано', acc, it.id, '→', r.sent.join(','));
    } else {
      const n = (xr.tries[it.id] = (xr.tries[it.id] || 0) + 1);
      if (n >= 4) {
        pushSeen(key, it.id);
        pa.note = 'не вдалось надіслати після 4 спроб';
      } else {
        pendingRetry++;
        pa.note = 'повтор відправки (' + n + '/4)';
      }
    }
  }

  Store.schedule();
  pa.note = handledCount
    ? (X.holdEnabled ? ('📅 у відложку (24ч): +' + handledCount) : ('✅ опубліковано: +' + handledCount))
    : (pendingRetry ? ('⏳ очікую рендер відео (' + pendingRetry + ')') : 'нових відео немає');
  return true;
}

let xRunning = false;
let xTimer = null;
async function xTick() {
  if (xRunning) return;
  xRunning = true;
  const t0 = Date.now();
  try {
    xr.ticks++;
    if (!X.enabled) { xr.phase = 'вимкнено'; return; }
    if (!Store.loaded && Store.token() && Date.now() - BOOT_AT < 150000) {
      await Store.load();
      if (!Store.loaded) { xr.phase = 'очікую завантаження стану'; return; }
    }
    if (!X.token && !Store.token()) { xr.phase = 'немає токена бота-постера'; return; }
    if (Date.now() - xr.lastDiscoverAt > 60000) await discoverChats();
    if (Date.now() - xr.lastVerifyAt > 15 * 60 * 1000) await verifyTargets().catch(() => {});

    // Спочатку перевіряємо чергу відложки (чи настав час публікувати пости, що відлежали 24 год)
    await processHoldQueue().catch(e => log('xwatch', 'помилка processHoldQueue:', e.message));

    const targets = allTargets();
    if (!targets.length) { xr.phase = 'немає каналів (додай бота адміном у канал або /xwatch chat)'; return; }
    if (!X.accounts.length) { xr.phase = 'немає акаунтів X'; return; }
    xr.phase = 'працює';

    // ВАЖЛИВО: Ротуємо стартовий акаунт на кожному тику (Round-Robin) + робимо паузу 850 мс між акаунтами,
    // щоб 1-й і 2-й акаунти (@GoalsXtra і @TheGoalsZone) мали однакову пріоритетність і НЕ збивали один одного лімітами!
    const accs = [...X.accounts];
    const shift = xr.rrIndex % accs.length;
    xr.rrIndex = (xr.rrIndex + 1) % Math.max(1, accs.length);
    const ordered = [...accs.slice(shift), ...accs.slice(0, shift)];

    let allOk = true;
    for (let i = 0; i < ordered.length; i++) {
      if (i > 0) await sleep(850);
      const ok = await pollAccount(ordered[i], targets).catch(e => {
        log('xwatch', 'збій pollAccount', ordered[i], e.message);
        return false;
      });
      if (!ok) allOk = false;
    }
    if (allOk) xr.lastError = '';
  } catch (e) {
    xr.lastError = redact(e.message).slice(0, 200);
    log('xwatch', 'збій циклу:', xr.lastError);
  } finally {
    xr.lastTickAt = Date.now();
    xRunning = false;
    xTimer = setTimeout(xTick, Math.max(2000, X_INTERVAL_SEC * 1000 - (Date.now() - t0)));
  }
}

// ---- ПОВНА ДІАГНОСТИКА ВСІХ АКАУНТІВ, ДЖЕРЕЛ ТА КАНАЛІВ (/xwatch diag) ----
async function runXDiag({ adminChat = '', fallbackToken = '' } = {}) {
  const admin = String(adminChat || runtime.adminChat || ADMIN_CHAT_ID);
  const adminTok = Store.token() || fallbackToken || X.token;
  await verifyTargets().catch(() => {});
  const targets = allTargets();
  const reportLines = ['🩺 <b>Повна діагностика X-Watcher (всі акаунти, джерела та канали)</b>', ''];

  // 1. Перевірка каналів
  reportLines.push('📢 <b>1. Перевірка Telegram-каналів (' + targets.length + '):</b>');
  if (!targets.length) {
    reportLines.push('   ❌ Жодного каналу не підключено!');
  } else {
    for (let idx = 0; idx < targets.length; idx++) {
      const c = targets[idx];
      const st = xr.targetStatus[c] || {};
      reportLines.push('   ' + (idx + 1) + ') <code>' + esc(c) + '</code>' + (st.title ? ' (' + esc(st.title) + ')' : '') + ': ' +
        (st.ok ? '✅ готовий до публікації' : ('❌ помилка: <code>' + esc(st.err || st.lastErr || 'немає прав адміна') + '</code>')));
    }
  }
  reportLines.push('');

  // 2. Перевірка КОЖНОГО акаунта X по всіх джерелах
  reportLines.push('👥 <b>2. Перевірка акаунтів X (' + X.accounts.length + '):</b>');
  for (let idx = 0; idx < X.accounts.length; idx++) {
    const acc = X.accounts[idx];
    const t0 = Date.now();
    try {
      const res = await fetchXPosts(acc, { forceAll: true });
      const perSrc = Object.entries(res.perSourceCounts || {}).map(([k, v]) => v.ok ? (k + ':' + v.count) : (k + ':❌')).join(', ');
      let resolvedSample = null;
      for (const it of res.items.slice(0, 6)) {
        if (it.videoUrl) { resolvedSample = { id: it.id, url: it.videoUrl }; break; }
        const v = await resolveTweetVideo(acc, it.id, 0, it);
        if (v && v.videoUrl) { resolvedSample = { id: it.id, url: v.videoUrl, donorId: v.donorId }; break; }
      }
      reportLines.push(
        '   ' + (idx + 1) + ') <b>@' + esc(acc) + '</b> (' + Math.round((Date.now() - t0) / 100) / 10 + 'с):\n' +
        '      • Знайдено постів: <b>' + res.items.length + '</b> (активні: <code>' + esc(res.source) + '</code>)\n' +
        '      • Деталі джерел: <code>' + esc(perSrc) + '</code>\n' +
        '      • Тест витягування відео: ' + (resolvedSample ? ('✅ OK (пост <code>' + resolvedSample.id + '</code>' + (resolvedSample.donorId ? ', донор <code>' + resolvedSample.donorId + '</code>' : '') + ')') : '⚠️ серед останніх 6 постів відео не знайдено')
      );
    } catch (e) {
      reportLines.push('   ' + (idx + 1) + ') <b>@' + esc(acc) + '</b>: ❌ ' + esc(redact(e.message).slice(0, 200)));
    }
  }
  reportLines.push('');

  // 3. Відложка та анти-дубль
  reportLines.push(
    '📅 <b>3. Режим відложки (1 день / 24 год):</b> ' + (X.holdEnabled ? ('🟢 УВІМКНЕНО (' + (X.holdHours || 24) + ' год)') : '⚪ ВИМКНЕНО (одразу в канал)') + '\n' +
    '   • Зараз у черзі відложки: <b>' + ((X.queue && X.queue.length) || 0) + '</b> відео\n' +
    '   • База анти-дублів відео: <b>' + ((X.mediaSeen && X.mediaSeen.length) || 0) + '</b> хешів'
  );

  const text = reportLines.join('\n');
  if (adminTok) {
    await tg(adminTok, 'sendMessage', { chat_id: admin, text, parse_mode: 'HTML', link_preview_options: { is_disabled: true } }, { timeout: 15000, noRetry: true }).catch(() => {});
  }
  xr.diag = { at: Date.now(), text: stripHtml(text) };
  return { ok: true, text };
}

// ---- ТЕСТ: бере останні N відео з КОЖНОГО акаунта і кладе у відложку (або одразу в усі канали, якщо mode='live') ----
async function runXTest({ account = '', chat = '', limit = 1, mode = 'auto', adminChat = '', fallbackToken = '' } = {}) {
  const lim = Math.max(1, Math.min(4, Number(limit) || 1));
  const accounts = account ? [parseXUsername(account)].filter(Boolean) : [...X.accounts];
  const destTargets = chat ? uniq(parseList(chat).map(parseTelegramChat).filter(Boolean)) : allTargets();
  const admin = String(adminChat || runtime.adminChat || ADMIN_CHAT_ID);
  const adminTok = Store.token() || fallbackToken || X.token;
  const posterTok = X.token || fallbackToken || Store.token();
  const useHold = mode === 'hold' ? true : (mode === 'live' ? false : !!X.holdEnabled);
  const lines = [];
  let total = 0;

  for (let i = 0; i < accounts.length; i++) {
    const acc = accounts[i];
    if (i > 0) await sleep(700);
    const key = lc(acc);
    try {
      const res = await fetchXPosts(acc, { forceAll: true });
      const videos = [];
      for (const it of res.items) {
        if (videos.length >= lim) break;
        if (it.videoUrl) { videos.push({ ...it, account: acc }); continue; }
        const v = await resolveTweetVideo(acc, it.id, 0, it);
        if (v && v.videoUrl) {
          videos.push({ ...it, account: acc, videoUrl: v.videoUrl, width: v.width, height: v.height, duration: v.duration, text: v.text || it.text, donorId: v.donorId || '' });
        }
      }
      if (!videos.length) {
        lines.push('• <b>@' + esc(acc) + '</b>: відео серед останніх ' + res.items.length + ' постів не знайдено (джерела: ' + esc(res.source) + ')');
        continue;
      }
      let posted = 0;
      let where = useHold ? ('відложка на ' + (X.holdHours || 24) + ' год (прев\'ю в адмін-чаті)') : (destTargets.join(', ') || admin);
      for (const v of videos.sort((a, b) => cmpId(a.id, b.id))) {
        pushSeen(key, v.id);
        if (useHold) {
          const dupSim = findSimilarRecentPost(v.text, acc, v.id);
          await enqueueHoldVideo(acc, v, destTargets, dupSim);
          posted++;
        } else {
          let r = destTargets.length && posterTok ? await postToTargets(v, destTargets, posterTok) : { sent: [], failed: [{ chat: where, err: 'немає каналу/токена' }] };
          if (!r.sent.length && adminTok) { r = await postToTargets(v, [admin], adminTok); where = admin + ' (резерв)'; }
          if (r.sent.length) posted++;
          else lines.push('   ⚠️ @' + esc(acc) + ' ' + v.id + ': ' + esc((r.failed[0] && r.failed[0].err) || 'не вдалось'));
        }
      }
      total += posted;
      if (!X.baseline[key] && res.items.length) {
        const newestTs = Math.max(...res.items.map(x => x.ts || snowTs(x.id) || 0));
        X.baseline[key] = newestTs > 0 ? newestTs : Date.now();
      }
      lines.push('• <b>@' + esc(acc) + '</b>: ' + (posted ? '✅ взято ' + posted + ' із ' + videos.length : '❌ не вдалось') + ' → <code>' + esc(where) + '</code> (джерела: ' + esc(res.source) + ')');
    } catch (e) {
      lines.push('• <b>@' + esc(acc) + '</b>: ❌ ' + esc(redact(e.message).slice(0, 260)));
    }
  }
  Store.schedule();
  const srcLine = Object.entries(srcStats).map(([n, s]) => n + (s.consec ? ' ❌' : (s.ok ? ' ✅' : ' —'))).join(' · ');
  const modeTitle = useHold ? ('у відложку на ' + (X.holdHours || 24) + ' год') : 'прямо в канали';
  const text = (total ? '✅ <b>Тест X-Watcher завершено (' + modeTitle + ')</b>\n\n' : '⚠️ <b>Тест X-Watcher: нічого не надіслано</b>\n\n') + lines.join('\n') + '\n\n<i>Джерела: ' + srcLine + '</i>';
  if (adminTok) await tg(adminTok, 'sendMessage', { chat_id: admin, text, parse_mode: 'HTML', link_preview_options: { is_disabled: true } }, { timeout: 15000, noRetry: true }).catch(e => log('xwatch', 'не вдалось надіслати підсумок тесту:', e.message));
  xr.test = { at: Date.now(), total, lines: lines.map(stripHtml) };
  return { total, lines };
}

function xStatusJson() {
  const targets = allTargets();
  const qList = Array.isArray(X.queue) ? X.queue : [];
  return {
    ok: true,
    enabled: X.enabled,
    accounts: X.accounts,
    targetChat: targets.join(', '),
    targets: X.targets,
    autoChats: X.autoChats,
    hasCustomToken: !!(X.token || Store.token()),
    creditText: X.credit,
    holdEnabled: !!X.holdEnabled,
    holdHours: Number(X.holdHours) || 24,
    queueCount: qList.length,
    queueItems: qList.slice(0, 25).map(q => ({
      qid: q.qid,
      account: q.account,
      id: q.id,
      text: String(q.text || '').slice(0, 120),
      createdAt: q.createdAt,
      publishAt: q.publishAt,
      dupWarning: q.dupWarning || '',
      targets: q.targets || targets
    })),
    postedCount: X.posted,
    lastCheckAt: xr.lastTickAt,
    lastOkAt: xr.lastOkAt,
    lastError: xr.lastError,
    phase: xr.phase,
    intervalSec: X_INTERVAL_SEC,
    fromState: state.loadedFromTelegram,
    persistence: { loaded: Store.loaded, hasToken: !!Store.token(), lastSavedAt: Store.lastSavedAt, lastError: Store.lastError, chat: String(STATE_CHAT_ID) },
    targetStatus: xr.targetStatus,
    perAccount: xr.perAccount,
    sources: Object.fromEntries(Object.entries(srcStats).map(([n, s]) => [n, { ok: s.ok, fail: s.fail, consec: s.consec, lastErr: s.lastErr, lastItems: s.lastItems, pausedSec: Math.max(0, Math.round((s.cooldownUntil - Date.now()) / 1000)) }])),
    discoverError: xr.discoverError,
    test: xr.test,
    diag: xr.diag
  };
}

async function applyXConfig(b) {
  const S = X;
  let changedTargets = false;
  if (b.mainBotToken && /^\d{5,}:[A-Za-z0-9_-]{20,}$/.test(String(b.mainBotToken).trim())) {
    runtime.mainToken = String(b.mainBotToken).trim();
  }
  if (typeof b.enabled === 'boolean') S.enabled = b.enabled;
  if (typeof b.holdEnabled === 'boolean') S.holdEnabled = b.holdEnabled;
  if (b.holdHours !== undefined && Number.isFinite(Number(b.holdHours)) && Number(b.holdHours) >= 0) {
    S.holdHours = Number(b.holdHours);
    if (S.holdHours === 0) S.holdEnabled = false;
  }
  if (b.accounts !== undefined) {
    const list = Array.isArray(b.accounts) ? b.accounts : parseList(b.accounts);
    const n = uniq(list.map(parseXUsername).filter(Boolean));
    if (n.length) S.accounts = n;
  }
  if (b.addAccount) {
    const list = Array.isArray(b.addAccount) ? b.addAccount : parseList(b.addAccount);
    for (const raw of list) {
      const a = parseXUsername(raw);
      if (a && !S.accounts.some(x => lc(x) === lc(a))) S.accounts.push(a);
    }
  }
  if (b.delAccount) {
    const list = Array.isArray(b.delAccount) ? b.delAccount : parseList(b.delAccount);
    for (const raw of list) {
      const a = lc(parseXUsername(raw));
      S.accounts = S.accounts.filter(x => lc(x) !== a);
    }
  }
  if (b.targets !== undefined || b.targetChat !== undefined) {
    const raw = b.targets !== undefined ? b.targets : b.targetChat;
    const list = (Array.isArray(raw) ? raw : parseList(raw)).map(parseTelegramChat).filter(Boolean);
    if (list.length || raw === '') { S.targets = uniq(list); changedTargets = true; }
  }
  if (b.addTarget) {
    const list = (Array.isArray(b.addTarget) ? b.addTarget : parseList(b.addTarget)).map(parseTelegramChat).filter(Boolean);
    for (const c of list) {
      if (c && !allTargets().some(x => lc(x) === lc(c))) { S.targets.push(c); changedTargets = true; }
    }
  }
  if (b.delTarget) {
    const list = (Array.isArray(b.delTarget) ? b.delTarget : parseList(b.delTarget)).map(parseTelegramChat).filter(Boolean);
    for (const raw of list) {
      const c = lc(raw);
      S.targets = S.targets.filter(x => lc(x) !== c);
      S.autoChats = S.autoChats.filter(x => lc(x) !== c);
      changedTargets = true;
    }
  }
  if (b.creditText !== undefined && String(b.creditText).trim()) S.credit = String(b.creditText).trim();
  if (b.botToken && /^\d{5,}:[A-Za-z0-9_-]{20,}$/.test(String(b.botToken).trim())) {
    const t = String(b.botToken).trim();
    if (t !== S.token) { S.token = t; xr.botId = 0; changedTargets = true; }
  }
  if (b.resetSeen) { S.seen = {}; S.baseline = {}; S.mediaSeen = []; xr.resolvedCache.clear(); }
  if (b.reset) {
    S.accounts = uniq(parseList(env('X_ACCOUNTS', 'GoalsXtra,TheGoalsZone')).map(parseXUsername).filter(Boolean));
    S.targets = uniq(parseList(env('X_TARGET_CHAT', '@Fantasyargoal')).map(parseTelegramChat).filter(Boolean));
    S.autoChats = []; S.seen = {}; S.baseline = {}; changedTargets = true;
  }
  S.updatedAt = Date.now();
  state.loadedFromTelegram = true;
  Store.schedule();
  if (changedTargets) verifyTargets().catch(() => {});
  if (S.enabled && !xRunning) {
    if (xTimer) clearTimeout(xTimer);
    xTimer = setTimeout(xTick, 400);
  }
}

// ==================== 14. ЗАПУСК COBALT ТА LOCAL BOT API ====================
let cobaltBackoff = 3000;
function startCobalt() {
  const cobaltDir = fs.existsSync('/app/src/cobalt.js') || fs.existsSync('/app/src/cobalt') ? '/app' : null;
  if (!cobaltDir) { cobaltLastError = '/app/src/cobalt не знайдено'; log('cobalt', cobaltLastError); return; }
  const cenv = {
    ...process.env,
    API_PORT: String(COBALT_PORT),
    API_LISTEN_ADDRESS: '127.0.0.1',
    API_URL: PUBLIC_URL + '/',
    API_AUTH_REQUIRED: '0',
    DURATION_LIMIT: '86400',
    // ВАЖЛИВО: усі запити приходять із 127.0.0.1, тому дефолтні 20 запитів/хв ділилися на ВСІХ користувачів
    RATELIMIT_WINDOW: '60', RATELIMIT_MAX: '3000',
    TUNNEL_RATELIMIT_WINDOW: '60', TUNNEL_RATELIMIT_MAX: '3000',
    NODE_OPTIONS: '--max-old-space-size=200'
  };
  if (env('COBALT_PROXY')) { cenv.HTTPS_PROXY = env('COBALT_PROXY'); cenv.HTTP_PROXY = env('COBALT_PROXY'); cenv.NO_PROXY = '127.0.0.1,localhost'; }
  const proc = spawn('node', ['src/cobalt'], { cwd: cobaltDir, env: cenv, stdio: ['ignore', 'pipe', 'pipe'] });
  const started = Date.now();
  proc.stdout.on('data', d => { const s = d.toString().trim(); if (s) log('cobalt', s.slice(0, 300)); });
  proc.stderr.on('data', d => { const s = d.toString().trim(); if (s) { cobaltLastError = s.split('\n').pop().slice(0, 200); log('cobalt-err', s.slice(0, 300)); } });
  proc.on('exit', code => {
    cobaltReady = false;
    if (Date.now() - started > 60000) cobaltBackoff = 3000;
    cobaltRestarts++;
    log('cobalt', 'завершився з кодом', code, '— перезапуск через', Math.round(cobaltBackoff / 1000), 'с');
    setTimeout(startCobalt, cobaltBackoff);
    cobaltBackoff = Math.min(cobaltBackoff * 2, 30000);
  });
  // справжня перевірка готовності замість setTimeout
  (async () => {
    for (let i = 0; i < 90; i++) {
      await sleep(1000);
      try {
        const r = await fetchRaw('http://127.0.0.1:' + COBALT_PORT + '/', {}, 2000);
        if (r.status > 0) { cobaltReady = true; cobaltLastError = ''; log('cobalt', 'готовий (', i + 1, 'с)'); return; }
      } catch (e) {}
    }
    log('cobalt', 'не відповів за 90 с');
  })();
}

let localApiBackoff = 3000;
function startLocalBotApi() {
  const bin = env('LOCAL_API_BIN', '/usr/local/bin/telegram-bot-api');
  if (!RAW_API_ID || !RAW_API_HASH) { localApiLastError = 'TELEGRAM_API_ID або TELEGRAM_API_HASH не задані'; return; }
  if (!API_ID_VALID) { localApiLastError = 'Невірний TELEGRAM_API_ID (' + RAW_API_ID.slice(0, 4) + '...): 5-12 цифр з my.telegram.org'; log('tgbotapi', localApiLastError); return; }
  if (!API_HASH_VALID) { localApiLastError = 'Невірний TELEGRAM_API_HASH (довжина ' + RAW_API_HASH.length + '): має бути 32 hex-символи'; log('tgbotapi', localApiLastError); return; }
  if (!fs.existsSync(bin)) { localApiLastError = 'Бінарник telegram-bot-api не знайдено'; return; }
  try { fs.mkdirSync('/tmp/tgbotapi', { recursive: true, mode: 0o755 }); fs.mkdirSync('/tmp/tgbotapi-tmp', { recursive: true, mode: 0o755 }); } catch (e) {}
  const proc = spawn(bin, ['--local', '--http-port=' + LOCAL_API_PORT, '--api-id=' + RAW_API_ID, '--api-hash=' + RAW_API_HASH, '--dir=/tmp/tgbotapi', '--temp-dir=/tmp/tgbotapi-tmp'], { stdio: ['ignore', 'pipe', 'pipe'] });
  const started = Date.now();
  proc.stdout.on('data', d => log('tgbotapi', redact(d.toString()).trim().slice(0, 200)));
  proc.stderr.on('data', d => { const s = redact(d.toString()).trim(); if (s) { localApiLastError = s.split('\n')[0].slice(0, 240); log('tgbotapi-err', s.slice(0, 240)); } });
  proc.on('exit', code => {
    localApiReady = false;
    if (Date.now() - started > 60000) localApiBackoff = 3000;
    log('tgbotapi', 'завершився з кодом', code, '— перезапуск через', Math.round(localApiBackoff / 1000), 'с');
    setTimeout(startLocalBotApi, localApiBackoff);
    localApiBackoff = Math.min(localApiBackoff * 2, 60000);
  });
  (async () => {
    for (let i = 0; i < 30; i++) {
      await sleep(1000);
      try {
        const r = await fetchRaw('http://127.0.0.1:' + LOCAL_API_PORT + '/', {}, 2000);
        if (r.status > 0) { localApiReady = true; localApiLastError = ''; log('tgbotapi', 'готовий на порту', LOCAL_API_PORT, '(файли до 2000 МБ)'); return; }
      } catch (e) {}
    }
  })();
}

// ==================== 15. HTTP ====================
function readJsonBody(req, limit = 15 * MB) {
  return new Promise((resolve, reject) => {
    const chunks = []; let len = 0;
    req.on('data', c => { len += c.length; if (len > limit) { req.destroy(); return reject(new Error('BODY_TOO_LARGE')); } chunks.push(c); });
    req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); } catch (e) { reject(new Error('INVALID_JSON')); } });
    req.on('error', reject);
  });
}

function checkAuth(req) {
  if (!API_KEY) return true;
  let supplied = String(req.headers['authorization'] || req.headers['x-api-key'] || '').trim().replace(/^(?:Api-Key|Bearer)\s+/i, '').trim();
  if (!supplied) { const m = String(req.url || '').match(/[?&]key=([^&]+)/); if (m) supplied = decodeURIComponent(m[1]); }
  if (!supplied) return false;
  const a = Buffer.from(supplied); const b = Buffer.from(API_KEY);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(body) });
  res.end(body);
}

function proxyToCobalt(req, res) {
  const h = { ...req.headers, host: '127.0.0.1:' + COBALT_PORT };
  for (const k of ['authorization', 'x-forwarded-for', 'cf-connecting-ip', 'true-client-ip', 'x-real-ip', 'x-client-ip', 'forwarded', 'x-api-key']) delete h[k];
  const proxyReq = http.request({ hostname: '127.0.0.1', port: COBALT_PORT, path: req.url, method: req.method, headers: h }, pr => {
    res.writeHead(pr.statusCode || 502, pr.headers);
    pr.pipe(res, { end: true });
  });
  proxyReq.on('error', err => {
    if (!res.headersSent) res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'error', error: { code: 'cobalt_unavailable', detail: err.message } }));
  });
  res.on('close', () => proxyReq.destroy());
  req.pipe(proxyReq, { end: true });
}

function healthJson() {
  return {
    ok: true,
    service: 'stream-sync-node',
    version: VERSION,
    cobaltReady, cobaltRestarts, cobaltLastError,
    ffmpeg: ffmpegOk,
    ytdlp: { version: ytdlpVersion || null, impersonate: ytdlpCanImpersonate, lastError: ytdlpLastError, cookies: Object.keys(COOKIE_FILES), proxy: !!(YT_PROXY || YTDLP_PROXY) },
    localBotApi: localApiReady,
    localBotApiDiagnostics: {
      apiIdProvided: !!RAW_API_ID, apiIdValid: API_ID_VALID,
      apiIdPreview: RAW_API_ID ? (RAW_API_ID.slice(0, 2) + '***' + RAW_API_ID.slice(-2)) : '',
      apiHashProvided: !!RAW_API_HASH, apiHashValid: API_HASH_VALID, apiHashLength: RAW_API_HASH.length,
      lastError: localApiLastError
    },
    jobs: { active: activeJobs, queued: jobQueue.length, max: MAX_JOBS },
    persistence: { loaded: Store.loaded, hasToken: !!Store.token(), lastSavedAt: Store.lastSavedAt, lastError: Store.lastError },
    xWatcher: {
      enabled: X.enabled, accounts: X.accounts, targetChat: allTargets().join(', '), targets: allTargets().length,
      postedCount: X.posted, lastCheckAt: xr.lastTickAt, phase: xr.phase, lastError: xr.lastError
    },
    uptime: Math.round(process.uptime())
  };
}

const server = http.createServer(async (req, res) => {
  const u = String(req.url || '/').split('?')[0];
  const qs = new URLSearchParams(String(req.url || '').split('?')[1] || '');
  const AUTH_FAIL = () => sendJson(res, 401, { ok: false, error: 'UNAUTHORIZED' });
  try {
    if (req.method === 'GET' && u === '/health') return sendJson(res, 200, healthJson());

    if (req.method === 'POST' && (u === '/send' || u === '/circle' || u === '/stickers' || u === '/stt')) {
      if (!checkAuth(req)) return AUTH_FAIL();
      let body;
      try { body = await readJsonBody(req); } catch (e) { return sendJson(res, 200, { ok: false, error: e.message }); }
      const fn = u === '/send' ? handleSend : u === '/circle' ? handleCircle : u === '/stt' ? handleStt : handleStickers;
      try { return sendJson(res, 200, await fn(body)); }
      catch (e) { log('http', u, 'виняток:', e.stack || e.message); return sendJson(res, 200, { ok: false, error: 'INTERNAL', detail: redact(e.message).slice(0, 300) }); }
    }

    if (req.method === 'GET' && u === '/yt-info') {
      if (!checkAuth(req)) return AUTH_FAIL();
      return sendJson(res, 200, await ytInfo(qs.get('url') || '').catch(e => ({ ok: false, error: redact(e.message) })));
    }
    if (req.method === 'GET' && u === '/diag') {
      if (!checkAuth(req)) return AUTH_FAIL();
      return sendJson(res, 200, await handleDiag(qs.get('url') || '', qs.get('q') || 720).catch(e => ({ ok: false, error: redact(e.message) })));
    }
    if (req.method === 'GET' && u === '/logs') {
      if (!checkAuth(req)) return AUTH_FAIL();
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      return res.end(LOG_RING.join('\n'));
    }
    if (req.method === 'GET' && u.startsWith('/job/')) {
      if (!checkAuth(req)) return AUTH_FAIL();
      return sendJson(res, 200, jobs.get(u.slice(5)) || { ok: false, error: 'NOT_FOUND' });
    }

    if (u === '/xwatch') {
      if (!checkAuth(req)) return AUTH_FAIL();
      if (req.method === 'GET') return sendJson(res, 200, xStatusJson());
      if (req.method === 'POST') {
        let body;
        try { body = await readJsonBody(req); } catch (e) { return sendJson(res, 400, { ok: false, error: e.message }); }
        if (body.adminChatId) runtime.adminChat = String(body.adminChatId);
        if (body.fallbackToken && !runtime.mainToken) runtime.mainToken = String(body.fallbackToken);
        await applyXConfig(body);
        if (body.queueAction) {
          const actionRes = await handleQueueAction(body);
          return sendJson(res, 200, { ...xStatusJson(), actionResult: actionRes });
        }
        if (body.diagNow) {
          runXDiag({ adminChat: body.adminChatId, fallbackToken: body.fallbackToken })
            .catch(e => log('xwatch', 'діагностика впала:', e.message));
          return sendJson(res, 200, { ...xStatusJson(), diagStarted: true });
        }
        if (body.testNow) {
          runXTest({
            account: body.testAccount,
            chat: body.testChat,
            limit: body.limit || 1,
            mode: body.testMode || 'auto',
            adminChat: body.adminChatId,
            fallbackToken: body.fallbackToken
          }).catch(e => log('xwatch', 'тест впав:', e.message));
          return sendJson(res, 200, { ...xStatusJson(), started: true });
        }
        return sendJson(res, 200, xStatusJson());
      }
    }

    if (req.method === 'POST' && (u === '/' || u === '/api/json')) {
      if (!checkAuth(req)) return sendJson(res, 401, { status: 'error', error: { code: 'error.api.auth.key.missing' } });
    }
    return proxyToCobalt(req, res);
  } catch (e) {
    log('http', 'необроблений виняток:', e.stack || e.message);
    if (!res.headersSent) sendJson(res, 500, { ok: false, error: 'INTERNAL' });
    else res.end();
  }
});
server.requestTimeout = 0;
server.headersTimeout = 65000;
server.keepAliveTimeout = 65000;

// ==================== 16. СЛУЖБОВІ ЗАДАЧІ ====================
function cleanTmp() {
  try {
    const now = Date.now();
    for (const f of fs.readdirSync(TMP_DIR)) {
      if (!/^ssn_|^media_|^cookies_/.test(f)) continue;
      const p = path.join(TMP_DIR, f);
      try { const st = fs.statSync(p); if (now - st.mtimeMs > 45 * 60 * 1000) fs.rmSync(p, { recursive: true, force: true }); } catch (e) {}
    }
  } catch (e) {}
}

async function shutdown(sig) {
  log('sys', 'отримано', sig, '— зберігаю стан');
  try { await withTimeout(Store.saveNow(), 12000, 'save'); } catch (e) {}
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', e => log('sys', 'unhandledRejection:', (e && e.message) || e));
process.on('uncaughtException', e => log('sys', 'uncaughtException:', (e && e.stack) || e));

(async function main() {
  setupCookies();
  startCobalt();
  startLocalBotApi();
  server.listen(PORT, '0.0.0.0', () => log('sys', 'stream-sync-node v' + VERSION + ' слухає порт ' + PORT + ' (public: ' + PUBLIC_URL + ')'));
  ytdlpInit = ensureYtDlp().catch(() => {});
  ytdlpInit.then(() => updateYtDlp()).catch(() => {});
  setInterval(() => updateYtDlp().catch(() => {}), 12 * 3600 * 1000);
  setInterval(cleanTmp, 10 * 60 * 1000);
  // Анти-сон для Render: пінгуємо власний публічний URL
  if (/^https:\/\//i.test(PUBLIC_URL)) setInterval(() => { fetch(PUBLIC_URL + '/health').catch(() => {}); }, 8 * 60 * 1000);
  // Підтягуємо збережений стан (якщо є токен у Environment)
  Store.load().then(() => { if (Store.dirty) Store.saveNow(); }).catch(() => {});
  setTimeout(xTick, 5000);
  log('sys', 'X-вотчер: акаунти', X.accounts.join(','), '| канали', allTargets().join(',') || '—', '| інтервал', X_INTERVAL_SEC, 'с');
})();
