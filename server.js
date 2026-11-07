'use strict';

const http = require('node:http');
const https = require('node:https');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const crypto = require('node:crypto');

const PORT = Number(process.env.PORT || 10000);
const COBALT_PORT = Number(process.env.COBALT_INTERNAL_PORT || 9000);
const LOCAL_API_PORT = Number(process.env.LOCAL_BOT_API_PORT || 8081);
const API_KEY = String(process.env.API_KEY || process.env.COBALT_API_KEY || '8bfef237-aef7-4964-a01d-65cd9c2a7ef2').trim();
const PUBLIC_URL = String(process.env.RENDER_EXTERNAL_URL || process.env.API_URL || ('http://127.0.0.1:' + PORT)).replace(/\/+$/, '');

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';
const MB = 1024 * 1024;
const STD_TG_LIMIT = Math.floor(49.2 * MB);

let cobaltReady = true;
let localApiReady = Boolean(process.env.TELEGRAM_API_ID && process.env.TELEGRAM_API_HASH);
let localApiLastError = '';
let ytDlpBin = fs.existsSync('/usr/local/bin/yt-dlp') ? '/usr/local/bin/yt-dlp' : '';

// Автоматичне підвантаження yt-dlp_linux у /tmp/yt-dlp, якщо образ зібрано зі старого Dockerfile
async function ensureYtDlp() {
  if (ytDlpBin && fs.existsSync(ytDlpBin)) return ytDlpBin;
  const tmpBin = '/tmp/yt-dlp';
  if (fs.existsSync(tmpBin)) {
    ytDlpBin = tmpBin;
    return ytDlpBin;
  }
  try {
    const r = await fetch('https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux', {
      headers: { 'User-Agent': UA },
      redirect: 'follow'
    });
    if (r.ok && r.body) {
      const buf = Buffer.from(await r.arrayBuffer());
      if (buf.length > 1000000) {
        fs.writeFileSync(tmpBin, buf, { mode: 0o755 });
        ytDlpBin = tmpBin;
        console.log('[yt-dlp] Standalone binary downloaded to /tmp/yt-dlp');
      }
    }
  } catch (e) {
    console.warn('[yt-dlp] Auto-download skipped:', e.message);
  }
  return ytDlpBin;
}

// Очищення та перевірка TELEGRAM_API_ID і TELEGRAM_API_HASH
const RAW_API_ID = String(process.env.TELEGRAM_API_ID || '').replace(/['"\s]+/g, '');
const RAW_API_HASH = String(process.env.TELEGRAM_API_HASH || '').replace(/['"\s]+/g, '');
const API_ID_VALID = /^\d{5,12}$/.test(RAW_API_ID);
const API_HASH_VALID = /^[0-9a-fA-F]{32}$/.test(RAW_API_HASH);

// ==================== 1. ЗАПУСК COBALT V10 ВНУТРІ КОНТЕЙНЕРА ====================
function startCobalt() {
  const cobaltDir = fs.existsSync('/app/src/cobalt.js') || fs.existsSync('/app/src/cobalt') ? '/app' : null;
  if (!cobaltDir) {
    console.warn('[cobalt] /app/src/cobalt not found, skipping internal Cobalt');
    return;
  }
  const env = {
    ...process.env,
    API_PORT: String(COBALT_PORT),
    API_LISTEN_ADDRESS: '0.0.0.0',
    API_URL: PUBLIC_URL + '/',
    API_AUTH_REQUIRED: '0',
    DURATION_LIMIT: '86400',
    NODE_OPTIONS: '--max-old-space-size=160'
  };
  const proc = spawn('node', ['src/cobalt'], {
    cwd: cobaltDir,
    env,
    stdio: ['ignore', 'pipe', 'pipe']
  });
  proc.stdout.on('data', d => {
    const s = d.toString();
    if (/listening|ready|port|9000/i.test(s)) cobaltReady = true;
    process.stdout.write('[cobalt] ' + s);
  });
  proc.stderr.on('data', d => process.stderr.write('[cobalt-err] ' + d.toString()));
  proc.on('exit', code => {
    cobaltReady = false;
    console.warn('[cobalt] exited with code', code, '— restarting in 3s');
    setTimeout(startCobalt, 3000);
  });
  setTimeout(() => { cobaltReady = true; }, 3500);
}

// ==================== 2. ЗАПУСК TELEGRAM LOCAL BOT API (ДО 2000 МБ) ====================
function startLocalBotApi() {
  const bin = '/usr/local/bin/telegram-bot-api';
  if (!RAW_API_ID || !RAW_API_HASH) {
    localApiLastError = 'TELEGRAM_API_ID or TELEGRAM_API_HASH is empty';
    return;
  }
  if (!API_ID_VALID) {
    localApiLastError = `Invalid TELEGRAM_API_ID (${RAW_API_ID.slice(0, 4)}...): must be 5-12 digits from my.telegram.org`;
    console.error('[tgbotapi]', localApiLastError);
    return;
  }
  if (!API_HASH_VALID) {
    localApiLastError = `Invalid TELEGRAM_API_HASH (len=${RAW_API_HASH.length}): must be 32 hex chars (0-9, a-f) from my.telegram.org`;
    console.error('[tgbotapi]', localApiLastError);
    return;
  }
  if (!fs.existsSync(bin)) {
    localApiLastError = 'Binary /usr/local/bin/telegram-bot-api not found';
    return;
  }

  try {
    fs.mkdirSync('/tmp/tgbotapi', { recursive: true, mode: 0o755 });
    fs.mkdirSync('/tmp/tgbotapi-tmp', { recursive: true, mode: 0o755 });
  } catch (e) {}

  let exited = false;
  let firstErrorLine = '';
  const proc = spawn(bin, [
    '--local',
    '--http-port=' + LOCAL_API_PORT,
    '--api-id=' + RAW_API_ID,
    '--api-hash=' + RAW_API_HASH,
    '--dir=/tmp/tgbotapi',
    '--temp-dir=/tmp/tgbotapi-tmp'
  ], {
    env: {
      ...process.env,
      TELEGRAM_API_ID: RAW_API_ID,
      TELEGRAM_API_HASH: RAW_API_HASH
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  proc.stdout.on('data', d => process.stdout.write('[tgbotapi] ' + d.toString()));
  proc.stderr.on('data', d => {
    const s = d.toString();
    const lines = s.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    if (lines.length && !firstErrorLine) {
      firstErrorLine = lines[0];
    }
    localApiLastError = (firstErrorLine || s.trim()).slice(0, 240);
    process.stderr.write('[tgbotapi] ' + s);
  });
  proc.on('exit', code => {
    exited = true;
    localApiReady = false;
    console.warn('[tgbotapi] exited with code', code);
    if (code !== 0) {
      localApiLastError = `Exited with code ${code}: ${localApiLastError}`;
    }
  });
  setTimeout(() => {
    if (!exited) {
      localApiReady = true;
      localApiLastError = '';
      console.log('[tgbotapi] Running on port ' + LOCAL_API_PORT + ' (up to 2000 MB limit active)');
    }
  }, 2500);
}

// ==================== 3. ДОПОМІЖНІ ФУНКЦІЇ ====================
function tmpFile(ext) {
  return path.join('/tmp', 'media_' + Date.now() + '_' + crypto.randomBytes(4).toString('hex') + (ext.startsWith('.') ? ext : '.' + ext));
}

function safeUnlink(f) {
  if (!f) return;
  try { if (fs.existsSync(f)) fs.unlinkSync(f); } catch (e) {}
}

function runFfmpeg(args, timeoutMs = 120000) {
  return new Promise((resolve, reject) => {
    const p = spawn('ffmpeg', ['-hide_banner', '-loglevel', 'error', ...args]);
    let errOut = '';
    const timer = setTimeout(() => {
      try { p.kill('SIGKILL'); } catch (e) {}
      reject(new Error('FFMPEG_TIMEOUT'));
    }, timeoutMs);
    p.stderr.on('data', d => { errOut += d.toString().slice(-800); });
    p.on('error', err => { clearTimeout(timer); reject(err); });
    p.on('close', code => {
      clearTimeout(timer);
      if (code === 0) resolve(true);
      else reject(new Error('ffmpeg exit ' + code + ': ' + errOut.slice(-300)));
    });
  });
}

function runYtDlpToFile(sourceUrl, outPath, quality = '1080', dubLang = '', isAudio = false, timeoutMs = 45000) {
  return new Promise((resolve, reject) => {
    if (!ytDlpBin || !fs.existsSync(ytDlpBin)) return reject(new Error('NO_YTDLP'));
    const qNum = Math.min(1080, Number(quality === 'max' ? 1080 : quality) || 720);
    const dub = dubLang && dubLang !== 'orig' ? String(dubLang).split(/[-_]/)[0].toLowerCase() : '';
    let fmt = '';
    if (isAudio) {
      fmt = dub ? `bestaudio[language^=${dub}]/bestaudio/best` : 'bestaudio/best';
    } else if (dub) {
      fmt = `bestvideo[height<=${qNum}][ext=mp4]+bestaudio[language^=${dub}]/bestvideo[height<=${qNum}]+bestaudio[language^=${dub}]/bestvideo[height<=${qNum}][ext=mp4]+bestaudio[ext=m4a]/best[height<=${qNum}]/best`;
    } else {
      fmt = `bestvideo[height<=${qNum}][ext=mp4]+bestaudio[ext=m4a]/best[height<=${qNum}][ext=mp4]/best[height<=${qNum}]/best`;
    }
    const args = [
      '--no-playlist',
      '--no-warnings',
      '--geo-bypass',
      '--extractor-args', 'youtube:player_client=ios,android_vr,tv_embedded',
      '-f', fmt,
      ...(isAudio ? ['-x', '--audio-format', 'mp3'] : ['--merge-output-format', 'mp4']),
      '-o', outPath,
      sourceUrl
    ];
    const p = spawn(ytDlpBin, args);
    let errOut = '';
    const timer = setTimeout(() => {
      try { p.kill('SIGKILL'); } catch (e) {}
      reject(new Error('YTDLP_TIMEOUT'));
    }, timeoutMs);
    p.stderr.on('data', d => { errOut += d.toString().slice(-600); });
    p.on('error', err => { clearTimeout(timer); reject(err); });
    p.on('close', code => {
      clearTimeout(timer);
      if (code === 0 && fs.existsSync(outPath) && fs.statSync(outPath).size > 1000) {
        resolve(outPath);
      } else {
        reject(new Error('yt-dlp exit ' + code + ': ' + errOut.slice(-240)));
      }
    });
  });
}

function probeVideoDimensions(filePath) {
  return new Promise(resolve => {
    const p = spawn('ffmpeg', ['-hide_banner', '-i', filePath]);
    let out = '';
    p.stderr.on('data', d => { out += d.toString(); });
    p.on('close', () => {
      const m = out.match(/Stream #0:\d+.*Video:.*?\s(\d{2,5})x(\d{2,5})\b/);
      const dur = out.match(/Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/);
      const width = m ? Number(m[1]) : 1080;
      const height = m ? Number(m[2]) : 1920;
      const duration = dur ? (Number(dur[1]) * 3600 + Number(dur[2]) * 60 + parseFloat(dur[3])) : 3;
      resolve({ width, height, duration });
    });
    p.on('error', () => resolve({ width: 1080, height: 1920, duration: 3 }));
  });
}

function fetchWithTimeout(url, options = {}, timeoutMs = 6000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  return fetch(url, { ...options, signal: ctrl.signal }).finally(() => clearTimeout(timer));
}

const DUB_FLAGS = {
  uk: '🇺🇦 UK', ua: '🇺🇦 UK', en: '🇬🇧 EN', ru: 'RU', pl: '🇵🇱 PL', de: '🇩🇪 DE',
  es: '🇪🇸 ES', fr: '🇫🇷 FR', it: '🇮🇹 IT', pt: '🇧🇷 PT', tr: '🇹🇷 TR', ar: '🇸🇦 AR',
  hi: '🇮🇳 HI', id: '🇮🇩 ID', kk: '🇰🇿 KK', ja: '🇯🇵 JA', ko: '🇰🇷 KO', zh: '🇨🇳 ZH'
};

function normDubCode(rawIdOrLocale, rawName = '') {
  const s = String(rawIdOrLocale || '').toLowerCase().split(/[._-]/)[0];
  if (s && s.length >= 2 && s.length <= 3) return s === 'ua' ? 'uk' : s;
  const n = String(rawName || '').toLowerCase();
  if (/укр|ukrain/i.test(n)) return 'uk';
  if (/рус|russian/i.test(n)) return 'ru';
  if (/англ|english/i.test(n)) return 'en';
  if (/исп|ісп|spanish|español/i.test(n)) return 'es';
  if (/нем|нім|german|deutsch/i.test(n)) return 'de';
  if (/франц|french|français/i.test(n)) return 'fr';
  if (/итал|італ|italian/i.test(n)) return 'it';
  if (/порт|portug/i.test(n)) return 'pt';
  if (/тур|turk/i.test(n)) return 'tr';
  if (/араб|arab/i.test(n)) return 'ar';
  if (/хинд|хінд|hindi/i.test(n)) return 'hi';
  if (/польс|polish|polski/i.test(n)) return 'pl';
  if (/индон|індон|indones/i.test(n)) return 'id';
  if (/казах|kazakh/i.test(n)) return 'kk';
  return s || 'orig';
}

function formatDubLabel(code, rawName = '') {
  if (DUB_FLAGS[code]) return DUB_FLAGS[code];
  const clean = String(rawName || '').replace(/\s*\(.*?\)\s*/g, '').trim();
  return (clean || code).toUpperCase().slice(0, 10);
}

async function tgApiCall(botToken, method, payload, useLocal = false) {
  const base = (useLocal && localApiReady)
    ? `http://127.0.0.1:${LOCAL_API_PORT}/bot${botToken}/${method}`
    : `https://api.telegram.org/bot${botToken}/${method}`;
  const r = await fetch(base, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  return r.json().catch(() => null);
}

async function updateProgressMsg(botToken, chatId, msgId, pct, stage = 'Завантаження') {
  if (!botToken || !chatId || !msgId) return;
  const p = Math.max(1, Math.min(99, Math.round(pct)));
  const filled = Math.max(1, Math.min(10, Math.round(p / 10)));
  const bar = '█'.repeat(filled) + '░'.repeat(10 - filled);
  const text = `⏳ <b>${stage}</b> · <b>${p}%</b>\n<code>${bar}</code>`;
  await tgApiCall(botToken, 'editMessageText', {
    chat_id: chatId,
    message_id: msgId,
    text,
    parse_mode: 'HTML',
    link_preview_options: { is_disabled: true }
  }).catch(() => {});
}

async function downloadToFile(url, destPath, referer = '', onPct = null, customUa = '') {
  const headers = { 'User-Agent': customUa || UA, Accept: '*/*' };
  if (referer) headers.Referer = referer;
  const r = await fetch(url, { headers, redirect: 'follow' });
  if (!r.ok || !r.body) throw new Error('HTTP ' + r.status);
  const len = Number(r.headers.get('content-length') || 0);
  const fileStream = fs.createWriteStream(destPath);
  let downloaded = 0;
  let lastReport = 0;

  for await (const chunk of r.body) {
    downloaded += chunk.length;
    fileStream.write(chunk);
    if (onPct && len > 0 && Date.now() - lastReport > 900) {
      lastReport = Date.now();
      onPct(Math.min(85, Math.round(20 + (downloaded / len) * 65)));
    }
  }
  await new Promise((res, rej) => {
    fileStream.end();
    fileStream.on('finish', res);
    fileStream.on('error', rej);
  });
  if (downloaded < 256) throw new Error('EMPTY_FILE');
  return downloaded;
}

function extractYtId(url) {
  const m = String(url || '').match(/(?:youtube\.com\/(?:watch\?(?:[^\s]*&)?v=|shorts\/|embed\/|live\/|v\/|clip\/)|youtu\.be\/)([A-Za-z0-9_-]{6,})/i);
  return m ? m[1] : '';
}

// Актуальні YouTube Innertube клієнти 2026 року (IOS 20.10.4 / 21.02.35 на youtubei.googleapis.com без params + ANDROID_VR 1.62.27)
const YT_CLIENTS = [
  {
    n: 5,
    name: 'IOS',
    ver: '20.10.4',
    endpoint: 'https://youtubei.googleapis.com/youtubei/v1/player?prettyPrint=false',
    extra: { deviceMake: 'Apple', deviceModel: 'iPhone16,2', osName: 'iPhone', osVersion: '18.3.1.22D72', timeZone: 'UTC', utcOffsetMinutes: 0 },
    ua: 'com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 18_3_1 like Mac OS X)'
  },
  {
    n: 5,
    name: 'IOS',
    ver: '21.02.35',
    endpoint: 'https://youtubei.googleapis.com/youtubei/v1/player?prettyPrint=false',
    extra: { deviceMake: 'Apple', deviceModel: 'iPhone16,2', osName: 'iPhone', osVersion: '18.3.2.22D82', timeZone: 'UTC', utcOffsetMinutes: 0 },
    ua: 'com.google.ios.youtube/21.02.35 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X)'
  },
  {
    n: 28,
    name: 'ANDROID_VR',
    ver: '1.62.27',
    endpoint: 'https://www.youtube.com/youtubei/v1/player?prettyPrint=false',
    extra: { androidSdkVersion: 32, osName: 'Android', osVersion: '12L', deviceMake: 'Oculus', deviceModel: 'Quest 3', timeZone: 'UTC', utcOffsetMinutes: 0 },
    ua: 'com.google.android.apps.youtube.vr.oculus/1.62.27 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip'
  },
  {
    n: 85,
    name: 'TVHTML5_SIMPLY_EMBEDDED_PLAYER',
    ver: '2.0',
    endpoint: 'https://www.youtube.com/youtubei/v1/player?prettyPrint=false',
    extra: {},
    thirdParty: { embedUrl: 'https://www.youtube.com/' },
    ua: 'Mozilla/5.0 (PlayStation; PlayStation 5/2.26) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15'
  }
];

async function queryYtInnertubeOnRender(id) {
  const settled = await Promise.allSettled(YT_CLIENTS.map(async c => {
    const r = await fetchWithTimeout(c.endpoint || 'https://www.youtube.com/youtubei/v1/player?prettyPrint=false', {
      method: 'POST',
      headers: {
        'User-Agent': c.ua,
        'Content-Type': 'application/json',
        'X-YouTube-Client-Name': String(c.n),
        'X-YouTube-Client-Version': c.ver,
        Origin: 'https://www.youtube.com'
      },
      body: JSON.stringify({
        videoId: id,
        contentCheckOk: true,
        racyCheckOk: true,
        context: {
          client: { clientName: c.name, clientVersion: c.ver, hl: 'uk', gl: 'UA', ...c.extra },
          ...(c.thirdParty ? { thirdParty: c.thirdParty } : {})
        }
      })
    }, 5500);
    const j = await r.json().catch(() => null);
    if (!j || !j.playabilityStatus || j.playabilityStatus.status !== 'OK') throw new Error('NOT_OK');
    return { client: c, data: j };
  }));

  const okList = settled.filter(s => s.status === 'fulfilled' && s.value).map(s => s.value);
  if (!okList.length) return null;

  const audioTracksMap = new Map();
  const vStreams = [];
  const progStreams = [];
  let hlsUrl = '';
  let hlsUa = UA;
  let title = '';
  let author = '';

  for (const { client: c, data: j } of okList) {
    const vd = j.videoDetails || {};
    if (!title && vd.title) title = String(vd.title);
    if (!author && vd.author) author = String(vd.author);
    const sd = j.streamingData || {};
    if (!hlsUrl && sd.hlsManifestUrl) {
      hlsUrl = sd.hlsManifestUrl;
      hlsUa = c.ua;
    }

    for (const f of (Array.isArray(sd.adaptiveFormats) ? sd.adaptiveFormats : [])) {
      if (!f || !f.url) continue;
      const mime = String(f.mimeType || '');
      if (/audio\/mp4/i.test(mime)) {
        const at = f.audioTrack || {};
        const code = normDubCode(at.id, at.displayName);
        const br = Number(f.bitrate || 0);
        const prev = audioTracksMap.get(code);
        if (!prev || br > prev.bitrate) {
          audioTracksMap.set(code, {
            lang: code,
            label: formatDubLabel(code, at.displayName),
            url: f.url,
            customUa: c.ua,
            bitrate: br,
            size: Number(f.contentLength) || 0,
            isDefault: !!at.audioIsDefault
          });
        }
      } else if (/video\/mp4.*avc1/i.test(mime)) {
        const q = Number((String(f.qualityLabel || '').match(/(\d{3,4})/) || [])[1] || f.height || 0);
        if (q > 0 && !vStreams.some(x => x.q === q)) {
          vStreams.push({ url: f.url, q, customUa: c.ua, size: Number(f.contentLength) || 0 });
        }
      }
    }

    for (const f of (Array.isArray(sd.formats) ? sd.formats : [])) {
      if (!f || !f.url || !/video\/mp4/i.test(String(f.mimeType || ''))) continue;
      const q = Number((String(f.qualityLabel || '').match(/(\d{3,4})/) || [])[1] || f.height || 0);
      const at = f.audioTrack || {};
      const dubCode = at.id ? normDubCode(at.id, at.displayName) : '';
      if (q > 0 && !progStreams.some(x => x.q === q && x.dubLang === dubCode)) {
        progStreams.push({ url: f.url, q, dubLang: dubCode, customUa: c.ua, size: Number(f.contentLength) || 0 });
      }
    }
  }

  // Якщо IOS клієнт повернув HLS-маніфест — парсимо #EXT-X-MEDIA:TYPE=AUDIO для отримання всіх доріжок озвучки!
  if (hlsUrl && audioTracksMap.size <= 1) {
    try {
      const mh = await fetchWithTimeout(hlsUrl, { headers: { 'User-Agent': hlsUa || UA } }, 4000);
      if (mh.ok) {
        const m3u8 = await mh.text();
        for (const line of m3u8.split(/\r?\n/)) {
          if (!line.startsWith('#EXT-X-MEDIA:') || !/TYPE=AUDIO/i.test(line)) continue;
          const langM = line.match(/LANGUAGE="([^"]+)"/i);
          const nameM = line.match(/NAME="([^"]+)"/i);
          const uriM = line.match(/URI="([^"]+)"/i);
          const defM = /DEFAULT=YES/i.test(line);
          if (uriM && uriM[1]) {
            const code = normDubCode(langM ? langM[1] : '', nameM ? nameM[1] : '');
            if (!audioTracksMap.has(code)) {
              audioTracksMap.set(code, {
                lang: code,
                label: formatDubLabel(code, nameM ? nameM[1] : ''),
                url: uriM[1],
                customUa: hlsUa || UA,
                bitrate: 128000,
                size: 0,
                isDefault: defM,
                isHls: true
              });
            }
          }
        }
      }
    } catch (e) {}
  }

  return {
    audioTracks: [...audioTracksMap.values()],
    vStreams,
    progStreams,
    hlsUrl,
    hlsUa,
    title,
    author,
    thumbnail: `https://i.ytimg.com/vi/${id}/hqdefault.jpg`
  };
}

async function resolveTikTokOnRender(sourceUrl, isAudio = false) {
  // 1) TikWM API (прямий CDN d.play без водяного знаку)
  try {
    const body = new URLSearchParams({ url: sourceUrl, hd: '1' }).toString();
    const r = await fetchWithTimeout('https://www.tikwm.com/api/', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'User-Agent': UA,
        Accept: 'application/json',
        Referer: 'https://www.tikwm.com/'
      },
      body
    }, 6000);
    const j = await r.json().catch(() => null);
    const d = j && j.code === 0 && j.data;
    if (d) {
      const fixUrl = u => (!u ? '' : u.startsWith('http') ? u : 'https://www.tikwm.com' + u);
      if (isAudio && (d.music || (d.music_info && d.music_info.play))) {
        return { url: fixUrl(d.music || d.music_info.play), audioUrl: '', customUa: UA, referer: 'https://www.tiktok.com/' };
      }
      const vUrl = fixUrl(d.play) || fixUrl(d.hdplay);
      if (vUrl) {
        return { url: vUrl, audioUrl: '', customUa: UA, referer: 'https://www.tiktok.com/' };
      }
    }
  } catch (e) {}

  // 2) Tiklydown API
  try {
    const r = await fetchWithTimeout('https://api.tiklydown.eu.org/api/download?url=' + encodeURIComponent(sourceUrl), {
      headers: { 'User-Agent': UA, Accept: 'application/json' }
    }, 5500);
    const j = await r.json().catch(() => null);
    if (j && j.video) {
      const vUrl = j.video.noWatermark || j.video.hd;
      if (vUrl) return { url: vUrl, audioUrl: '', customUa: UA, referer: 'https://www.tiktok.com/' };
    }
  } catch (e) {}

  return null;
}

async function resolveYouTubeOnRender(sourceUrl, quality = '1080', dubLang = '', isAudio = false) {
  const id = extractYtId(sourceUrl);
  if (!id) throw new Error('NO_YT_ID');
  const qNum = Math.min(1080, Number(quality === 'max' ? 1080 : quality) || 720);
  const canonUrl = 'https://www.youtube.com/watch?v=' + id;
  const targetDub = dubLang ? normDubCode(dubLang, dubLang) : '';

  // 1) YouTube Innertube напряму з IP Render (IOS 20.10.4 + ANDROID_VR 1.62.27 + TVHTML5) з підтримкою всіх доріжок озвучки!
  try {
    const inner = await queryYtInnertubeOnRender(id);
    if (inner) {
      const tracks = inner.audioTracks || [];
      let chosenAudio = null;
      if (targetDub && targetDub !== 'orig') {
        chosenAudio = tracks.find(t => t.lang === targetDub);
      }
      if (!chosenAudio) {
        chosenAudio = tracks.find(t => t.isDefault) || tracks.sort((a, b) => b.bitrate - a.bitrate)[0];
      }

      if (isAudio && chosenAudio && chosenAudio.url) {
        return { url: chosenAudio.url, audioUrl: '', customUa: chosenAudio.customUa || UA, referer: 'https://www.youtube.com/' };
      }

      const exactAdapt = inner.vStreams.find(v => v.q === qNum) || inner.vStreams.filter(v => v.q <= qNum).sort((a, b) => b.q - a.q)[0];
      if (exactAdapt && chosenAudio && chosenAudio.url) {
        return {
          url: exactAdapt.url,
          audioUrl: chosenAudio.url,
          customUa: exactAdapt.customUa || chosenAudio.customUa || UA,
          audioUa: chosenAudio.customUa || exactAdapt.customUa || UA,
          referer: 'https://www.youtube.com/'
        };
      }

      if (!targetDub || tracks.length <= 1) {
        const bestProg = inner.progStreams.find(v => v.q === qNum) || inner.progStreams.sort((a, b) => b.q - a.q)[0];
        if (bestProg && bestProg.url) {
          return { url: bestProg.url, audioUrl: '', customUa: bestProg.customUa || UA, referer: 'https://www.youtube.com/' };
        }
      }

      if (inner.hlsUrl) {
        return { url: inner.hlsUrl, audioUrl: '', customUa: inner.hlsUa || UA, referer: 'https://www.youtube.com/' };
      }
    }
  } catch (e) {}

  // 2) Внутрішній Cobalt v10 на Render (http://127.0.0.1:9000/)
  if (cobaltReady) {
    try {
      const rCob = await fetchWithTimeout(`http://127.0.0.1:${COBALT_PORT}/`, {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'User-Agent': UA },
        body: JSON.stringify({
          url: canonUrl,
          videoQuality: String(qNum),
          downloadMode: isAudio ? 'audio' : 'auto',
          youtubeVideoCodec: 'h264',
          youtubeVideoContainer: 'mp4',
          filenameStyle: 'basic',
          ...(targetDub && targetDub !== 'orig' ? { youtubeDubLang: targetDub } : {})
        })
      }, 6500);
      const jCob = await rCob.json().catch(() => null);
      if (jCob && jCob.status !== 'error') {
        if (jCob.status === 'local-processing' && Array.isArray(jCob.tunnel) && jCob.tunnel.length) {
          return {
            url: jCob.tunnel[0].replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`),
            audioUrl: (jCob.tunnel[1] || '').replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`),
            customUa: UA
          };
        }
        const uCob = typeof jCob.url === 'string' ? jCob.url : (typeof jCob.tunnel === 'string' ? jCob.tunnel : '');
        if (uCob) {
          return { url: uCob.replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`), audioUrl: '', customUa: UA };
        }
      }
    } catch (e) {}
  }

  // 3) OceanSaver + Loader.to API паралельно (підтримує audio_language для дубляжу!)
  const oceanBases = ['https://p.oceansaver.in', 'https://loader.to'];
  for (const base of oceanBases) {
    try {
      const fmt = isAudio ? 'mp3' : String(qNum);
      const dubParam = targetDub && targetDub !== 'orig' ? `&audio_language=${encodeURIComponent(targetDub)}` : '';
      const initUrl = `${base}/ajax/download.php?copyright=0&button=1&start=1&end=1&format=${fmt}${dubParam}&api=dfcb6d76f2f6a9894gjkege8a4ab232222&iframe_source=direct&url=${encodeURIComponent(canonUrl)}`;
      const r = await fetchWithTimeout(initUrl, { headers: { 'User-Agent': UA, Referer: base + '/' } }, 6000);
      const j = await r.json().catch(() => null);
      if (j && j.id) {
        const pollEndpoint = j.progress_url || `${base}/ajax/progress.php?id=${encodeURIComponent(j.id)}`;
        for (let step = 0; step < 15; step++) {
          await new Promise(res => setTimeout(res, 900));
          const pr = await fetchWithTimeout(pollEndpoint, {
            headers: { 'User-Agent': UA, Referer: base + '/' }
          }, 4500);
          const pj = await pr.json().catch(() => null);
          if (pj && pj.download_url && /^https?:\/\//i.test(pj.download_url)) {
            return { url: pj.download_url, audioUrl: '', customUa: UA, referer: base + '/' };
          }
          if (pj && (pj.success === 1 || pj.progress >= 1000) && !pj.download_url) break;
        }
      }
    } catch (e) {}
  }

  // 4) SaveTube CDN API
  const stHosts = ['https://cdn59.savetube.su', 'https://cdn54.savetube.me', 'https://cdn61.savetube.su', 'https://cdn51.savetube.su'];
  for (const base of stHosts) {
    try {
      const r = await fetchWithTimeout(`${base}/info?url=${encodeURIComponent(canonUrl)}`, {
        headers: { 'User-Agent': UA, Accept: 'application/json', Origin: 'https://yt.savetube.me', Referer: 'https://yt.savetube.me/' }
      }, 4500);
      if (!r.ok) continue;
      const j = await r.json().catch(() => null);
      const d = j && j.data;
      if (!d) continue;
      if (isAudio) {
        const a = (Array.isArray(d.audio_formats) ? d.audio_formats : []).find(x => x && x.url);
        if (a && a.url) return { url: a.url, audioUrl: '', customUa: UA, referer: 'https://yt.savetube.me/' };
      }
      const vids = (Array.isArray(d.video_formats) ? d.video_formats : []).filter(v => v && v.url && !(/googlevideo\.com/i.test(v.url) && /[?&]ip=/i.test(v.url)));
      const pick = vids.find(v => Number(v.height || v.quality) === qNum) || vids[0];
      if (pick && pick.url) return { url: pick.url, audioUrl: '', customUa: UA, referer: 'https://yt.savetube.me/' };
    } catch (e) {}
  }

  // 5) Invidious API (local=true proxy)
  const invHosts = ['https://inv.nadeko.net', 'https://invidious.nerdvpn.de', 'https://invidious.jing.rocks', 'https://yewtu.be'];
  for (const base of invHosts) {
    try {
      const r = await fetchWithTimeout(`${base}/api/v1/videos/${encodeURIComponent(id)}`, { headers: { 'User-Agent': UA, Accept: 'application/json' } }, 4500);
      if (!r.ok) continue;
      const j = await r.json().catch(() => null);
      const streams = (j && Array.isArray(j.formatStreams) ? j.formatStreams : []).filter(f => f && f.itag);
      if (streams.length) {
        const pick = streams.find(f => String(f.qualityLabel || '').includes(String(qNum))) || streams[0];
        return {
          url: `${base}/latest_version?id=${encodeURIComponent(id)}&itag=${pick.itag}&local=true`,
          audioUrl: '',
          customUa: UA,
          referer: base + '/'
        };
      }
    } catch (e) {}
  }

  throw new Error('YOUTUBE_ALL_ENGINES_FAILED');
}

async function callInternalCobalt(sourceUrl, quality = 'max', dubLang = '', isAudio = false) {
  const isYt = /youtube\.com|youtu\.be/i.test(String(sourceUrl));
  if (isYt) {
    return resolveYouTubeOnRender(sourceUrl, quality, dubLang, isAudio);
  }
  const isTt = /tiktok\.com/i.test(String(sourceUrl));
  if (isTt) {
    const ttDirect = await resolveTikTokOnRender(sourceUrl, isAudio);
    if (ttDirect && ttDirect.url) return ttDirect;
  }

  const r = await fetch(`http://127.0.0.1:${COBALT_PORT}/`, {
    method: 'POST',
    headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'User-Agent': UA },
    body: JSON.stringify({
      url: sourceUrl,
      videoQuality: String(quality || 'max'),
      downloadMode: isAudio ? 'audio' : 'auto',
      filenameStyle: 'basic',
      allowH265: true,
      ...(isAudio ? { tiktokFullAudio: true } : {})
    })
  });
  const j = await r.json().catch(() => null);
  if (!j || j.status === 'error') throw new Error('COBALT_ERR');
  if (j.status === 'local-processing' && Array.isArray(j.tunnel) && j.tunnel.length) {
    return {
      url: j.tunnel[0].replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`),
      audioUrl: (j.tunnel[1] || '').replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`),
      customUa: UA
    };
  }
  const u = typeof j.url === 'string' ? j.url : (typeof j.tunnel === 'string' ? j.tunnel : '');
  if (!u) throw new Error('NO_COBALT_URL');
  return { url: u.replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`), audioUrl: '', customUa: UA };
}

async function uploadMultipartToTelegram({
  botToken, chatId, mode, filePath, fileNameOverride = '', caption, replyMarkup, replyToMsgId, businessConnectionId, useLocalApi
}) {
  const stat = fs.statSync(filePath);
  const method = mode === 'document' ? 'sendDocument' : mode === 'audio' ? 'sendAudio' : mode === 'video_note' ? 'sendVideoNote' : 'sendVideo';
  const field = mode === 'document' ? 'document' : mode === 'audio' ? 'audio' : mode === 'video_note' ? 'video_note' : 'video';
  const fileName = fileNameOverride || (mode === 'audio' ? 'audio.mp3' : mode === 'video_note' ? 'circle.mp4' : 'video.mp4');
  const mime = mode === 'audio' ? 'audio/mpeg' : 'video/mp4';

  const buildMultipartForm = () => {
    const form = new FormData();
    form.append('chat_id', String(chatId));
    if (caption && mode !== 'video_note') {
      form.append('caption', String(caption));
      form.append('parse_mode', 'HTML');
    }
    if (mode === 'video') {
      form.append('supports_streaming', 'true');
      form.append('show_caption_above_media', 'true');
    }
    if (mode === 'video_note') {
      form.append('length', '384');
    }
    if (replyToMsgId) {
      form.append('reply_parameters', JSON.stringify({ message_id: Number(replyToMsgId), allow_sending_without_reply: true }));
    }
    if (businessConnectionId) {
      form.append('business_connection_id', String(businessConnectionId));
    }
    if (replyMarkup) {
      form.append('reply_markup', typeof replyMarkup === 'string' ? replyMarkup : JSON.stringify(replyMarkup));
    }
    const fileBuf = fs.readFileSync(filePath);
    form.append(field, new Blob([fileBuf], { type: mime }), fileName);
    return form;
  };

  // А) Якщо файл <= 49.2 МБ — спершу відправляємо на api.telegram.org (щоб file_id гарантовано кешувався в основній сесії бота!)
  if (stat.size <= STD_TG_LIMIT) {
    try {
      const rStd = await fetch(`https://api.telegram.org/bot${botToken}/${method}`, {
        method: 'POST',
        body: buildMultipartForm()
      });
      const jStd = await rStd.json().catch(() => null);
      if (jStd && jStd.ok && jStd.result) return jStd.result;
    } catch (e) {}
  }

  // Б) Якщо файл > 49.2 МБ (до 2000 МБ!) або api.telegram.org не прийняв — відправляємо через Local Bot API (127.0.0.1:8081)!
  if ((useLocalApi || stat.size > STD_TG_LIMIT) && localApiReady) {
    try {
      const payload = {
        chat_id: chatId,
        [field]: 'file://' + filePath,
        ...(caption && mode !== 'video_note' ? { caption, parse_mode: 'HTML' } : {}),
        ...(mode === 'video' ? { supports_streaming: true, show_caption_above_media: true } : {}),
        ...(mode === 'video_note' ? { length: 384 } : {}),
        ...(replyToMsgId ? { reply_parameters: { message_id: Number(replyToMsgId), allow_sending_without_reply: true } } : {}),
        ...(businessConnectionId ? { business_connection_id: businessConnectionId } : {}),
        ...(replyMarkup ? { reply_markup: replyMarkup } : {})
      };
      const jLocal = await tgApiCall(botToken, method, payload, true);
      if (jLocal && jLocal.ok && jLocal.result) return jLocal.result;
    } catch (e) {}

    try {
      const rLoc = await fetch(`http://127.0.0.1:${LOCAL_API_PORT}/bot${botToken}/${method}`, {
        method: 'POST',
        body: buildMultipartForm()
      });
      const jLoc = await rLoc.json().catch(() => null);
      if (jLoc && jLoc.ok && jLoc.result) return jLoc.result;
    } catch (e) {}
  }

  throw new Error(stat.size > STD_TG_LIMIT ? 'FILE_OVER_50MB_NEED_FIT' : ('TG_UPLOAD_FAILED_SIZE_' + stat.size));
}

// ==================== 4. ОБРОБНИК /send (ВІДЕО >20 МБ ТА ДО 2 ГБ, СКЛЕЙКА 1080p, YOUTUBE МУЛЬТИ-РУШІЙ + YT-DLP) ====================
async function handleSendMedia(body) {
  const {
    botToken, chatId, mode = 'video', url, audioUrl = '', sourceUrl = '',
    quality = '1080', dubLang = '', customUa = '', tunnelUsed = false,
    referer = '', caption = '', replyMarkup = null, replyToMsgId = null, replyToMessageId = null,
    businessConnectionId = null, progressMsgId = null
  } = body || {};
  const rplId = replyToMsgId || replyToMessageId || null;

  if (!botToken || !chatId || (!url && !sourceUrl)) {
    throw new Error('MISSING_PARAMS');
  }

  let targetUrl = String(url || '').replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`);
  let targetAudio = String(audioUrl || '').replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`);
  let activeUa = customUa || UA;
  let activeRef = referer || '';

  const filesToClean = [];
  try {
    await updateProgressMsg(botToken, chatId, progressMsgId, 30, 'Завантаження');

    let finalPath = tmpFile(mode === 'audio' ? '.mp3' : '.mp4');
    filesToClean.push(finalPath);

    const downloadAndMux = async (vUrl, aUrl, uaToUse, refToUse, audioUaToUse = '') => {
      const isHls = /\.m3u8(\?|$)|\/manifest\/hls/i.test(vUrl);
      if (isHls) {
        if (aUrl) {
          await runFfmpeg([
            '-user_agent', uaToUse || UA,
            ...(refToUse ? ['-headers', `Referer: ${refToUse}\r\n`] : []),
            '-i', vUrl,
            '-user_agent', audioUaToUse || uaToUse || UA,
            '-i', aUrl,
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-map', '0:v:0',
            '-map', '1:a:0',
            '-shortest',
            '-movflags', '+faststart',
            '-y', finalPath
          ]);
        } else {
          await runFfmpeg([
            '-user_agent', uaToUse || UA,
            ...(refToUse ? ['-headers', `Referer: ${refToUse}\r\n`] : []),
            '-i', vUrl,
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            '-movflags', '+faststart',
            '-y', finalPath
          ]);
        }
      } else if (aUrl) {
        const isAudioHls = /\.m3u8(\?|$)|\/manifest\/hls/i.test(aUrl);
        if (isAudioHls) {
          const vPath = tmpFile('_v.mp4');
          filesToClean.push(vPath);
          await downloadToFile(vUrl, vPath, refToUse, p => updateProgressMsg(botToken, chatId, progressMsgId, p, 'Завантаження відео'), uaToUse);
          await updateProgressMsg(botToken, chatId, progressMsgId, 88, 'Збірка MP4');
          await runFfmpeg([
            '-threads', '0',
            '-i', vPath,
            '-user_agent', audioUaToUse || uaToUse || UA,
            '-i', aUrl,
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-map', '0:v:0',
            '-map', '1:a:0',
            '-shortest',
            '-movflags', '+faststart',
            '-y', finalPath
          ]);
        } else {
          const vPath = tmpFile('_v.mp4');
          const aPath = tmpFile('_a.m4a');
          filesToClean.push(vPath, aPath);
          await Promise.all([
            downloadToFile(vUrl, vPath, refToUse, p => updateProgressMsg(botToken, chatId, progressMsgId, p, 'Завантаження відео'), uaToUse),
            downloadToFile(aUrl, aPath, refToUse, null, audioUaToUse || uaToUse)
          ]);
          await updateProgressMsg(botToken, chatId, progressMsgId, 88, 'Збірка MP4');
          await runFfmpeg([
            '-threads', '0',
            '-i', vPath,
            '-i', aPath,
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-map', '0:v:0',
            '-map', '1:a:0',
            '-shortest',
            '-movflags', '+faststart',
            '-y', finalPath
          ]);
        }
      } else {
        await downloadToFile(vUrl, finalPath, refToUse, p => updateProgressMsg(botToken, chatId, progressMsgId, p, 'Завантаження'), uaToUse);
      }
    };

    let downloadedOk = false;

    // 1) Якщо передано вже готовий прямий потік (і не одноразовий використаний тунель) — пробуємо його
    if (targetUrl && !tunnelUsed) {
      try {
        await downloadAndMux(targetUrl, targetAudio, activeUa, activeRef);
        downloadedOk = true;
      } catch (e) {
        safeUnlink(finalPath);
      }
    }

    // 2) Якщо є sourceUrl і це YouTube/TikTok або перша спроба не вдалася — резолвимо свіжий потік на Render
    if (!downloadedOk && sourceUrl) {
      try {
        const fresh = await callInternalCobalt(sourceUrl, quality || '1080', dubLang, mode === 'audio');
        if (fresh && fresh.url) {
          await downloadAndMux(
            fresh.url,
            fresh.audioUrl || '',
            fresh.customUa || activeUa,
            fresh.referer || activeRef,
            fresh.audioUa || fresh.customUa || activeUa
          );
          downloadedOk = true;
        }
      } catch (e) {
        safeUnlink(finalPath);
      }
    }

    // 3) Залізобетонний резерв через yt-dlp (якщо доступний бінарник yt-dlp)
    if (!downloadedOk && sourceUrl && ytDlpBin) {
      await runYtDlpToFile(sourceUrl, finalPath, quality || '1080', dubLang, mode === 'audio');
      downloadedOk = true;
    }

    if (!downloadedOk || !fs.existsSync(finalPath)) {
      throw new Error('DOWNLOAD_ALL_METHODS_FAILED');
    }

    await updateProgressMsg(botToken, chatId, progressMsgId, 95, 'Відправка в Telegram');

    try {
      const result = await uploadMultipartToTelegram({
        botToken,
        chatId,
        mode,
        filePath: finalPath,
        caption,
        replyMarkup,
        replyToMsgId: rplId,
        businessConnectionId,
        useLocalApi: localApiReady
      });
      return { ok: true, result };
    } catch (uploadErr) {
      const stat = fs.statSync(finalPath);
      if (stat.size <= STD_TG_LIMIT) throw uploadErr;

      await updateProgressMsg(botToken, chatId, progressMsgId, 91, 'Оптимізація розміру (>50 МБ)');
      const compPath = tmpFile('_fit.mp4');
      filesToClean.push(compPath);
      await runFfmpeg([
        '-threads', '0',
        '-i', finalPath,
        '-vf', "scale='min(1280,iw)':-2",
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-crf', '27',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-fs', String(STD_TG_LIMIT),
        '-movflags', '+faststart',
        '-y', compPath
      ], 110000);

      const result = await uploadMultipartToTelegram({
        botToken,
        chatId,
        mode,
        filePath: compPath,
        caption,
        replyMarkup,
        replyToMsgId: rplId,
        businessConnectionId,
        useLocalApi: false
      });
      return { ok: true, result };
    }
  } finally {
    filesToClean.forEach(safeUnlink);
  }
}

// ==================== 5. ОБРОБНИК /circle (НАДШВИДКІ КРУЖЕЧКИ ЗА 1-2 СЕКУНДИ ЧЕРЕЗ FFMPEG) ====================
async function handleCircle(body) {
  const {
    botToken, chatId, fileId = '', url = '', replyToMsgId = null,
    businessConnectionId = null, replyMarkup = null, progressMsgId = null
  } = body || {};

  if (!botToken || !chatId || (!fileId && !url)) {
    throw new Error('MISSING_CIRCLE_PARAMS');
  }

  const inPath = tmpFile('_in.mp4');
  const outPath = tmpFile('_circle.mp4');
  let inputFileToUse = inPath;
  try {
    let dlUrl = url;
    if (!dlUrl && fileId) {
      let gf = await tgApiCall(botToken, 'getFile', { file_id: fileId }, false);
      let fp = gf && gf.ok && gf.result && gf.result.file_path;
      if (fp) {
        dlUrl = `https://api.telegram.org/file/bot${botToken}/${fp}`;
      } else if (localApiReady) {
        gf = await tgApiCall(botToken, 'getFile', { file_id: fileId }, true);
        fp = gf && gf.ok && gf.result && gf.result.file_path;
        if (fp && fs.existsSync(fp)) {
          inputFileToUse = fp;
        } else if (fp) {
          dlUrl = `http://127.0.0.1:${LOCAL_API_PORT}/file/bot${botToken}/${fp.replace(/^\/+/, '')}`;
        }
      }
      if (!dlUrl && inputFileToUse === inPath) throw new Error('GET_FILE_FAILED');
    }

    if (inputFileToUse === inPath) {
      await updateProgressMsg(botToken, chatId, progressMsgId, 35, 'Завантаження відео');
      await downloadToFile(dlUrl, inPath);
    }

    await updateProgressMsg(botToken, chatId, progressMsgId, 68, 'Обрізка у кружечок (FFmpeg)');
    await runFfmpeg([
      '-threads', '0',
      '-t', '60',
      '-i', inputFileToUse,
      '-vf', "crop='min(iw,ih)':'min(iw,ih)',scale=384:384,setsar=1",
      '-r', '30',
      '-c:v', 'libx264',
      '-preset', 'ultrafast',
      '-tune', 'zerolatency',
      '-profile:v', 'baseline',
      '-level', '3.0',
      '-pix_fmt', 'yuv420p',
      '-crf', '25',
      '-c:a', 'aac',
      '-ac', '1',
      '-ar', '44100',
      '-b:a', '96k',
      '-movflags', '+faststart',
      '-y', outPath
    ], 45000);

    await updateProgressMsg(botToken, chatId, progressMsgId, 94, 'Відправка кружечка');
    const result = await uploadMultipartToTelegram({
      botToken,
      chatId,
      mode: 'video_note',
      filePath: outPath,
      replyMarkup,
      replyToMsgId,
      businessConnectionId,
      useLocalApi: false
    });
    return { ok: true, result };
  } finally {
    safeUnlink(inPath);
    safeUnlink(outPath);
  }
}

// ==================== 6. НАРІЗКА ВІДЕО НА КВАДРАТИКИ ДЛЯ СТІКЕРІВ ПРОФІЛЮ (/stickers) ====================
async function handleStickersGrid(body) {
  const {
    botToken, chatId, url = '', sourceUrl = '', fileId = '', cols = 3, rows = 3,
    replyToMsgId = null, progressMsgId = null
  } = body || {};

  if (!botToken || !chatId || (!url && !sourceUrl && !fileId)) {
    throw new Error('MISSING_STICKERS_PARAMS');
  }

  const gridCols = Math.max(1, Math.min(5, Number(cols) || 3));
  const gridRows = Math.max(1, Math.min(5, Number(rows) || 3));
  const filesToClean = [];

  try {
    const inPath = tmpFile('_grid_in.mp4');
    filesToClean.push(inPath);

    let dlUrl = String(url || '').replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`);
    let dlReferer = /tiktok/i.test(dlUrl || sourceUrl) ? 'https://www.tiktok.com/' : '';
    if (!dlUrl && fileId) {
      const gf = await tgApiCall(botToken, 'getFile', { file_id: fileId });
      const fp = gf && gf.ok && gf.result && gf.result.file_path;
      if (!fp) throw new Error('GET_FILE_FAILED');
      dlUrl = `https://api.telegram.org/file/bot${botToken}/${fp}`;
    }
    if (!dlUrl && sourceUrl) {
      const c = await callInternalCobalt(sourceUrl, 'max');
      dlUrl = c.url;
      if (c.referer) dlReferer = c.referer;
    }

    await updateProgressMsg(botToken, chatId, progressMsgId, 25, 'Завантаження оригіналу');
    try {
      await downloadToFile(dlUrl, inPath, dlReferer);
    } catch (dlErr) {
      if (sourceUrl) {
        const c2 = await callInternalCobalt(sourceUrl, 'max');
        await downloadToFile(c2.url, inPath, c2.referer || dlReferer);
      } else {
        throw dlErr;
      }
    }

    const { width, height, duration } = await probeVideoDimensions(inPath);
    const maxTileW = Math.floor(width / gridCols);
    const maxTileH = Math.floor(height / gridRows);
    const tileSize = Math.max(64, Math.min(maxTileW, maxTileH));
    const gridW = tileSize * gridCols;
    const gridH = tileSize * gridRows;
    const startX = Math.max(0, Math.floor((width - gridW) / 2));
    const startY = Math.max(0, Math.floor((height - gridH) / 2));
    const inset = Math.max(2, Math.round(tileSize * 0.02));
    const cropSize = Math.max(32, tileSize - inset * 2);
    const clipDur = Math.min(duration || 3, 2.95).toFixed(2);

    const totalTiles = gridCols * gridRows;
    const outPaths = [];

    for (let r = 0; r < gridRows; r++) {
      for (let c = 0; c < gridCols; c++) {
        const idx = r * gridCols + c + 1;
        const pct = Math.round(30 + (idx / totalTiles) * 55);
        await updateProgressMsg(botToken, chatId, progressMsgId, pct, `Нарізка квадратика ${idx}/${totalTiles}`);
        const cellX = startX + c * tileSize + inset;
        const cellY = startY + r * tileSize + inset;
        const tilePath = tmpFile(`_sticker_${idx}.mp4`);
        filesToClean.push(tilePath);

        await runFfmpeg([
          '-i', inPath,
          '-t', clipDur,
          '-vf', `crop=${cropSize}:${cropSize}:${cellX}:${cellY},scale=512:512,setsar=1`,
          '-c:v', 'libx264',
          '-preset', 'fast',
          '-crf', '18',
          '-pix_fmt', 'yuv420p',
          '-an',
          '-movflags', '+faststart',
          '-y', tilePath
        ], 45000);

        outPaths.push({ path: tilePath, name: `sticker_${r + 1}_${c + 1}_(${idx}).mp4` });
      }
    }

    await updateProgressMsg(botToken, chatId, progressMsgId, 92, 'Відправка готових файлів');
    const sentResults = [];
    for (let i = 0; i < outPaths.length; i++) {
      const item = outPaths[i];
      const cap = i === 0 ? `🧩 <b>Нарізка ${gridCols}×${gridRows} (${outPaths.length} ч. · 512×512 без втрати якості)</b>` : '';
      const sent = await uploadMultipartToTelegram({
        botToken,
        chatId,
        mode: 'document',
        filePath: item.path,
        fileNameOverride: item.name,
        caption: cap,
        replyToMsgId: i === 0 ? replyToMsgId : null,
        useLocalApi: false
      });
      if (sent) sentResults.push(sent);
    }

    return { ok: true, count: sentResults.length, result: sentResults[0] || true };
  } finally {
    filesToClean.forEach(safeUnlink);
  }
}

// ==================== 7. АВТОПАРСЕР X (TWITTER) ДЛЯ ФУТБОЛЬНИХ ГОЛІВ (КОЖНІ 15 СЕК + КЕШ НА ДИСК ТА БД) ====================
const XWATCH_STATE_FILE = '/tmp/xwatch-state.json';

function parseXUsername(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  const m = s.match(/(?:https?:\/\/)?(?:www\.|mobile\.)?(?:x|twitter|fxtwitter|vxtwitter|fixupx|xcancel|nitter\.[^\/]+)\.com\/([A-Za-z0-9_]{1,30})/i) ||
            s.match(/(?:https?:\/\/)?nitter\.[^\/]+\/([A-Za-z0-9_]{1,30})/i);
  if (m && m[1] && !/^(?:home|explore|search|i|intent|share|hashtag|status)$/i.test(m[1])) {
    return m[1];
  }
  return s.replace(/^@+/, '').split(/[\/?#\s]/)[0].trim();
}

function parseTelegramChatInput(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  const m = s.match(/(?:https?:\/\/)?(?:www\.)?(?:t\.me|telegram\.me)\/(?:s\/)?([A-Za-z0-9_]{4,64})/i);
  if (m && m[1] && !/^(?:joinchat|addstickers|share|iv|c)$/i.test(m[1])) {
    return '@' + m[1];
  }
  if (/^-?\d{6,20}$/.test(s)) return s;
  if (s.startsWith('@')) return s.split(/[\/?#\s]/)[0];
  if (/^[A-Za-z0-9_]{4,64}$/.test(s)) return '@' + s;
  return s;
}

const xWatchConfig = {
  enabled: process.env.X_ENABLED !== '0',
  accounts: String(process.env.X_ACCOUNTS || 'GoalsXtra,TheGoalsZone').split(',').map(parseXUsername).filter(Boolean),
  channels: [parseTelegramChatInput(process.env.X_TARGET_CHAT || '@Fantasyargoal')].filter(Boolean),
  targetChat: parseTelegramChatInput(process.env.X_TARGET_CHAT || '@Fantasyargoal'),
  autoChats: new Set(),
  botToken: String(process.env.X_POSTER_BOT_TOKEN || process.env.BOT_TOKEN || '').trim(),
  creditText: String(process.env.X_CREDIT_TEXT || '- Uploaded by @jiijhwiazbot').trim(),
  intervalSec: Math.max(12, Number(process.env.X_INTERVAL_SEC || 15)),
  seenIds: new Set(),
  lastSeenByAcc: {},
  lastUpdateOffset: 0,
  lastCheckAt: 0,
  lastError: '',
  postedCount: 0
};

function saveXWatchStateToDisk() {
  try {
    const data = {
      enabled: xWatchConfig.enabled,
      accounts: xWatchConfig.accounts,
      channels: xWatchConfig.channels,
      targetChat: xWatchConfig.targetChat,
      autoChats: [...xWatchConfig.autoChats],
      botToken: xWatchConfig.botToken,
      creditText: xWatchConfig.creditText,
      seenIds: [...xWatchConfig.seenIds].slice(-400),
      lastSeenByAcc: xWatchConfig.lastSeenByAcc,
      postedCount: xWatchConfig.postedCount
    };
    fs.writeFileSync(XWATCH_STATE_FILE, JSON.stringify(data), 'utf8');
  } catch (e) {}
}

function loadXWatchStateFromDisk() {
  try {
    if (!fs.existsSync(XWATCH_STATE_FILE)) return;
    const j = JSON.parse(fs.readFileSync(XWATCH_STATE_FILE, 'utf8'));
    if (typeof j.enabled === 'boolean') xWatchConfig.enabled = j.enabled;
    if (Array.isArray(j.accounts) && j.accounts.length) xWatchConfig.accounts = j.accounts;
    if (Array.isArray(j.channels) && j.channels.length) xWatchConfig.channels = j.channels;
    if (j.targetChat) xWatchConfig.targetChat = j.targetChat;
    if (Array.isArray(j.autoChats)) j.autoChats.forEach(c => xWatchConfig.autoChats.add(c));
    if (j.botToken) xWatchConfig.botToken = j.botToken;
    if (j.creditText) xWatchConfig.creditText = j.creditText;
    if (Array.isArray(j.seenIds)) j.seenIds.forEach(id => xWatchConfig.seenIds.add(String(id)));
    if (j.lastSeenByAcc && typeof j.lastSeenByAcc === 'object') xWatchConfig.lastSeenByAcc = { ...j.lastSeenByAcc };
    if (Number(j.postedCount) > 0) xWatchConfig.postedCount = Number(j.postedCount);
  } catch (e) {}
}
loadXWatchStateFromDisk();

function isValidTweetSnowflake(idStr) {
  if (!/^\d{17,21}$/.test(String(idStr || ''))) return false;
  try {
    const b = BigInt(idStr);
    if (b < 1400000000000000000n) return false;
    const tsMs = Number((b >> 22n) + 1288834974657n);
    return tsMs <= Date.now() + 600000;
  } catch (e) {
    return false;
  }
}

function sortSnowflakesDesc(ids) {
  return [...new Set(ids.map(String).filter(isValidTweetSnowflake))].sort((a, b) => {
    const pa = String(a).padStart(24, '0');
    const pb = String(b).padStart(24, '0');
    return pb.localeCompare(pa);
  });
}

const SEED_X_VIDEO_TCO = {
  goalsxtra: [
    'https://t.co/2v9P8CZWVE',
    'https://t.co/OTXryduJZc',
    'https://t.co/5PmcLxIWCs',
    'https://t.co/9DtiUOpnIL',
    'https://t.co/1VZiSZeetm',
    'https://t.co/iUrNyMcvvk'
  ],
  thegoalszone: [
    'https://t.co/2IjhiQZwCC',
    'https://t.co/2v9P8CZWVE',
    'https://t.co/OTXryduJZc'
  ]
};

async function resolveTcoToStatusId(tcoUrl) {
  try {
    const r = await fetchWithTimeout(tcoUrl, {
      redirect: 'manual',
      headers: { 'User-Agent': 'TelegramBot (like TwitterBot)' }
    }, 4000);
    const loc = r.headers.get('location') || '';
    const mLoc = loc.match(/\/status(?:es)?\/(\d{17,21})/i);
    if (mLoc && isValidTweetSnowflake(mLoc[1])) return mLoc[1];
    const txt = await r.text().catch(() => '');
    const mTxt = txt.match(/\/status(?:es)?\/(\d{17,21})/i);
    if (mTxt && isValidTweetSnowflake(mTxt[1])) return mTxt[1];
  } catch (e) {}
  return '';
}

// Паралельний пошук останніх твітів акаунта (Syndication + Nitter + DuckDuckGo)
async function fetchLatestVideosFromXAccount(rawInput, allowSeedFallback = false) {
  const cleanUser = parseXUsername(rawInput);
  if (!cleanUser) return [];

  const directStatus = String(rawInput || '').match(/\/status(?:es)?\/(\d{15,22})/i);
  if (directStatus) return [directStatus[1]];

  const collected = [];

  const tasks = [
    // 1) Офіційний віджет syndication.twitter.com (відповідає за ~250 мс)
    (async () => {
      try {
        const r = await fetchWithTimeout(`https://syndication.twitter.com/srv/timeline-profile/screen-name/${encodeURIComponent(cleanUser)}?dnt=true`, {
          headers: { 'User-Agent': UA, Accept: 'text/html,application/xhtml+xml,application/json,*/*' }
        }, 5000);
        if (r.ok) {
          const html = await r.text();
          for (const m of html.matchAll(/"entry_id"\s*:\s*"tweet-(\d{17,21})"/g)) {
            if (isValidTweetSnowflake(m[1])) collected.push(m[1]);
          }
          for (const m of html.matchAll(/\/status\/(\d{17,21})/g)) {
            if (isValidTweetSnowflake(m[1])) collected.push(m[1]);
          }
        }
      } catch (e) {}
    })(),

    // 2) Хронологічні інстанси Nitter паралельно
    ...['https://nitter.poast.org', 'https://nitter.privacydev.net', 'https://xcancel.com', 'https://nitter.jaydenha.uk'].map(host => (async () => {
      try {
        const rRss = await fetchWithTimeout(`${host}/${encodeURIComponent(cleanUser)}/rss`, {
          headers: { 'User-Agent': UA, Accept: 'application/rss+xml,text/xml,text/html,*/*' }
        }, 4000);
        if (rRss.ok) {
          const xml = await rRss.text();
          for (const m of xml.matchAll(/\/status\/(\d{17,21})/g)) {
            if (isValidTweetSnowflake(m[1])) collected.push(m[1]);
          }
          return;
        }
      } catch (e) {}
      try {
        const r = await fetchWithTimeout(`${host}/${encodeURIComponent(cleanUser)}`, {
          headers: { 'User-Agent': UA, Accept: 'text/html,application/xhtml+xml,*/*' }
        }, 4500);
        if (r.ok) {
          const html = await r.text();
          if (html.includes('timeline-item') || html.includes('/status/')) {
            for (const m of html.matchAll(/\/status\/(\d{17,21})/g)) {
              if (isValidTweetSnowflake(m[1])) collected.push(m[1]);
            }
          }
        }
      } catch (e) {}
    })())
  ];

  await Promise.allSettled(tasks);

  if (collected.length < 3) {
    try {
      const q = encodeURIComponent(`site:x.com/${cleanUser}/status OR "${cleanUser}" "pic.twitter.com"`);
      const r = await fetchWithTimeout(`https://html.duckduckgo.com/html/?q=${q}`, {
        headers: { 'User-Agent': UA, Accept: 'text/html,*/*' }
      }, 5000);
      if (r.ok) {
        const html = await r.text();
        for (const m of html.matchAll(/\/status\/(\d{17,21})/g)) {
          if (isValidTweetSnowflake(m[1])) collected.push(m[1]);
        }
        const tcos = [...new Set([...html.matchAll(/(?:https?:\/\/)?t\.co\/([A-Za-z0-9]{7,12})/g)].map(m => 'https://t.co/' + m[1]))].slice(0, 4);
        const resolved = await Promise.all(tcos.map(resolveTcoToStatusId));
        for (const sid of resolved) {
          if (sid) collected.push(sid);
        }
      }
    } catch (e) {}
  }

  if (allowSeedFallback && collected.length === 0) {
    const low = cleanUser.toLowerCase();
    const seeds = SEED_X_VIDEO_TCO[low] || SEED_X_VIDEO_TCO.goalsxtra;
    const resolved = await Promise.all(seeds.slice(0, 4).map(resolveTcoToStatusId));
    for (const sid of resolved) {
      if (sid) collected.push(sid);
    }
  }

  return sortSnowflakesDesc(collected).slice(0, 15);
}

// Підтримка X Amplify Video (де прямі .mp4 лежать у v.formats[] та v.variants[], а v.url є .m3u8!)
function pickVideoFromFxObj(tObj) {
  if (!tObj || typeof tObj !== 'object') return '';
  const m = tObj.media || {};
  let fallbackHls = '';

  const extractBestFromItem = item => {
    if (!item || typeof item !== 'object') return '';
    const subVariants = [
      ...(Array.isArray(item.formats) ? item.formats : []),
      ...(Array.isArray(item.variants) ? item.variants : [])
    ];
    const mp4Variants = subVariants
      .filter(f => f && f.url && !/\.m3u8(\?|$)/i.test(f.url) && (/mp4/i.test(String(f.container || f.content_type || '')) || /\.mp4/i.test(f.url)))
      .sort((a, b) => (Number(b.bitrate) || 0) - (Number(a.bitrate) || 0));
    if (mp4Variants.length) return mp4Variants[0].url;

    const u = item.url || (item.videos && item.videos[0] && item.videos[0].url) || '';
    if (u && !/\.m3u8(\?|$)/i.test(u)) return u;
    if (u && /\.m3u8(\?|$)/i.test(u) && !fallbackHls) fallbackHls = u;
    return '';
  };

  const vids = Array.isArray(m.videos) ? m.videos : [];
  for (const v of vids) {
    const mp4 = extractBestFromItem(v);
    if (mp4) return mp4;
  }
  const all = Array.isArray(m.all) ? m.all : [];
  for (const item of all) {
    if (item && (item.type === 'video' || item.type === 'gif')) {
      const mp4 = extractBestFromItem(item);
      if (mp4) return mp4;
    }
  }
  if (m.external && m.external.url) {
    if (!/\.m3u8(\?|$)/i.test(m.external.url)) return m.external.url;
    if (!fallbackHls) fallbackHls = m.external.url;
  }
  return fallbackHls;
}

function pickVideoFromVxObj(vObj) {
  if (!vObj || typeof vObj !== 'object') return '';
  let fallbackHls = '';
  const ext = Array.isArray(vObj.media_extended) ? vObj.media_extended : [];
  for (const m of ext) {
    if (m && m.url && (m.type === 'video' || m.type === 'gif' || /\.mp4|video\.twimg\.com/i.test(m.url))) {
      if (!/\.m3u8(\?|$)/i.test(m.url)) return m.url;
      if (!fallbackHls) fallbackHls = m.url;
    }
  }
  const urls = Array.isArray(vObj.mediaURLs) ? vObj.mediaURLs : [];
  for (const u of urls) {
    if (u && /\.mp4|video\.twimg\.com/i.test(String(u))) {
      if (!/\.m3u8(\?|$)/i.test(String(u))) return String(u);
      if (!fallbackHls) fallbackHls = String(u);
    }
  }
  return fallbackHls;
}

function pickVideoFromSynObj(sObj) {
  if (!sObj || typeof sObj !== 'object') return '';
  let fallbackHls = '';
  const details = Array.isArray(sObj.mediaDetails) ? sObj.mediaDetails : [];
  for (const md of details) {
    if (md && (md.type === 'video' || md.type === 'animated_gif') && md.video_info && Array.isArray(md.video_info.variants)) {
      const mp4s = md.video_info.variants
        .filter(v => v && v.url && !/\.m3u8(\?|$)/i.test(v.url) && (/video\/mp4/i.test(String(v.content_type || '')) || /\.mp4/i.test(v.url)))
        .sort((a, b) => (Number(b.bitrate) || 0) - (Number(a.bitrate) || 0));
      if (mp4s.length) return mp4s[0].url;
      const hls = md.video_info.variants.find(v => v && v.url && /\.m3u8(\?|$)/i.test(v.url));
      if (hls && !fallbackHls) fallbackHls = hls.url;
    }
  }
  if (sObj.video && Array.isArray(sObj.video.variants)) {
    const mp4s = sObj.video.variants
      .filter(v => v && (v.src || v.url) && !/\.m3u8(\?|$)/i.test(String(v.src || v.url)) && (/video\/mp4/i.test(String(v.type || v.content_type || '')) || /\.mp4/i.test(String(v.src || v.url))));
    if (mp4s.length) return mp4s[mp4s.length - 1].src || mp4s[mp4s.length - 1].url;
  }
  return fallbackHls;
}

async function resolveXVideoFromTweetId(username, statusId, depth = 0) {
  const cleanUser = parseXUsername(username) || 'i';
  const BOT_UA = 'TelegramBot (like TwitterBot)';
  let mainText = '';
  let fallbackHlsResult = null;
  const embeddedStatusIds = [];
  const embeddedTcos = [];

  const collectEmbeddedLinks = str => {
    const s = String(str || '');
    for (const m of s.matchAll(/\/status(?:es)?\/(\d{17,21})/gi)) {
      if (m[1] !== String(statusId) && isValidTweetSnowflake(m[1])) embeddedStatusIds.push(m[1]);
    }
    for (const m of s.matchAll(/https?:\/\/t\.co\/[A-Za-z0-9]{7,12}/gi)) {
      embeddedTcos.push(m[0]);
    }
  };

  // 1) FxTwitter & FixupX API (включно з Amplify v.formats[], Quote-твітами tw.quote та Retweet tw.retweet!)
  for (const base of ['https://api.fxtwitter.com', 'https://api.fixupx.com']) {
    try {
      const r = await fetchWithTimeout(`${base}/${encodeURIComponent(cleanUser)}/status/${statusId}`, {
        headers: { 'User-Agent': BOT_UA, Accept: 'application/json' }
      }, 4500);
      if (!r.ok) continue;
      const j = await r.json().catch(() => null);
      const t = j && j.tweet;
      if (!t) continue;
      if (!mainText && t.text) mainText = String(t.text);
      collectEmbeddedLinks(t.text);
      if (t.raw_text && t.raw_text.text) collectEmbeddedLinks(t.raw_text.text);

      const directVid = pickVideoFromFxObj(t) || pickVideoFromFxObj(t.quote) || pickVideoFromFxObj(t.retweet);
      if (directVid) {
        const resObj = {
          videoUrl: directVid,
          text: String(mainText || (t.quote && t.quote.text) || '').replace(/https?:\/\/\S+/g, '').trim(),
          author: (t.author && (t.author.screen_name || t.author.name)) || cleanUser
        };
        if (!/\.m3u8(\?|$)/i.test(directVid)) return resObj;
        fallbackHlsResult = resObj;
      }
      if (t.quote && t.quote.id && String(t.quote.id) !== String(statusId)) embeddedStatusIds.push(String(t.quote.id));
      if (t.retweet && t.retweet.id && String(t.retweet.id) !== String(statusId)) embeddedStatusIds.push(String(t.retweet.id));
      break;
    } catch (e) {}
  }

  // 2) VxTwitter & FixVX API
  for (const base of ['https://api.vxtwitter.com', 'https://api.fixvx.com']) {
    try {
      const r2 = await fetchWithTimeout(`${base}/${encodeURIComponent(cleanUser)}/status/${statusId}`, {
        headers: { 'User-Agent': BOT_UA, Accept: 'application/json' }
      }, 4500);
      if (!r2.ok) continue;
      const j2 = await r2.json().catch(() => null);
      if (!j2) continue;
      if (!mainText && j2.text) mainText = String(j2.text);
      collectEmbeddedLinks(j2.text);
      collectEmbeddedLinks(j2.qrtURL);
      collectEmbeddedLinks(j2.retweetURL);

      const vxVid = pickVideoFromVxObj(j2) || pickVideoFromVxObj(j2.qrt) || pickVideoFromVxObj(j2.retweet);
      if (vxVid) {
        const resObj = {
          videoUrl: vxVid,
          text: String(mainText).replace(/https?:\/\/\S+/g, '').trim(),
          author: j2.user_screen_name || cleanUser
        };
        if (!/\.m3u8(\?|$)/i.test(vxVid)) return resObj;
        if (!fallbackHlsResult) fallbackHlsResult = resObj;
      }
      break;
    } catch (e) {}
  }

  // 3) Офіційний CDN Syndication tweet-result
  try {
    const token = ((Number(statusId) / 1e15) * Math.PI).toString(36).replace(/(0+|\.)/g, '');
    const r3 = await fetchWithTimeout(`https://cdn.syndication.twimg.com/tweet-result?id=${statusId}&token=${token}&lang=en`, {
      headers: { 'User-Agent': BOT_UA, Accept: 'application/json' }
    }, 4500);
    if (r3.ok) {
      const j3 = await r3.json().catch(() => null);
      if (j3) {
        if (!mainText && j3.text) mainText = String(j3.text);
        collectEmbeddedLinks(j3.text);
        if (j3.entities) {
          for (const uObj of [...(j3.entities.urls || []), ...(j3.entities.media || [])]) {
            if (uObj && uObj.expanded_url) collectEmbeddedLinks(uObj.expanded_url);
          }
        }
        const synVid = pickVideoFromSynObj(j3) || pickVideoFromSynObj(j3.quoted_tweet) || pickVideoFromSynObj(j3.parent) || pickVideoFromSynObj(j3.retweeted_status);
        if (synVid) {
          const resObj = {
            videoUrl: synVid,
            text: String(mainText).replace(/https?:\/\/\S+/g, '').trim(),
            author: (j3.user && j3.user.screen_name) || cleanUser
          };
          if (!/\.m3u8(\?|$)/i.test(synVid)) return resObj;
          if (!fallbackHlsResult) fallbackHlsResult = resObj;
        }
      }
    }
  } catch (e) {}

  // 4) Розкриття вбудованих відео-посилань з донорських акаунтів (/status/ID/video/1 або t.co)
  if (depth < 1) {
    if (!embeddedStatusIds.length && embeddedTcos.length) {
      const resolvedTcos = await Promise.all([...new Set(embeddedTcos)].slice(0, 3).map(resolveTcoToStatusId));
      for (const rId of resolvedTcos) {
        if (rId && rId !== String(statusId)) embeddedStatusIds.push(rId);
      }
    }
    for (const subId of [...new Set(embeddedStatusIds)].slice(0, 3)) {
      const subMedia = await resolveXVideoFromTweetId('i', subId, depth + 1);
      if (subMedia && subMedia.videoUrl) {
        return {
          videoUrl: subMedia.videoUrl,
          text: String(mainText || subMedia.text || '').replace(/https?:\/\/\S+/g, '').trim(),
          author: cleanUser
        };
      }
    }
  }

  if (fallbackHlsResult) return fallbackHlsResult;

  // 5) Резерв через локальний Cobalt v10 на Render
  if (depth === 0 && cobaltReady) {
    try {
      const cob = await callInternalCobalt(`https://x.com/${cleanUser}/status/${statusId}`, '1080', '', false);
      if (cob && cob.url) {
        return {
          videoUrl: cob.url,
          text: String(mainText || '').replace(/https?:\/\/\S+/g, '').trim(),
          author: cleanUser
        };
      }
    } catch (e) {}
  }

  return null;
}

// Автоматичне виявлення каналів та груп, куди додали бота-постера (напр. @argoalbot)
async function discoverPartnerBotChannels() {
  const tok = xWatchConfig.botToken;
  if (!tok) return;
  try {
    const r = await fetchWithTimeout(`https://api.telegram.org/bot${tok}/getUpdates?offset=${xWatchConfig.lastUpdateOffset + 1}&limit=50&allowed_updates=${encodeURIComponent(JSON.stringify(['my_chat_member', 'channel_post', 'message']))}`, {
      headers: { 'User-Agent': UA }
    }, 4500);
    const j = await r.json().catch(() => null);
    if (!j || !j.ok || !Array.isArray(j.result)) return;
    let changed = false;
    for (const upd of j.result) {
      if (upd.update_id > xWatchConfig.lastUpdateOffset) {
        xWatchConfig.lastUpdateOffset = upd.update_id;
      }
      const chatObj =
        (upd.my_chat_member && upd.my_chat_member.chat) ||
        (upd.channel_post && upd.channel_post.chat) ||
        (upd.message && upd.message.chat);
      const newStatus = upd.my_chat_member && upd.my_chat_member.new_chat_member && upd.my_chat_member.new_chat_member.status;
      if (chatObj && (chatObj.type === 'channel' || chatObj.type === 'supergroup' || chatObj.type === 'group')) {
        if (newStatus === 'kicked' || newStatus === 'left') {
          xWatchConfig.autoChats.delete(String(chatObj.id));
          if (chatObj.username) xWatchConfig.autoChats.delete('@' + chatObj.username);
          changed = true;
        } else {
          const cid = chatObj.username ? ('@' + chatObj.username) : String(chatObj.id);
          xWatchConfig.autoChats.add(cid);
          if (!xWatchConfig.channels.some(c => c.toLowerCase() === cid.toLowerCase())) {
            xWatchConfig.channels.push(cid);
          }
          if (!xWatchConfig.targetChat) xWatchConfig.targetChat = cid;
          changed = true;
        }
      }
    }
    if (changed) saveXWatchStateToDisk();
  } catch (e) {}
}

function getAllTargetChatsForXWatch() {
  const list = [];
  const addUnique = raw => {
    const clean = parseTelegramChatInput(raw);
    if (clean && !list.some(x => x.toLowerCase() === clean.toLowerCase())) {
      list.push(clean);
    }
  };
  for (const ch of (xWatchConfig.channels || [])) addUnique(ch);
  if (xWatchConfig.targetChat) {
    for (const part of String(xWatchConfig.targetChat).split(/[,\s]+/)) addUnique(part);
  }
  for (const c of xWatchConfig.autoChats) addUnique(c);
  return list;
}

function formatGoalCaption(rawTweetText) {
  const cleanText = String(rawTweetText || '')
    .replace(/https?:\/\/\S+/g, '')
    .replace(/\s+$/g, '')
    .trim();
  const credit = xWatchConfig.creditText || '- Uploaded by @jiijhwiazbot';
  return cleanText ? `${cleanText}\n\n${credit}` : credit;
}

async function sendGoalVideoToChat(botToken, targetChat, videoUrl, caption) {
  const isHls = /\.m3u8(\?|$)/i.test(videoUrl);

  // 1) Спершу пробуємо пряму відправку URL через Telegram API (якщо це прямий .mp4)
  if (!isHls) {
    const sentDirect = await tgApiCall(botToken, 'sendVideo', {
      chat_id: targetChat,
      video: videoUrl,
      caption,
      supports_streaming: true
    }, false);
    if (sentDirect && sentDirect.ok && sentDirect.result) return sentDirect.result;
  }

  // 2) Якщо відео є .m3u8 (HLS) або > 20 МБ або Telegram не зміг забрати за URL — качаємо/збираємо на Render і заливаємо файлом!
  const tmpVid = tmpFile('_xgoal.mp4');
  try {
    if (isHls) {
      await runFfmpeg([
        '-user_agent', UA,
        '-headers', 'Referer: https://x.com/\r\n',
        '-i', videoUrl,
        '-c', 'copy',
        '-bsf:a', 'aac_adtstoasc',
        '-movflags', '+faststart',
        '-y', tmpVid
      ], 60000);
    } else {
      await downloadToFile(videoUrl, tmpVid, 'https://x.com/');
    }
    return await uploadMultipartToTelegram({
      botToken,
      chatId: targetChat,
      mode: 'video',
      filePath: tmpVid,
      caption,
      useLocalApi: localApiReady
    });
  } finally {
    safeUnlink(tmpVid);
  }
}

async function pollXAccountsOnce() {
  if (!xWatchConfig.botToken) return;
  await discoverPartnerBotChannels();
  const targets = getAllTargetChatsForXWatch();
  if (!xWatchConfig.enabled || !xWatchConfig.accounts.length || !targets.length) return;
  xWatchConfig.lastCheckAt = Date.now();
  let stateModified = false;

  for (const rawAcc of xWatchConfig.accounts) {
    const acc = parseXUsername(rawAcc);
    if (!acc) continue;
    const accKey = acc.toLowerCase();

    try {
      const tweetIds = await fetchLatestVideosFromXAccount(acc, false);
      if (!tweetIds.length) continue;

      const newestId = tweetIds[0];
      const prevBaseline = xWatchConfig.lastSeenByAcc[accKey] || '';

      // Перший запуск для цього акаунта: запам'ятовуємо поточний найновіший твіт як точку відліку, щоб не спамити старими постами!
      if (!prevBaseline) {
        xWatchConfig.lastSeenByAcc[accKey] = newestId;
        tweetIds.forEach(id => xWatchConfig.seenIds.add(String(id)));
        stateModified = true;
        continue;
      }

      // Шукаємо ТІЛЬКИ нові пости, які мають Snowflake ID суворо більший за prevBaseline!
      const brandNewIds = tweetIds
        .filter(tid => !xWatchConfig.seenIds.has(String(tid)) && String(tid).padStart(24, '0') > String(prevBaseline).padStart(24, '0'))
        .reverse();

      if (!brandNewIds.length) continue;

      for (const tid of brandNewIds.slice(-2)) {
        xWatchConfig.seenIds.add(String(tid));
        if (String(tid).padStart(24, '0') > String(xWatchConfig.lastSeenByAcc[accKey] || '').padStart(24, '0')) {
          xWatchConfig.lastSeenByAcc[accKey] = String(tid);
        }
        stateModified = true;

        const media = await resolveXVideoFromTweetId(acc, tid);
        // Тільки відео: якщо в новому пості немає відео — нічого не відправляємо!
        if (!media || !media.videoUrl) continue;

        const caption = formatGoalCaption(media.text);
        for (const chatTarget of targets) {
          try {
            const res = await sendGoalVideoToChat(xWatchConfig.botToken, chatTarget, media.videoUrl, caption);
            if (res) xWatchConfig.postedCount++;
          } catch (sendErr) {
            xWatchConfig.lastError = String((sendErr && sendErr.message) || sendErr).slice(0, 200);
          }
        }
      }
    } catch (e) {
      xWatchConfig.lastError = String((e && e.message) || e).slice(0, 200);
    }
  }

  if (stateModified) saveXWatchStateToDisk();
}

setInterval(() => {
  pollXAccountsOnce().catch(() => {});
}, 15000);

// Анти-сон для Render: кожні 8 хвилин пінгуємо зовнішній URL, щоб сервіс працював 24/7
setInterval(() => {
  if (PUBLIC_URL && PUBLIC_URL.startsWith('http')) {
    fetch(PUBLIC_URL + '/health').catch(() => {});
  }
}, 8 * 60 * 1000);

// ==================== 8. HTTP СЕРВЕР ТА ПРОКСІ НА ВНУТРІШНІЙ COBALT ====================
function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let len = 0;
    req.on('data', c => {
      len += c.length;
      if (len > 15 * MB) {
        req.destroy();
        return reject(new Error('BODY_TOO_LARGE'));
      }
      chunks.push(c);
    });
    req.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'));
      } catch (e) {
        reject(new Error('INVALID_JSON'));
      }
    });
    req.on('error', reject);
  });
}

function checkAuth(req) {
  if (!API_KEY) return true;
  const auth = String(req.headers['authorization'] || req.headers['x-api-key'] || '').trim();
  const clean = auth.replace(/^(?:Api-Key|Bearer)\s+/i, '').trim();
  return clean === API_KEY;
}

// Карта реєстрації тунелів Cobalt + Yoinks (pablostanley/yoinks архітектура: yt-dlp + ffmpeg + Cobalt v10)
const cobaltTunnelFallbackMap = new Map();

function rememberTunnelMeta(id, meta) {
  if (!id || !meta) return;
  if (cobaltTunnelFallbackMap.size > 500) {
    const firstKey = cobaltTunnelFallbackMap.keys().next().value;
    if (firstKey) cobaltTunnelFallbackMap.delete(firstKey);
  }
  cobaltTunnelFallbackMap.set(id, { ...meta, ts: Date.now() });
}

function extractTunnelIdFromUrl(u) {
  try {
    const qs = String(u || '').split('?')[1] || '';
    return new URLSearchParams(qs).get('id') || '';
  } catch (e) {
    return '';
  }
}

async function streamViaYoinksEngine(meta, res) {
  const { url: sourceUrl, quality = '1080', dubLang = '', isAudio = false } = meta || {};
  if (!sourceUrl) {
    if (!res.headersSent) res.writeHead(502, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ status: 'error', error: { code: 'no_source_url' } }));
  }

  const ext = isAudio ? '.mp3' : '.mp4';
  const outPath = tmpFile('_yoink' + ext);
  const extraFiles = [outPath];

  try {
    let ready = false;
    // 1) Спершу швидкий резолвер (TikWM no-watermark / YouTube Innertube / OceanSaver)
    try {
      const resolved = await callInternalCobalt(sourceUrl, quality, dubLang, isAudio);
      if (resolved && resolved.url) {
        const isHls = /\.m3u8(\?|$)|\/manifest\/hls/i.test(resolved.url);
        if (isHls) {
          await runFfmpeg([
            '-user_agent', resolved.customUa || UA,
            ...(resolved.referer ? ['-headers', `Referer: ${resolved.referer}\r\n`] : []),
            '-i', resolved.url,
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            '-movflags', '+faststart',
            '-y', outPath
          ], 55000);
          ready = true;
        } else if (resolved.audioUrl) {
          const vP = tmpFile('_yv.mp4');
          const aP = tmpFile('_ya.m4a');
          extraFiles.push(vP, aP);
          await Promise.all([
            downloadToFile(resolved.url, vP, resolved.referer || '', null, resolved.customUa || UA),
            downloadToFile(resolved.audioUrl, aP, resolved.referer || '', null, resolved.audioUa || resolved.customUa || UA)
          ]);
          await runFfmpeg([
            '-threads', '0', '-i', vP, '-i', aP,
            '-c:v', 'copy', '-c:a', 'aac',
            '-map', '0:v:0', '-map', '1:a:0',
            '-shortest', '-movflags', '+faststart', '-y', outPath
          ], 55000);
          ready = true;
        } else {
          await downloadToFile(resolved.url, outPath, resolved.referer || '', null, resolved.customUa || UA);
          ready = true;
        }
      }
    } catch (e) {
      safeUnlink(outPath);
    }

    // 2) Резерв через рушій Yoinks (yt-dlp + ffmpeg)
    if (!ready && ytDlpBin) {
      await runYtDlpToFile(sourceUrl, outPath, quality, dubLang, isAudio, 50000);
      ready = true;
    }

    if (!ready || !fs.existsSync(outPath)) {
      throw new Error('YOINKS_STREAM_FAILED');
    }

    const stat = fs.statSync(outPath);
    res.writeHead(200, {
      'Content-Type': isAudio ? 'audio/mpeg' : 'video/mp4',
      'Content-Length': String(stat.size),
      'Content-Disposition': `inline; filename="media${ext}"`,
      'Cache-Control': 'no-store'
    });
    const rs = fs.createReadStream(outPath);
    rs.on('close', () => extraFiles.forEach(safeUnlink));
    rs.on('error', () => extraFiles.forEach(safeUnlink));
    rs.pipe(res);
  } catch (e) {
    extraFiles.forEach(safeUnlink);
    if (!res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
    }
    res.end(JSON.stringify({ status: 'error', error: { code: 'yoinks_fallback_failed', detail: String(e.message || e) } }));
  }
}

async function handleCobaltPostWithYoinksFallback(req, res) {
  let body = {};
  try {
    body = await readJsonBody(req);
  } catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ status: 'error', error: { code: 'error.api.invalid_body' } }));
  }

  const sourceUrl = String(body.url || '').trim();
  const isAudio = body.downloadMode === 'audio' || body.isAudioOnly === true;
  const quality = String(body.videoQuality || body.vQuality || '1080');
  const dubLang = String(body.youtubeDubLang || '');

  // Прибираємо tiktokFullAudio на відео-запитах, щоб Cobalt v10 не видавав HTTP 500 у /tunnel
  const cleanBody = { ...body };
  if (!isAudio) {
    delete cleanBody.tiktokFullAudio;
    delete cleanBody.isTTFullAudio;
  }

  try {
    const r = await fetchWithTimeout(`http://127.0.0.1:${COBALT_PORT}/`, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'User-Agent': UA
      },
      body: JSON.stringify(cleanBody)
    }, 7500);

    const j = await r.json().catch(() => null);
    if (j && j.status && j.status !== 'error') {
      const meta = { url: sourceUrl, quality, dubLang, isAudio };
      if (typeof j.url === 'string' && j.url.includes('/tunnel')) {
        rememberTunnelMeta(extractTunnelIdFromUrl(j.url), meta);
      }
      if (Array.isArray(j.tunnel)) {
        for (const tu of j.tunnel) rememberTunnelMeta(extractTunnelIdFromUrl(tu), meta);
      }
      if (Array.isArray(j.picker)) {
        for (const p of j.picker) {
          if (p && p.url && p.url.includes('/tunnel')) rememberTunnelMeta(extractTunnelIdFromUrl(p.url), meta);
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify(j));
    }
  } catch (e) {}

  // Якщо внутрішній Cobalt v10 повернув помилку (напр. на YouTube або TikTok) — автоматично вмикаємо рушій Yoinks!
  const yoinkId = crypto.randomBytes(12).toString('hex');
  rememberTunnelMeta(yoinkId, { url: sourceUrl, quality, dubLang, isAudio });
  const ext = isAudio ? 'mp3' : 'mp4';
  res.writeHead(200, { 'Content-Type': 'application/json' });
  return res.end(JSON.stringify({
    status: 'tunnel',
    url: `${PUBLIC_URL}/yoink-stream?id=${yoinkId}`,
    filename: `video_${quality === 'max' ? '1080' : quality}p.${ext}`
  }));
}

function proxyToCobalt(req, res) {
  const cleanHeaders = { ...req.headers, host: `127.0.0.1:${COBALT_PORT}` };
  delete cleanHeaders['authorization'];
  delete cleanHeaders['x-forwarded-for'];
  delete cleanHeaders['cf-connecting-ip'];
  delete cleanHeaders['true-client-ip'];
  delete cleanHeaders['x-real-ip'];
  delete cleanHeaders['x-client-ip'];
  delete cleanHeaders['forwarded'];

  const isTunnelGet = req.method === 'GET' && String(req.url || '').startsWith('/tunnel');
  const tunnelId = isTunnelGet ? extractTunnelIdFromUrl(req.url) : '';

  const options = {
    hostname: '127.0.0.1',
    port: COBALT_PORT,
    path: req.url,
    method: req.method,
    headers: cleanHeaders
  };

  const proxyReq = http.request(options, proxyRes => {
    const code = proxyRes.statusCode || 502;
    // Якщо внутрішній Cobalt v10 /tunnel повернув помилку HTTP 500/403/404 — перехоплюємо її і віддаємо відео через Yoinks!
    if (isTunnelGet && code >= 400 && tunnelId && cobaltTunnelFallbackMap.has(tunnelId)) {
      proxyRes.resume();
      const meta = cobaltTunnelFallbackMap.get(tunnelId);
      return streamViaYoinksEngine(meta, res);
    }
    res.writeHead(code, proxyRes.headers);
    proxyRes.pipe(res, { end: true });
  });
  proxyReq.on('error', err => {
    if (isTunnelGet && tunnelId && cobaltTunnelFallbackMap.has(tunnelId) && !res.headersSent) {
      return streamViaYoinksEngine(cobaltTunnelFallbackMap.get(tunnelId), res);
    }
    if (!res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
    }
    res.end(JSON.stringify({ status: 'error', error: { code: 'cobalt_unavailable', detail: err.message } }));
  });
  req.pipe(proxyReq, { end: true });
}

const server = http.createServer(async (req, res) => {
  const u = String(req.url || '/').split('?')[0];

  if (req.method === 'GET' && u === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({
      ok: true,
      service: 'stream-sync-node',
      cobaltReady,
      ffmpeg: true,
      ytdlp: Boolean(ytDlpBin),
      localBotApi: localApiReady,
      localBotApiDiagnostics: {
        apiIdProvided: !!RAW_API_ID,
        apiIdValid: API_ID_VALID,
        apiIdPreview: RAW_API_ID ? (RAW_API_ID.slice(0, 2) + '***' + RAW_API_ID.slice(-2)) : '',
        apiHashProvided: !!RAW_API_HASH,
        apiHashValid: API_HASH_VALID,
        apiHashLength: RAW_API_HASH.length,
        lastError: localApiLastError
      },
      xWatcher: {
        enabled: xWatchConfig.enabled,
        accounts: xWatchConfig.accounts,
        channels: getAllTargetChatsForXWatch(),
        targetChat: getAllTargetChatsForXWatch().join(', ') || xWatchConfig.targetChat,
        postedCount: xWatchConfig.postedCount,
        lastCheckAt: xWatchConfig.lastCheckAt
      },
      uptime: Math.round(process.uptime())
    }));
  }

  if (req.method === 'POST' && u === '/send') {
    try {
      const body = await readJsonBody(req);
      const out = await handleSendMedia(body);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify(out));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, error: String((e && e.message) || e) }));
    }
  }

  if (req.method === 'POST' && u === '/circle') {
    try {
      const body = await readJsonBody(req);
      const out = await handleCircle(body);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify(out));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, error: String((e && e.message) || e) }));
    }
  }

  if (req.method === 'POST' && u === '/stickers') {
    try {
      const body = await readJsonBody(req);
      const out = await handleStickersGrid(body);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify(out));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, error: String((e && e.message) || e) }));
    }
  }

  if (req.method === 'GET' && u === '/yt-info') {
    try {
      const qs = String(req.url || '').split('?')[1] || '';
      const params = new URLSearchParams(qs);
      const rawUrl = params.get('url') || '';
      const id = extractYtId(rawUrl);
      if (!id) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ ok: false, error: 'NO_YT_ID' }));
      }
      const info = await queryYtInnertubeOnRender(id);
      if (!info) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ ok: false, error: 'NO_INNERTUBE' }));
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: true, ...info }));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, error: String(e.message || e) }));
    }
  }

  if (u === '/xwatch') {
    if (!checkAuth(req)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, error: 'UNAUTHORIZED' }));
    }
    if (req.method === 'GET') {
      await discoverPartnerBotChannels();
      const allChannels = getAllTargetChatsForXWatch();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({
        ok: true,
        enabled: xWatchConfig.enabled,
        accounts: xWatchConfig.accounts,
        channels: allChannels,
        targetChat: allChannels[0] || xWatchConfig.targetChat,
        autoChats: [...xWatchConfig.autoChats],
        seenIds: [...xWatchConfig.seenIds].slice(-400),
        lastSeenByAcc: xWatchConfig.lastSeenByAcc,
        hasCustomToken: !!xWatchConfig.botToken,
        creditText: xWatchConfig.creditText,
        postedCount: xWatchConfig.postedCount,
        lastCheckAt: xWatchConfig.lastCheckAt,
        lastError: xWatchConfig.lastError
      }));
    }
    if (req.method === 'POST') {
      try {
        const body = await readJsonBody(req);
        if (typeof body.enabled === 'boolean') xWatchConfig.enabled = body.enabled;
        if (Array.isArray(body.accounts) && body.accounts.length) {
          xWatchConfig.accounts = body.accounts.map(parseXUsername).filter(Boolean);
        }
        if (Array.isArray(body.channels) && body.channels.length) {
          xWatchConfig.channels = body.channels.map(parseTelegramChatInput).filter(Boolean);
          if (xWatchConfig.channels[0]) xWatchConfig.targetChat = xWatchConfig.channels[0];
        } else if (body.targetChat !== undefined) {
          const parsedChat = parseTelegramChatInput(body.targetChat);
          if (parsedChat) {
            xWatchConfig.targetChat = parsedChat;
            if (!xWatchConfig.channels.some(c => c.toLowerCase() === parsedChat.toLowerCase())) {
              xWatchConfig.channels.unshift(parsedChat);
            }
          }
        }
        if (Array.isArray(body.seenIds)) {
          for (const id of body.seenIds) xWatchConfig.seenIds.add(String(id));
        }
        if (body.lastSeenByAcc && typeof body.lastSeenByAcc === 'object') {
          xWatchConfig.lastSeenByAcc = { ...xWatchConfig.lastSeenByAcc, ...body.lastSeenByAcc };
        }
        if (Number(body.postedCount) > xWatchConfig.postedCount) {
          xWatchConfig.postedCount = Number(body.postedCount);
        }
        if (body.creditText !== undefined && String(body.creditText).trim()) {
          xWatchConfig.creditText = String(body.creditText).trim();
        }
        if (body.botToken) {
          xWatchConfig.botToken = String(body.botToken).trim();
          await discoverPartnerBotChannels();
        }
        saveXWatchStateToDisk();

        if (body.testNow) {
          const accountsToTest = body.testAccount
            ? [parseXUsername(body.testAccount)].filter(Boolean)
            : (xWatchConfig.accounts.length ? xWatchConfig.accounts : ['GoalsXtra', 'TheGoalsZone']);
          const testChats = body.testChat
            ? [parseTelegramChatInput(body.testChat)].filter(Boolean)
            : getAllTargetChatsForXWatch();
          const primaryChat = testChats[0] || body.adminChatId;
          const tokToUse = xWatchConfig.botToken || body.fallbackToken || '';
          const postedList = [];

          for (const testAcc of accountsToTest) {
            const ids = await fetchLatestVideosFromXAccount(testAcc, true);
            let accSent = 0;
            for (const tid of ids) {
              if (accSent >= 2) break;
              const media = await resolveXVideoFromTweetId(testAcc, tid);
              if (media && media.videoUrl) {
                const caption = formatGoalCaption(media.text);
                let actualTarget = primaryChat;
                let sentMsg = await sendGoalVideoToChat(tokToUse, primaryChat, media.videoUrl, caption).catch(() => null);
                if (!sentMsg && body.adminChatId && String(body.adminChatId) !== String(primaryChat)) {
                  actualTarget = body.adminChatId;
                  sentMsg = await sendGoalVideoToChat(tokToUse, body.adminChatId, media.videoUrl, caption).catch(() => null);
                }
                if (sentMsg) {
                  accSent++;
                  xWatchConfig.seenIds.add(String(tid));
                  postedList.push({ account: testAcc, tweetId: tid, targetChat: actualTarget, caption });
                }
              }
            }
          }
          saveXWatchStateToDisk();
          res.writeHead(200, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ ok: postedList.length > 0, posted: postedList[0] || null, postedList, targetChat: primaryChat }));
        }

        const allChannels = getAllTargetChatsForXWatch();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
          ok: true,
          enabled: xWatchConfig.enabled,
          accounts: xWatchConfig.accounts,
          channels: allChannels,
          targetChat: allChannels[0] || xWatchConfig.targetChat,
          autoChats: [...xWatchConfig.autoChats],
          postedCount: xWatchConfig.postedCount
        }));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ ok: false, error: String(e.message || e) }));
      }
    }
  }

  if (req.method === 'GET' && u === '/yoink-stream') {
    const qs = String(req.url || '').split('?')[1] || '';
    const id = new URLSearchParams(qs).get('id') || '';
    const meta = cobaltTunnelFallbackMap.get(id);
    if (!meta) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'error', error: { code: 'yoink_tunnel_expired' } }));
    }
    return streamViaYoinksEngine(meta, res);
  }

  if (req.method === 'POST' && (u === '/' || u === '/api/json')) {
    if (!checkAuth(req)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'error', error: { code: 'error.api.auth.key.missing' } }));
    }
    return handleCobaltPostWithYoinksFallback(req, res);
  }

  return proxyToCobalt(req, res);
});

ensureYtDlp().catch(() => {});
startCobalt();
startLocalBotApi();

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[stream-sync-node] Gateway + Cobalt + FFmpeg + yt-dlp + Stickers + X-Watcher listening on port ${PORT} (public: ${PUBLIC_URL})`);
});