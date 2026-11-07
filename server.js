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
let localApiReady = Boolean( process.env.TELEGRAM_API_ID && process.env.TELEGRAM_API_HASH );
let localApiLastError = '';

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

  // КРИТИЧНО: На Render /tmp монтується як чистий tmpfs під час старту контейнера,
  // тому обов'язково створюємо /tmp/tgbotapi та /tmp/tgbotapi-tmp у рантаймі перед запуском бінарника
  // (інакше td::realpath() у telegram-bot-api падає з помилкою No such file or directory та кодом 1!)
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

async function downloadToFile(url, destPath, referer = '', onPct = null) {
  const headers = { 'User-Agent': UA, Accept: '*/*' };
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

async function callInternalCobalt(sourceUrl, quality = 'max') {
  const isYt = /youtube\.com|youtu\.be/i.test(String(sourceUrl));
  const r = await fetch(`http://127.0.0.1:${COBALT_PORT}/`, {
    method: 'POST',
    headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'User-Agent': UA },
    body: JSON.stringify({
      url: sourceUrl,
      videoQuality: isYt ? String(quality === 'max' ? '1080' : (quality || '1080')) : String(quality || 'max'),
      downloadMode: 'auto',
      filenameStyle: 'basic',
      allowH265: !isYt,
      tiktokFullAudio: true,
      ...(isYt ? { youtubeVideoCodec: 'h264' } : {})
    })
  });
  const j = await r.json().catch(() => null);
  if (!j || j.status === 'error') throw new Error('COBALT_ERR');
  if (j.status === 'local-processing' && Array.isArray(j.tunnel) && j.tunnel.length) {
    return {
      url: j.tunnel[0].replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`),
      audioUrl: (j.tunnel[1] || '').replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`)
    };
  }
  const u = typeof j.url === 'string' ? j.url : (typeof j.tunnel === 'string' ? j.tunnel : '');
  if (!u) throw new Error('NO_COBALT_URL');
  return { url: u.replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`), audioUrl: '' };
}

async function uploadMultipartToTelegram({
  botToken, chatId, mode, filePath, fileNameOverride = '', caption, replyMarkup, replyToMsgId, businessConnectionId, useLocalApi
}) {
  const stat = fs.statSync(filePath);
  const method = mode === 'document' ? 'sendDocument' : mode === 'audio' ? 'sendAudio' : mode === 'video_note' ? 'sendVideoNote' : 'sendVideo';
  const field = mode === 'document' ? 'document' : mode === 'audio' ? 'audio' : mode === 'video_note' ? 'video_note' : 'video';
  const fileName = fileNameOverride || (mode === 'audio' ? 'audio.mp3' : mode === 'video_note' ? 'circle.mp4' : 'video.mp4');
  const mime = mode === 'audio' ? 'audio/mpeg' : 'video/mp4';

  // 1) Спроба через Local Bot API (до 2000 МБ за 0.1 сек через локальний шлях file://)
  if (useLocalApi && localApiReady) {
    try {
      const payload = {
        chat_id: chatId,
        [field]: 'file://' + filePath,
        ...(caption && mode !== 'video_note' ? { caption, parse_mode: 'HTML' } : {}),
        ...(mode === 'video' ? { supports_streaming: true, show_caption_above_media: true } : {}),
        ...(mode === 'video_note' ? { length: 480 } : {}),
        ...(replyToMsgId ? { reply_parameters: { message_id: Number(replyToMsgId), allow_sending_without_reply: true } } : {}),
        ...(businessConnectionId ? { business_connection_id: businessConnectionId } : {}),
        ...(replyMarkup ? { reply_markup: replyMarkup } : {})
      };
      const jLocal = await tgApiCall(botToken, method, payload, true);
      if (jLocal && jLocal.ok && jLocal.result) return jLocal.result;
    } catch (e) {}
  }

  // 2) Стандартний multipart upload на api.telegram.org (до 50 МБ)
  if (stat.size > STD_TG_LIMIT) {
    throw new Error('FILE_OVER_50MB_NEED_FIT');
  }

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
    form.append('length', '480');
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

  const r = await fetch(`https://api.telegram.org/bot${botToken}/${method}`, {
    method: 'POST',
    body: form
  });
  const j = await r.json().catch(() => null);
  if (!j || !j.ok) {
    throw new Error((j && j.description) || ('TG_UPLOAD_' + r.status + '_SIZE_' + stat.size));
  }
  return j.result;
}

// ==================== 4. ОБРОБНИК /send (ВІДЕО >20 МБ ТА ДО 2 ГБ, СКЛЕЙКА 1080p, АВТО-СПЛІТ/ФІТ) ====================
async function handleSendMedia(body) {
  const {
    botToken, chatId, mode = 'video', url, audioUrl = '', sourceUrl = '', tunnelUsed = false,
    referer = '', caption = '', replyMarkup = null, replyToMsgId = null, replyToMessageId = null,
    businessConnectionId = null, progressMsgId = null
  } = body || {};
  const rplId = replyToMsgId || replyToMessageId || null;

  if (!botToken || !chatId || (!url && !sourceUrl)) {
    throw new Error('MISSING_PARAMS');
  }

  let targetUrl = String(url || '').replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`);
  let targetAudio = String(audioUrl || '').replace(PUBLIC_URL, `http://127.0.0.1:${COBALT_PORT}`);

  if ((tunnelUsed || !targetUrl) && sourceUrl) {
    const fresh = await callInternalCobalt(sourceUrl, '1080').catch(() => null);
    if (fresh && fresh.url) {
      targetUrl = fresh.url;
      if (fresh.audioUrl && !targetAudio) targetAudio = fresh.audioUrl;
    }
  }

  const filesToClean = [];
  try {
    await updateProgressMsg(botToken, chatId, progressMsgId, 30, 'Завантаження');

    let finalPath = tmpFile('.mp4');
    filesToClean.push(finalPath);

    const isHls = /\.m3u8(\?|$)/i.test(targetUrl);
    if (isHls) {
      await runFfmpeg([
        '-user_agent', UA,
        ...(referer ? ['-headers', `Referer: ${referer}\r\n`] : []),
        '-i', targetUrl,
        '-c', 'copy',
        '-bsf:a', 'aac_adtstoasc',
        '-movflags', '+faststart',
        '-y', finalPath
      ]);
    } else if (targetAudio) {
      const vPath = tmpFile('_v.mp4');
      const aPath = tmpFile('_a.m4a');
      filesToClean.push(vPath, aPath);
      await Promise.all([
        downloadToFile(targetUrl, vPath, referer, p => updateProgressMsg(botToken, chatId, progressMsgId, p, 'Завантаження 1080p')),
        downloadToFile(targetAudio, aPath, referer)
      ]);
      await updateProgressMsg(botToken, chatId, progressMsgId, 88, 'Збірка MP4');
      await runFfmpeg([
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
    } else {
      await downloadToFile(targetUrl, finalPath, referer, p => updateProgressMsg(botToken, chatId, progressMsgId, p, 'Завантаження'));
    }

    await updateProgressMsg(botToken, chatId, progressMsgId, 95, 'Відправка в Telegram');

    // Спершу пробуємо відправити оригінал (якщо запущений Local Bot API — пройде до 2000 МБ!)
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
      // Якщо файл > 50 МБ і бот працює через хмарний api.telegram.org — автоматично стискаємо під 49 МБ через FFmpeg!
      const stat = fs.statSync(finalPath);
      if (stat.size <= STD_TG_LIMIT) throw uploadErr;

      await updateProgressMsg(botToken, chatId, progressMsgId, 91, 'Оптимізація розміру (>50 МБ)');
      const compPath = tmpFile('_fit.mp4');
      filesToClean.push(compPath);
      await runFfmpeg([
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

// ==================== 5. ОБРОБНИК /circle (ІДЕАЛЬНІ КРУЖЕЧКИ ЧЕРЕЗ СПРАВЖНІЙ FFMPEG) ====================
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
  try {
    let dlUrl = url;
    if (!dlUrl && fileId) {
      const gf = await tgApiCall(botToken, 'getFile', { file_id: fileId });
      const fp = gf && gf.ok && gf.result && gf.result.file_path;
      if (!fp) throw new Error('GET_FILE_FAILED');
      dlUrl = `https://api.telegram.org/file/bot${botToken}/${fp}`;
    }

    await updateProgressMsg(botToken, chatId, progressMsgId, 55, 'Швидка обрізка у кружечок');
    const ffArgs = srcInput => [
      '-threads', '0',
      '-i', srcInput,
      '-t', '60',
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
    ];

    try {
      // Потокова конвертація напряму з URL в один прохід (без очікування подвійного запису на диск!)
      await runFfmpeg(ffArgs(dlUrl), 45000);
    } catch (e) {
      await downloadToFile(dlUrl, inPath);
      await runFfmpeg(ffArgs(inPath), 45000);
    }

    await updateProgressMsg(botToken, chatId, progressMsgId, 92, 'Відправка кружечка');
    const result = await uploadMultipartToTelegram({
      botToken,
      chatId,
      mode: 'video_note',
      filePath: outPath,
      replyMarkup,
      replyToMsgId,
      businessConnectionId,
      useLocalApi: localApiReady
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
    if (!dlUrl && fileId) {
      const gf = await tgApiCall(botToken, 'getFile', { file_id: fileId });
      const fp = gf && gf.ok && gf.result && gf.result.file_path;
      if (!fp) throw new Error('GET_FILE_FAILED');
      dlUrl = `https://api.telegram.org/file/bot${botToken}/${fp}`;
    }
    if (!dlUrl && sourceUrl) {
      const c = await callInternalCobalt(sourceUrl, 'max');
      dlUrl = c.url;
    }

    await updateProgressMsg(botToken, chatId, progressMsgId, 25, 'Завантаження оригіналу');
    await downloadToFile(dlUrl, inPath);

    const { width, height, duration } = await probeVideoDimensions(inPath);
    // Обчислюємо область сітки квадратиків по центру кадру (відрізаючи чорні смуги TikTok зверху і знизу)
    const maxTileW = Math.floor(width / gridCols);
    const maxTileH = Math.floor(height / gridRows);
    const tileSize = Math.max(64, Math.min(maxTileW, maxTileH));
    const gridW = tileSize * gridCols;
    const gridH = tileSize * gridRows;
    const startX = Math.max(0, Math.floor((width - gridW) / 2));
    const startY = Math.max(0, Math.floor((height - gridH) / 2));
    // Прибираємо 2% рамки всередині кожної клітинки, щоб лінії сітки з TikTok не потрапляли на стікер
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

// ==================== 7. АВТОПАРСЕР X (TWITTER) ДЛЯ ФУТБОЛЬНИХ ГОЛІВ (КОЖНІ 15 СЕК) ====================
function parseXUsername(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  const m = s.match(/(?:https?:\/\/)?(?:www\.|mobile\.)?(?:x|twitter|fxtwitter|vxtwitter|fixupx|xcancel)\.com\/([A-Za-z0-9_]{1,30})/i);
  if (m && m[1] && !/^(?:home|explore|search|i|intent|share|hashtag|status)$/i.test(m[1])) {
    return m[1];
  }
  return s.replace(/^@+/, '').split(/[\/?#\s]/)[0].trim();
}

const xWatchConfig = {
  enabled: !!(process.env.X_ACCOUNTS && process.env.X_TARGET_CHAT),
  accounts: String(process.env.X_ACCOUNTS || '').split(',').map(parseXUsername).filter(Boolean),
  targetChat: String(process.env.X_TARGET_CHAT || '').trim(),
  botToken: String(process.env.X_POSTER_BOT_TOKEN || process.env.BOT_TOKEN || '').trim(),
  intervalSec: Math.max(12, Number(process.env.X_INTERVAL_SEC || 15)),
  seenIds: new Set(),
  initializedAccounts: new Set(),
  lastCheckAt: 0,
  postedCount: 0
};

function sortSnowflakesDesc(ids) {
  return [...new Set(ids)].sort((a, b) => {
    const pa = String(a).padStart(24, '0');
    const pb = String(b).padStart(24, '0');
    return pb.localeCompare(pa);
  });
}

async function fetchLatestVideosFromXAccount(rawInput) {
  const cleanUser = parseXUsername(rawInput);
  if (!cleanUser) return [];

  // Якщо передано пряме посилання на конкретний твіт (/status/ID)
  const directStatus = String(rawInput || '').match(/\/status(?:es)?\/(\d{15,22})/i);
  if (directStatus) return [directStatus[1]];

  const collected = [];

  // Джерело 1: Офіційний віджет Twitter Syndication srv/timeline-profile (JSON __NEXT_DATA__)
  try {
    const r = await fetch(`https://syndication.twitter.com/srv/timeline-profile/screen-name/${encodeURIComponent(cleanUser)}`, {
      headers: { 'User-Agent': UA, Accept: 'text/html,application/xhtml+xml,application/json,*/*' }
    });
    if (r.ok) {
      const html = await r.text();
      for (const m of html.matchAll(/"id_str"\s*:\s*"(\d{15,22})"/g)) collected.push(m[1]);
      for (const m of html.matchAll(/\/status\/(\d{15,22})/g)) collected.push(m[1]);
      if (collected.length) return sortSnowflakesDesc(collected).slice(0, 6);
    }
  } catch (e) {}

  // Джерело 2: cdn.syndication.twimg.com
  try {
    const r = await fetch(`https://cdn.syndication.twimg.com/timeline/profile?screen_name=${encodeURIComponent(cleanUser)}&with_replies=false`, {
      headers: { 'User-Agent': UA, Accept: 'text/html,application/json,*/*' }
    });
    if (r.ok) {
      const txt = await r.text();
      for (const m of txt.matchAll(/\/status\/(\d{15,22})/g)) collected.push(m[1]);
      if (collected.length) return sortSnowflakesDesc(collected).slice(0, 6);
    }
  } catch (e) {}

  // Джерело 3: Jina Reader проксі для профілю X
  try {
    const r = await fetch(`https://r.jina.ai/https://x.com/${encodeURIComponent(cleanUser)}`, {
      headers: { 'User-Agent': UA, Accept: 'text/plain,*/*' }
    });
    if (r.ok) {
      const txt = await r.text();
      for (const m of txt.matchAll(/\/status\/(\d{15,22})/g)) collected.push(m[1]);
      if (collected.length) return sortSnowflakesDesc(collected).slice(0, 6);
    }
  } catch (e) {}

  // Джерело 4: Живі Nitter інстанси 2026
  for (const host of ['https://nitter.tiekoetter.com', 'https://nitter.netbub.com', 'https://nitter.poast.org', 'https://nitter.privacydev.net']) {
    try {
      const r = await fetch(`${host}/${encodeURIComponent(cleanUser)}/rss`, {
        headers: { 'User-Agent': UA, Accept: 'application/rss+xml,text/xml,*/*' }
      });
      if (r.ok) {
        const xml = await r.text();
        for (const m of xml.matchAll(/\/status\/(\d{15,22})/g)) collected.push(m[1]);
        if (collected.length) return sortSnowflakesDesc(collected).slice(0, 6);
      }
    } catch (e) {}
  }

  return [];
}

async function resolveXVideoFromTweetId(username, statusId) {
  const cleanUser = parseXUsername(username) || 'i';
  try {
    const r = await fetch(`https://api.fxtwitter.com/${encodeURIComponent(cleanUser)}/status/${statusId}`, {
      headers: { 'User-Agent': UA, Accept: 'application/json' }
    });
    if (r.ok) {
      const j = await r.json();
      const t = j && j.tweet;
      if (t) {
        const vids = (t.media && (t.media.videos || (t.media.all && t.media.all.filter(m => m.type === 'video' || m.type === 'gif')))) || [];
        if (vids.length && vids[0].url) {
          return {
            videoUrl: vids[0].url,
            text: String(t.text || '').replace(/https?:\/\/t\.co\/\S+/g, '').trim(),
            author: (t.author && (t.author.screen_name || t.author.name)) || cleanUser
          };
        }
      }
    }
  } catch (e) {}

  try {
    const r2 = await fetch(`https://api.vxtwitter.com/${encodeURIComponent(cleanUser)}/status/${statusId}`, {
      headers: { 'User-Agent': UA, Accept: 'application/json' }
    });
    if (r2.ok) {
      const j2 = await r2.json();
      const mediaList = Array.isArray(j2 && j2.media_extended) ? j2.media_extended : [];
      const vid = mediaList.find(m => m && (m.type === 'video' || m.type === 'gif') && m.url);
      if (vid) {
        return {
          videoUrl: vid.url,
          text: String(j2.text || '').replace(/https?:\/\/t\.co\/\S+/g, '').trim(),
          author: j2.user_screen_name || cleanUser
        };
      }
    }
  } catch (e) {}
  return null;
}

async function pollXAccountsOnce() {
  if (!xWatchConfig.enabled || !xWatchConfig.accounts.length || !xWatchConfig.targetChat || !xWatchConfig.botToken) return;
  xWatchConfig.lastCheckAt = Date.now();

  for (const acc of xWatchConfig.accounts) {
    try {
      const tweetIds = await fetchLatestVideosFromXAccount(acc);
      if (!tweetIds.length) continue;

      if (!xWatchConfig.initializedAccounts.has(acc)) {
        tweetIds.forEach(id => xWatchConfig.seenIds.add(id));
        xWatchConfig.initializedAccounts.add(acc);
        continue;
      }

      for (const tid of [...tweetIds].reverse()) {
        if (xWatchConfig.seenIds.has(tid)) continue;
        xWatchConfig.seenIds.add(tid);

        const media = await resolveXVideoFromTweetId(acc, tid);
        if (!media || !media.videoUrl) continue;

        const caption = media.text ? `⚽ <b>${media.text.slice(0, 800)}</b>` : `⚽ @${acc}`;
        const sent = await tgApiCall(xWatchConfig.botToken, 'sendVideo', {
          chat_id: xWatchConfig.targetChat,
          video: media.videoUrl,
          caption,
          parse_mode: 'HTML',
          supports_streaming: true
        });
        if (sent && sent.ok) {
          xWatchConfig.postedCount++;
        }
      }
    } catch (e) {}
  }
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

function proxyToCobalt(req, res) {
  const cleanHeaders = { ...req.headers, host: `127.0.0.1:${COBALT_PORT}` };
  // Видаляємо заголовки проксі IP, щоб внутрішній Cobalt бачив однаковий IP (127.0.0.1)
  // і для POST /, і для GET /tunnel (інакше Cobalt відхиляє тунель з помилкою HTTP 500!)
  delete cleanHeaders['authorization'];
  delete cleanHeaders['x-forwarded-for'];
  delete cleanHeaders['cf-connecting-ip'];
  delete cleanHeaders['true-client-ip'];
  delete cleanHeaders['x-real-ip'];
  delete cleanHeaders['x-client-ip'];
  delete cleanHeaders['forwarded'];

  const options = {
    hostname: '127.0.0.1',
    port: COBALT_PORT,
    path: req.url,
    method: req.method,
    headers: cleanHeaders
  };

  const proxyReq = http.request(options, proxyRes => {
    res.writeHead(proxyRes.statusCode || 502, proxyRes.headers);
    proxyRes.pipe(res, { end: true });
  });
  proxyReq.on('error', err => {
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
        targetChat: xWatchConfig.targetChat,
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

  if (u === '/xwatch') {
    if (!checkAuth(req)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: false, error: 'UNAUTHORIZED' }));
    }
    if (req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({
        ok: true,
        enabled: xWatchConfig.enabled,
        accounts: xWatchConfig.accounts,
        targetChat: xWatchConfig.targetChat,
        postedCount: xWatchConfig.postedCount,
        lastCheckAt: xWatchConfig.lastCheckAt
      }));
    }
    if (req.method === 'POST') {
      try {
        const body = await readJsonBody(req);
        if (typeof body.enabled === 'boolean') xWatchConfig.enabled = body.enabled;
        if (Array.isArray(body.accounts)) {
          xWatchConfig.accounts = body.accounts.map(parseXUsername).filter(Boolean);
          xWatchConfig.initializedAccounts.clear();
        }
        if (body.targetChat !== undefined) xWatchConfig.targetChat = String(body.targetChat || '').trim();
        if (body.botToken) xWatchConfig.botToken = String(body.botToken).trim();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
          ok: true,
          enabled: xWatchConfig.enabled,
          accounts: xWatchConfig.accounts,
          targetChat: xWatchConfig.targetChat,
          postedCount: xWatchConfig.postedCount
        }));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ ok: false, error: String(e.message || e) }));
      }
    }
  }

  if (req.method === 'POST' && (u === '/' || u === '/api/json')) {
    if (!checkAuth(req)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'error', error: { code: 'error.api.auth.key.missing' } }));
    }
  }

  return proxyToCobalt(req, res);
});

startCobalt();
startLocalBotApi();

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[stream-sync-node] Gateway + Cobalt + FFmpeg + Stickers + X-Watcher listening on port ${PORT} (public: ${PUBLIC_URL})`);
});