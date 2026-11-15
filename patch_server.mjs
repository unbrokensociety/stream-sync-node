import fs from 'node:fs';
import { execSync } from 'node:child_process';

const file = process.argv[2] || 'server.js';
let src = fs.readFileSync(file, 'utf8');

// 1. PORT и PUBLIC_URL под ClawCloud Run, Zeabur, Koyeb, HF Spaces, Railway и любой Docker-хостинг
src = src.replace(
  /const PORT = envNum\('PORT', 10000\);/,
  "const PORT = envNum('PORT', env('SPACE_HOST') ? 7860 : 10000);"
);
src = src.replace(
  /const PUBLIC_URL = .*?;/,
  "const PUBLIC_URL = (env('SPACE_HOST') ? ('https://' + env('SPACE_HOST')) : (env('KOYEB_PUBLIC_DOMAIN') ? ('https://' + env('KOYEB_PUBLIC_DOMAIN')) : (env('ZEABUR_WEB_DOMAIN') ? ('https://' + env('ZEABUR_WEB_DOMAIN')) : (env('RAILWAY_PUBLIC_DOMAIN') ? ('https://' + env('RAILWAY_PUBLIC_DOMAIN')) : (env('RENDER_EXTERNAL_URL') || env('API_URL') || ('http://127.0.0.1:' + PORT)))))).replace(/\\/+$/, '');"
);

// 2. Убираем STATE_CHAT_ID
src = src.replace(/^\s*const STATE_CHAT_ID = .*?\r?\n/m, '');

// 3. Убираем Секцию 3 (X-Watcher persistence/Store) и оставляем минимальный runtime
src = src.replace(
  /\/\/ ==================== 3\. СТАН[\s\S]*?(?=\/\/ ==================== 4\. TELEGRAM API ====================)/,
  "// ==================== 3. RUNTIME ====================\nconst runtime = { mainToken: env('BOT_TOKEN'), adminChat: ADMIN_CHAT_ID };\n\n"
);

// 4. Убираем вызов Store.load() из handleSend
src = src.replace(
  /if \(!runtime\.mainToken && !env\('BOT_TOKEN'\) && !env\('STATE_BOT_TOKEN'\)\) \{[\s\S]*?\}/,
  "if (!runtime.mainToken && !env('BOT_TOKEN')) runtime.mainToken = botToken;"
);

// 5. Убираем всю Секцию 13 (X-ВОТЧЕР)
src = src.replace(
  /\/\/ ==================== 13\. X-ВОТЧЕР[\s\S]*?(?=\/\/ ==================== 14\. ЗАПУСК COBALT ТА LOCAL BOT API ====================)/,
  ''
);

// 6. Убираем persistence и xWatcher из healthJson()
src = src.replace(/^\s*persistence:\s*\{[\s\S]*?\},?\r?\n/gm, '');
src = src.replace(/^\s*xWatcher:\s*\{[\s\S]*?\},?\r?\n/gm, '');

// 7. Убираем маршрут /xwatch из HTTP-сервера
src = src.replace(
  /\s*if \(u === '\/xwatch'\) \{[\s\S]*?return sendJson\(res, 200, xStatusJson\(\)\);\s*\}\s*\}/,
  ''
);

// 8. Убираем Store.saveNow() из shutdown() и запуск Store/xTick из main()
src = src.replace(/^\s*await withTimeout\(Store\.saveNow\(\)[\s\S]*?\);\r?\n/gm, '');
src = src.replace(/^\s*Store\.load\(\)[\s\S]*?;\r?\n/gm, '');
src = src.replace(/^\s*setTimeout\(xTick,.*?\);\r?\n/gm, '');
src = src.replace(/^\s*log\('sys', 'X-вотчер:.*?\);\r?\n/gm, '');

// 9. Автономность: снимаем жёсткий лимит 200MB для Cobalt (настраивается через COBALT_NODE_OPTIONS)
src = src.replace(
  /NODE_OPTIONS:\s*'--max-old-space-size=200'/,
  "NODE_OPTIONS: env('COBALT_NODE_OPTIONS', '--max-old-space-size=384')"
);

// 10. Автономность: очистка /tmp при старте + более частый self-ping (каждые 4 мин) если задан PUBLIC_URL
src = src.replace(
  /setInterval\(cleanTmp, 10 \* 60 \* 1000\);/,
  "cleanTmp(); setInterval(cleanTmp, 5 * 60 * 1000);"
);
src = src.replace(
  /8 \* 60 \* 1000/,
  "4 * 60 * 1000"
);

fs.writeFileSync(file, src, 'utf8');
execSync(`node --check "${file}"`, { stdio: 'inherit' });
console.log('OK: server.js успешно пропатчен (X-Watcher удалён, автономность и порты/URL настроены)!');
