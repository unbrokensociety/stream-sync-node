FROM aiogram/telegram-bot-api:latest AS tgapi
FROM ghcr.io/imputnet/cobalt:11

USER root

# FFmpeg + бібліотеки для telegram-bot-api (Alpine) + curl для завантаження yt-dlp
RUN apk add --no-cache ffmpeg libstdc++ openssl zlib ca-certificates curl

# Telegram Local Bot API (завантаження файлів до 2000 МБ)
COPY --from=tgapi /usr/local/bin/telegram-bot-api /usr/local/bin/telegram-bot-api
RUN chmod +x /usr/local/bin/telegram-bot-api && mkdir -p /tmp/tgbotapi /tmp/tgbotapi-tmp /helper

# yt-dlp (самодостатній musl-бінарник). Якщо GitHub недоступний під час збірки — збірка НЕ падає,
# сервер сам докачає yt-dlp при старті і щодоби оновлює його (yt-dlp -U).
RUN (curl -fsSL --retry 3 --retry-delay 2 -o /usr/local/bin/yt-dlp \
        https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_musllinux \
     && chmod a+rx /usr/local/bin/yt-dlp \
     && /usr/local/bin/yt-dlp --version) \
    || (echo "yt-dlp: пропущено при збірці, буде завантажено при старті"; rm -f /usr/local/bin/yt-dlp)

# Єдиний комбайн-сервер: Gateway + Cobalt + yt-dlp + ffmpeg + Telegram-відправник + X-вотчер
COPY server.js /helper/server.cjs

ENV PORT=10000
ENV COBALT_INTERNAL_PORT=9000
ENV LOCAL_BOT_API_PORT=8081
EXPOSE 10000

CMD ["node", "--max-old-space-size=160", "/helper/server.cjs"]
