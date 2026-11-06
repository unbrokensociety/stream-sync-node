FROM aiogram/telegram-bot-api:latest AS tgapi
FROM ghcr.io/imputnet/cobalt:10

USER root

# Ставимо FFmpeg та бібліотеки для офіційного бінарника telegram-bot-api (Alpine)
RUN apk add --no-cache ffmpeg libstdc++ openssl zlib ca-certificates curl

# Копіюємо готовий скомпільований бінарник Telegram Local Bot API (ліміт до 2000 МБ)
COPY --from=tgapi /usr/local/bin/telegram-bot-api /usr/local/bin/telegram-bot-api
RUN chmod +x /usr/local/bin/telegram-bot-api && mkdir -p /tmp/tgbotapi /tmp/tgbotapi-tmp /helper

# Копіюємо єдиний комбайн-сервер (Gateway + Cobalt Proxy + FFmpeg Media Helper + Circle + X-Watcher)
COPY server.js /helper/server.cjs

ENV PORT=10000
ENV COBALT_INTERNAL_PORT=9000
ENV LOCAL_BOT_API_PORT=8081
EXPOSE 10000

CMD ["node", "--max-old-space-size=160", "/helper/server.cjs"]
