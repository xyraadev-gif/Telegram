# Бот ежедневных напоминаний (aiogram 3 + aiosqlite + APScheduler)

## Запуск
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env      # заполните BOT_TOKEN, ADMIN_SECRET_KEY, (PAYMENT_PROVIDER_TOKEN)
python bot.py
```
Docker:
```bash
docker build -t reminder-bot .
docker run -d --restart=always --env-file .env -v $(pwd)/data:/data reminder-bot
```
Запускайте **один** экземпляр бота (long polling + SQLite).

## Платежи
- **Stars (XTR)** работают сразу, токен не нужен.
- **Рубли**: @BotFather → Payments → подключите провайдера → скопируйте токен в `PAYMENT_PROVIDER_TOKEN`.
  У провайдеров есть минимальная сумма счёта — если 65 ₽ отклоняется, проверьте лимиты провайдера.

## Админ-панель
`/admin <ADMIN_SECRET_KEY>` (или `/admin` и ввод кода следующим сообщением). Сообщение с кодом удаляется.
5 неверных попыток — блокировка на 15 минут. Выход — кнопка «🚪 Выйти».

## Заметки
- Часовой пояс пользователя: `/timezone Europe/Berlin` (по умолчанию `DEFAULT_TIMEZONE`).
- Telegram не позволяет задать кастомный звук для сообщения бота: «стиль» реализован через
  оформление текста, необязательный стикер (`STICKER_*`) и тихую доставку для 🧘 «Спокойного».
- Приоритетная доставка: Премиум-напоминания отправляются первыми и с большей параллельностью.
- Если подписка истекла, существующие напоминания сохраняются, но создавать новые сверх лимита 10 нельзя.
