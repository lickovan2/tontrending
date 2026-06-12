# ogelite — майлстоуны сборов TON → темы Telegram

Только одна функция: следить за токенами TopBlast / Uranus / Stonks и при
достижении **25 / 50 / 75 / 100 %** Hard Cap публиковать карточку в нужную
ветку (Topic) супергруппы.

| Файл | Назначение |
|------|-----------|
| `config.py` | площадки, опкоды, Hard Cap (TopBlast 1550 / Uranus 1050 / Stonks 2000), майлстоуны |
| `ton_source.py` | on-chain `get_meme_data` / `get_bonding_data` + redo.trade (mcap/price) + PNG-график |
| `monitor.py` | SSE-индексатор: новые токены, прогресс, проверка майлстоунов |
| `notify.py` | карточка майлстоуна → ветка (`message_thread_id`) |
| `bot.py` | `/bind` (привязка темы к проценту) + точка входа |
| `db.py` | PostgreSQL/SQLite: `tokens`, `token_milestones`, `group_topics` |

## Поток
```
TonAPI SSE (traces: фабрики + Meme-контракты)
   └─ snapshot = get_meme_data (raisedFunds) / get_bonding_data
        progress = raisedFunds / HardCap × 100
        пересёк 25/50/75/100% и ещё не слали → карточка в ветку
```

## Настройка тем
В супергруппе с включёнными Topics, бот — админ. В каждой нужной теме:
`/bind 25` (и `50`, `75`, `100`). Бот берёт `message_thread_id` из сообщения.

## Запуск
```bash
pip install -r requirements.txt
cp .env.example .env   # BOT_TOKEN, GROUP_CHAT_ID, ключи API
python bot.py
```

> Примечание: realtime-сервис на Node.js/TypeScript (тот же функционал майлстоунов
> через WebSocket TonAPI, реакция 3–6 с) — в `node-service/`.
