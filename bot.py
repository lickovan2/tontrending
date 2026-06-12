"""Telegram-бот (aiogram 3.x) + точка входа. Только майлстоуны 25/50/75/100%.

Команда в ГРУППЕ (только админ, внутри нужной темы):
  /bind <процент>  — привязать ТЕКУЩУЮ тему к майлстоуну (25/50/75/100).
                     Бот берёт message_thread_id из самого сообщения.

main() поднимает polling бота (для /bind) и SSE-монитор майлстоунов.
"""
from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import Message

import config
import db
from monitor import Monitor
from notify import Notifier
from ton_source import TonSource

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("ogelite.bot")

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()


@dp.message(CommandStart(), F.chat.type == ChatType.PRIVATE)
async def cmd_start(m: Message) -> None:
    await m.answer(
        "👋 Я слежу за сборами токенов на TopBlast / Uranus / Stonks.\n"
        "Майлстоуны 25/50/75/100% публикуются в темы группы.\n\n"
        "В группе: /bind 25 (внутри нужной темы) — привязать ветку к проценту.",
    )


# ── ГРУППА: привязка темы к майлстоуну (админ) ───────────────────────────────
@dp.message(Command("bind"), F.chat.type.in_({ChatType.SUPERGROUP, ChatType.GROUP}))
async def cmd_bind(m: Message, command: CommandObject) -> None:
    """Привязывает ТЕКУЩУЮ тему к майлстоуну. Вызывать внутри нужной темы.
    message_thread_id берётся из самого сообщения — отдельно вводить не нужно."""
    member = await bot.get_chat_member(m.chat.id, m.from_user.id)
    if member.status not in ("administrator", "creator"):
        return await m.reply("Только администратор может привязывать темы.")
    try:
        milestone = int((command.args or "").strip())
    except ValueError:
        return await m.reply("Использование: /bind 25  (внутри нужной темы)")
    if milestone not in config.MILESTONES:
        return await m.reply(f"Допустимые проценты: {', '.join(map(str, config.MILESTONES))}")
    thread_id = m.message_thread_id
    if thread_id is None:
        return await m.reply("Команду нужно вызвать внутри темы (Topic), не в General.")

    async with db.Session() as s:
        existing = await s.scalar(
            db.select(db.GroupTopic).where(
                db.GroupTopic.chat_id == m.chat.id, db.GroupTopic.milestone == milestone)
        )
        if existing:
            existing.message_thread_id = thread_id
        else:
            s.add(db.GroupTopic(chat_id=m.chat.id, milestone=milestone,
                                message_thread_id=thread_id))
        await s.commit()
    await m.reply(f"✅ Тема привязана к майлстоуну <b>{milestone}%</b> "
                  f"(thread_id={thread_id})", parse_mode=ParseMode.HTML)


# ── точка входа: бот + монитор майлстоунов ───────────────────────────────────
async def main() -> None:
    await db.init_db()
    ton = TonSource()
    notifier = Notifier(bot, ton)
    monitor = Monitor(ton, notifier)
    try:
        await asyncio.gather(
            dp.start_polling(bot),   # обработка /bind
            monitor.run(),           # SSE-монитор майлстоунов
        )
    finally:
        await ton.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
