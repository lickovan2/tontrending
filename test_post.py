"""Тест: отправить карточку токена в каждую ветку группы (25/50/75/100).
Карточка = график redo.trade + копируемый CA + кнопки Redotrade/Dtrade.
Запуск: .venv/Scripts/python.exe test_post.py"""
import asyncio
from aiogram import Bot

import config
from notify import Notifier
from ton_source import TonSource

# (ветка_thread_id, майлстоун, CA, platform_key, label_override, target_override)
CARDS = [
    (3, 25, "EQA63zFjp9or3lPrXhVdkJHEaKusklyHCrMGf75Io09maBmw", "topblast", "topblast", None),
    (5, 50, "EQDUoDHHPY5Z6jo26HtH1WjfM6Fu-e_EL1IxEz4HTE5dGLgI", "stonks", "stonks", None),
    (7, 75, "EQAOFHBzPpFDwuVOlT7e97u4nnOGmTUNCwdhedUSPi97PwQP", "topblast", "uranus", 1050.0),
    (9, 100, "EQA6nsxnABGi8aDTLJ1e7q_wgz9TOR7JdtvuPqnPUurJ1tE_", "topblast", "topblast", None),
]


async def main():
    bot = Bot(token=config.BOT_TOKEN)
    ton = TonSource()
    notifier = Notifier(bot, ton)
    try:
        for thread_id, milestone, ca, pkey, label, target in CARDS:
            snap = await ton.snapshot(ca, pkey)
            if not snap:
                print(f"нет данных по {ca[:12]}")
                continue
            snap["platform"] = label
            if target:  # для uranus пересчёт прогресса под таргет 1050
                snap["target_ton"] = target
                snap["progress"] = min(snap["collected_ton"] / target * 100, 100.0)
            ok = await notifier.send_token_card(
                config.GROUP_CHAT_ID, snap, thread_id=thread_id, milestone=milestone)
            print(f"ветка {thread_id} ({milestone}%) {label} ${snap['symbol']}: {'OK' if ok else 'FAIL'}")
    finally:
        await ton.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
