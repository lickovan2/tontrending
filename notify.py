"""Слой уведомлений: майлстоуны сборов 25/50/75/100% → ТЕМА супергруппы.

Карточка отправляется в привязанную ветку (message_thread_id, привязка
«процент → тема» в group_topics). Других уведомлений нет.
"""
from __future__ import annotations

import asyncio
import logging

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import BufferedInputFile, InlineKeyboardButton, InlineKeyboardMarkup

import config
import db

logger = logging.getLogger("ogelite.notify")


def _fast_buy_kb(friendly_ca: str) -> InlineKeyboardMarkup:
    """Две кнопки Fast Buy: Redotrade и Dtrade (реф-ссылки с адресом токена)."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🦴 Redotrade",
                             url=config.REDOTRADE_URL.format(ca=friendly_ca)),
        InlineKeyboardButton(text="💱 Dtrade",
                             url=config.DTRADE_URL.format(ca=friendly_ca)),
    ]])


def _bar(progress: float, width: int = 10) -> str:
    filled = int(round(progress / 100 * width))
    return "█" * filled + "░" * (width - filled)


def _fmt_usd(x: float) -> str:
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.2f}"


class Notifier:
    def __init__(self, bot: Bot, ton=None) -> None:
        self.bot = bot
        self.ton = ton  # TonSource — для PNG-графика redo и friendly-адреса

    # ── Карточка токена: график redo + копируемый CA + кнопки Fast Buy ─────────
    async def send_token_card(self, chat_id: int, snap: dict,
                              thread_id: int | None = None,
                              milestone: int | None = None) -> bool:
        """Универсальная карточка. CA в <code> копируется тапом. Под фото-графиком
        redo.trade — кнопки Redotrade и Dtrade с реф-ссылками на этот токен."""
        friendly = self.ton.to_friendly(snap["ca"]) if self.ton else snap["ca"]
        head = (f"🎯 <b>{snap['symbol']}</b> — <b>{milestone}% Bonding Curve</b>\n\n"
                if milestone else f"🪙 <b>{snap['symbol']}</b> · {snap.get('name','')}\n\n")
        caption = (
            head +
            f"{_bar(snap['progress'])}  <b>{snap['progress']:.1f}%</b>\n"
            f"💰 <b>{snap['collected_ton']:.0f} / {snap['target_ton']:.0f} TON</b>\n"
            f"📊 MCap: <b>{_fmt_usd(snap['mcap_usd'])}</b>  ·  🌐 {snap['platform']}\n\n"
            f"<code>{friendly}</code>"  # тап = копировать адрес
        )
        kb = _fast_buy_kb(friendly)
        png = await self.ton.get_chart_png(snap["ca"]) if self.ton else None
        try:
            if png:
                await self.bot.send_photo(
                    chat_id=chat_id, message_thread_id=thread_id,
                    photo=BufferedInputFile(png, "chart.png"),
                    caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb)
            else:  # график не отдался — шлём текст с кнопками
                await self.bot.send_message(
                    chat_id=chat_id, message_thread_id=thread_id, text=caption,
                    parse_mode=ParseMode.HTML, reply_markup=kb,
                    disable_web_page_preview=True)
            return True
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            return await self.send_token_card(chat_id, snap, thread_id, milestone)
        except Exception as e:
            logger.warning("card в %s (thread=%s): %s", chat_id, thread_id, e)
            return False

    # ── МАЙЛСТОУН → ТЕМА ГРУППЫ ───────────────────────────────────────────────
    async def notify_milestone(self, s, snap: dict, milestone: int) -> None:
        """Уведомление о достижении X% сбора — в привязанную тему супергруппы."""
        thread_id = await db.topic_for_milestone(s, config.GROUP_CHAT_ID, milestone)
        if thread_id is None:
            logger.warning("нет привязки темы для %s%% (chat %s) — пропуск",
                           milestone, config.GROUP_CHAT_ID)
            return
        # карточка с графиком redo + копируемым CA + кнопками → в нужную ветку
        await self.send_token_card(config.GROUP_CHAT_ID, snap,
                                   thread_id=thread_id, milestone=milestone)
        logger.info("майлстоун %s%% по %s → тема %s", milestone, snap["symbol"], thread_id)
