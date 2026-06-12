"""Центральная конфигурация платформы ogelite.

Все секреты — из .env (см. .env.example). Здесь же зашиты блокчейн-константы
площадок (адреса фабрик, пороги грэдуэйшна, опкоды), чтобы монитор и бот
ссылались на единый источник правды.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# ── Telegram ──────────────────────────────────────────────────────────────
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
GROUP_CHAT_ID: int = int(os.getenv("GROUP_CHAT_ID") or "0")

# ── API ───────────────────────────────────────────────────────────────────
TONAPI_KEY: str = os.getenv("TONAPI_KEY", "")
TONCENTER_KEY: str = os.getenv("TONCENTER_KEY", "")

TONAPI_BASE = "https://tonapi.io"
TONAPI_SSE = "https://tonapi.io/v2/sse"          # стриминг трейсов в реальном времени
TONCENTER_BASE = "https://toncenter.com/api/v3"  # runGetMethod
REDO_API_BASE = "https://redo.trade/api"         # истина по mcap/price/ath/volume
REDO_CHART_BASE = "https://redo.trade/start"     # PNG-график (og:image)
DEXSCREENER_BASE = "https://api.dexscreener.com" # курс TON/USD

# ── Fast Buy реф-ссылки (кнопки под карточкой токена) ───────────────────────
# {ca} подставляется friendly-адрес токена.
REDOTRADE_URL: str = os.getenv("REDOTRADE_URL", "https://t.me/redotrade?start=AYlII2Fl-{ca}")
DTRADE_URL: str = os.getenv("DTRADE_URL", "https://t.me/dtrade?start=1Bl6JBuOmV_{ca}")

# ── База данных ────────────────────────────────────────────────────────────
DATABASE_URL: str = os.getenv(
    "DATABASE_URL", "postgresql+asyncpg://ogelite:ogelite@localhost:5432/ogelite"
)

# ── Веб ────────────────────────────────────────────────────────────────────
WEB_HOST: str = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT: int = int(os.getenv("WEB_PORT", "8080"))


# ── Площадки (launchpad-ы) ──────────────────────────────────────────────────
# Каждая площадка = bonding-curve фабрика + параметры. Монитор подписывается на
# адреса фабрик через SSE, а прогресс читает get_bonding_data на контракте-кривой.
# ── Опкоды TopBlast/Uranus (общий MemeFactory) ──────────────────────────────
# Источник: https://groypfi.io/docs/topblast (подтверждены on-chain).
OP_DEPLOY = 0x6FF416DC        # → MemeFactory (деплой нового токена)
OP_BUY = 0x742B36D8           # → Meme (jetton master): купить
OP_SELL = 0x595F07BC          # → MemeWallet (jetton wallet): продать/сжечь
OP_REQUEST_WALLET = 0x2C76B973
# События (external-out из Meme-контракта) — ИХ ловим для realtime-ленты сделок:
OP_BUY_EVENT = 0xA0AA6BC2     # BuyEvent  {trader, amountIn, amountOut, fees, supply, raised, graduated}
OP_SELL_EVENT = 0x3AB0FCCC    # SellEvent {trader, amountIn, amountOut, fees, supply, raised}
TRADE_EVENT_OPS = {OP_BUY_EVENT, OP_SELL_EVENT}


class Platform:
    def __init__(self, key: str, name: str, factories: list[str],
                 target_ton: float, model: str):
        self.key = key                # машинный идентификатор (topblast/stonks)
        self.name = name              # человекочитаемое имя
        self.factories = factories    # адреса фабрик (для SSE-подписки)
        self.target_ton = target_ton  # TON до грэдуэйшна (для процентов сборов)
        self.model = model            # "meme" (get_meme_data) | "bonding" (get_bonding_data)


# ⚠️ Пороги грэдуэйшна заданы по словам владельца (TopBlast 1550, Stonks 2000,
# Uranus 1050). Доки groypfi указывают 1500(v3)/2500(legacy) — при расхождении
# меняется только когда срабатывают майлстоуны 25/50/75/100%; правится здесь.
PLATFORMS: dict[str, Platform] = {
    # TopBlast / GroypFi — общий MemeFactory с Uranus, читается get_meme_data
    "topblast": Platform(
        key="topblast", name="TopBlast",
        factories=[
            "EQA6ivhIOBQJvqO1SY3IvutmluM1d817gZDeEjiaVNBLePd3",  # MemeFactory v1.3 (новые деплои)
            "EQAO4cYqithwdltzmrlal1L5JKLK5Xk76feAJq0VoBC6Fy8T",  # legacy (Uranus-shared)
        ],
        target_ton=1550.0, model="meme",
    ),
    # Stonks(pump) — virtual-liquidity, читается get_bonding_data
    "stonks": Platform(
        key="stonks", name="Stonks",
        factories=["EQDEcx-hDfCH4ktFKAaz_PV7lqIlv1R38oCPEyar8DOE0zCs"],
        target_ton=2000.0, model="bonding",
    ),
}

# Быстрый обратный индекс «адрес фабрики → платформа»
FACTORY_TO_PLATFORM: dict[str, Platform] = {
    f: p for p in PLATFORMS.values() for f in p.factories
}

# Таргет для токенов Uranus на общем legacy-факторе (EQAO4cYq...).
# Применяется, когда деплой помечен memo «Uranus»/без «Topblast». Различение по
# memo — TODO; до него legacy-токены идут как TopBlast (1550).
URANUS_TARGET: float = 1050.0

# Майлстоуны сборов (%), на которых шлём уведомление в тему группы.
MILESTONES: tuple[int, ...] = (25, 50, 75, 100)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в .env")
