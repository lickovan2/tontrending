"""Источник данных TON: чтение bonding-кривой on-chain + redo.trade + курс TON.

Архитектурная идея разделения источников:
  • redo.trade  — «истина» по mcap/price/ath/volume/holders (готовые цифры).
  • get_bonding_data (on-chain через Toncenter runGetMethod) — прогресс сбора и
    резервы кривой. Это первоисточник, который не зависит от чужого индексатора.
  • TonAPI     — метаданные джеттона (symbol/name/image/decimals).
  • DexScreener — курс TON/USD.

Логика _parse_bonding / _read_bonding_data перенесена из E:\ton analyzator\ton.py,
где она проверена в бою на Stonks-кривых (4-полевая и 8-полевая модели).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp

import config

logger = logging.getLogger("ogelite.ton")


def _to_friendly(raw: str) -> str:
    """Заглушка-нормализатор адреса. В проде используйте pytonapi/tonsdk Address.
    Здесь возвращаем как есть — Toncenter v3 принимает и raw (0:...), и friendly."""
    return raw


class TonSource:
    VIRTUAL_TON = 500.0          # базовый виртуальный TON-резерв кривой
    _TON_REF = "EQA1EIDrR33zgL21rwDIfGo7h4ETWieentUvg7jIT-3aP5GG"  # pTON для курса

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._ton_usd_cache: tuple[float, float] | None = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Accept": "application/json"},
                timeout=aiohttp.ClientTimeout(total=15),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── низкоуровневые запросы ────────────────────────────────────────────────
    def _ta_headers(self) -> dict:
        h = {"Accept": "application/json"}
        if config.TONAPI_KEY:
            h["Authorization"] = f"Bearer {config.TONAPI_KEY}"
        return h

    async def _ta_get(self, path: str, params: dict | None = None) -> Optional[dict]:
        s = await self._sess()
        url = f"{config.TONAPI_BASE}/v2{path}"
        for attempt in range(3):
            try:
                async with s.get(url, params=params, headers=self._ta_headers()) as r:
                    if r.status == 429:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    return await r.json(content_type=None) if r.status == 200 else None
            except Exception as e:
                if attempt == 2:
                    logger.error("TONAPI %s: %s", path, e)
                await asyncio.sleep(0.3)
        return None

    async def _tc_post(self, path: str, body: dict) -> Optional[dict]:
        s = await self._sess()
        url = f"{config.TONCENTER_BASE}{path}"
        headers = {"X-API-Key": config.TONCENTER_KEY} if config.TONCENTER_KEY else {}
        try:
            async with s.post(url, json=body, headers=headers,
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json() if r.status == 200 else None
        except Exception as e:
            logger.error("TC POST %s: %s", path, e)
            return None

    async def _run_method(self, account: str, method: str, stack: list | None = None) -> Optional[list]:
        """runGetMethod через Toncenter v3. Возвращает stack при exit_code == 0."""
        d = await self._tc_post("/runGetMethod", {
            "address": account, "method": method, "stack": stack or [],
        })
        if not d or d.get("exit_code") != 0:
            return None
        return d.get("stack")

    # ── метаданные джеттона ───────────────────────────────────────────────────
    async def get_meta(self, ca: str) -> dict:
        data = await self._ta_get(f"/jettons/{ca}") or {}
        md = data.get("metadata") or {}
        return {
            "symbol": md.get("symbol") or "???",
            "name": md.get("name") or md.get("symbol") or "Unknown",
            "decimals": int(md.get("decimals") or 9),
            "image_url": md.get("image") or "",
            "total_supply": int(data.get("total_supply") or 0),
        }

    # ── bonding curve (перенос из ton analyzator) ─────────────────────────────
    def _parse_bonding(self, addr: str, vals: list[int]) -> Optional[dict]:
        """Единый разбор get_bonding_data для обеих моделей кривой:
          4 поля: [presale, progress%, ton_res(nano), jet_res(nano)]
          8 полей: [progress×100, target(nano), ton_res, jet_res, collected(nano), _, _, presale]
        Цена = ton_res / jet_res в обоих случаях."""
        if len(vals) < 4 or vals[3] <= 0:
            return None
        virtual = len(vals) >= 8
        return {
            "curve": addr,
            "ton_reserve": vals[2] / 1e9,
            "jet_reserve_raw": vals[3],
            "presale": bool(vals[7] if virtual else vals[0]),
            "progress": (vals[0] / 100.0) if virtual else float(vals[1]),
            "collected": (vals[4] / 1e9) if virtual else 0.0,
            "target": (vals[1] / 1e9) if virtual else 0.0,
        }

    async def read_bonding(self, ca: str, holders_hint: list[str] | None = None) -> Optional[dict]:
        """Читает get_bonding_data: сперва на самом минтере (virtual-liquidity),
        затем на контрактах-кандидатах кривой (jettons-on-curve)."""
        candidates = [ca] + list(holders_hint or [])
        seen: set[str] = set()
        for raw in candidates:
            if not raw or raw in seen:
                continue
            seen.add(raw)
            addr = _to_friendly(raw)
            stack = await self._run_method(addr, "get_bonding_data")
            if not stack:
                continue
            try:
                vals = [int(x.get("value", "0"), 16) for x in stack if x.get("type") == "num"]
            except Exception:
                continue
            bd = self._parse_bonding(addr, vals)
            if bd:
                return bd
        return None

    # ── TopBlast get_meme_data (точный first-party источник) ──────────────────
    async def get_meme_data(self, meme: str) -> Optional[dict]:
        """Читает get_meme_data на Meme-контракте TopBlast/Uranus.
        Stack (13 полей, подтверждён on-chain). bool в TON = -1.
        Если exit_code != 0 — адрес не является Meme-контрактом (фильтр Method 3)."""
        st = await self._run_method(meme, "get_meme_data")
        if not st or len(st) < 13:
            return None

        def num(i: int) -> int:
            return int(st[i].get("value", "0"), 16)

        return {
            "initialized": num(0) != 0,
            "migrated": num(1) != 0,          # ликвидность ушла на DEX
            "creator_fee": num(4) / 1e9,
            "is_graduated": num(6) != 0,      # достигнут порог, идёт миграция
            "alpha": num(7),
            "beta": num(8),
            "trade_fee_bps": num(10),
            "raised_funds": num(11) / 1e9,    # ТОЧНЫЙ собранный TON
            "current_supply_raw": num(12),
        }

    def _meme_price_ton(self, alpha: int, beta: int, supply_raw: int) -> float:
        """Линейная кривая TopBlast: price(s) = (alpha + beta*s/1e9) / 1e9 (в TON)."""
        PREC = 1_000_000_000
        return (alpha + (beta * supply_raw) // PREC) / 1e9

    # ── Парсер событий BuyEvent/SellEvent (опционально, точные суммы) ──────────
    def parse_meme_event(self, body_boc: str) -> Optional[dict]:
        """Разбирает тело external-out события Meme-контракта.
        Требует pytoniq-core; при его отсутствии возвращает None — монитор тогда
        берёт суммы из in-message трейса, а точное состояние из get_meme_data."""
        try:
            from pytoniq_core import Cell  # лёгкий парсер ячеек, без ноды
        except Exception:
            return None
        try:
            s = Cell.one_from_boc(body_boc).begin_parse()
            op = s.load_uint(32)
            if op not in (config.OP_BUY_EVENT, config.OP_SELL_EVENT):
                return None
            trader = s.load_address()
            amount_in = s.load_coins() or 0
            amount_out = s.load_coins() or 0
            fees = s.load_ref().begin_parse()
            fee_total = sum(filter(None, (fees.load_coins(), fees.load_coins(),
                                          fees.load_coins(), fees.load_coins())))
            supply = s.load_coins() or 0
            raised = s.load_coins() or 0
            is_buy = op == config.OP_BUY_EVENT
            return {
                "side": "buy" if is_buy else "sell",
                "trader": trader.to_str(is_user_friendly=True) if trader else "",
                "ton_amount": (amount_in if is_buy else amount_out) / 1e9,
                "jetton_amount": (amount_out if is_buy else amount_in) / 1e9,
                "fees": fee_total / 1e9,
                "current_supply_raw": supply,
                "raised_funds": raised / 1e9,
                "is_graduated": bool(s.load_uint(1)) if is_buy and s.remaining_bits else False,
            }
        except Exception as e:
            logger.debug("parse_meme_event: %s", e)
            return None

    # ── redo.trade: истина по mcap/price/ath/volume ───────────────────────────
    async def redo_data(self, ca: str) -> Optional[dict]:
        """jettonData с redo.trade — mcap/price/ath/volume/holders/status."""
        s = await self._sess()
        url = f"{config.REDO_API_BASE}/chart/{ca}"
        try:
            async with s.get(url, params={"chartTime": "1", "limit": "1"},
                             headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.warning("redo %s → %s", ca[:12], r.status)
                    return None
                d = await r.json(content_type=None)
                return d.get("jettonData")
        except Exception as e:
            logger.warning("redo %s: %s", ca[:12], e)
            return None

    def chart_png_url(self, ca: str, tf: str = "1m", ref: str | None = None) -> str:
        """URL PNG-графика redo.trade (страница отдаёт картинку при бот-UA)."""
        from urllib.parse import urlencode
        params = {"c": ca, "m": tf, "u": "dark", "t": str(int(time.time()))}
        if ref:
            params["r"] = ref
        return f"{config.REDO_CHART_BASE}?{urlencode(params)}"

    async def get_chart_png(self, ca: str, tf: str = "1m") -> Optional[bytes]:
        """Скачивает PNG-график redo.trade. Страница при бот-UA отдаёт картинку
        напрямую либо HTML с og:image — обрабатываем оба случая."""
        s = await self._sess()
        url = self.chart_png_url(ca, tf)
        headers = {"User-Agent": "TelegramBot (like TwitterBot)",
                   "Accept": "text/html,image/png,image/*"}
        try:
            async with s.get(url, headers=headers,
                             timeout=aiohttp.ClientTimeout(total=12)) as r:
                ct = (r.content_type or "").lower()
                if r.status == 200 and "image" in ct:
                    return await r.read()
                if r.status == 200 and "html" in ct:
                    import re
                    html = await r.text()
                    m = re.search(r'<meta[^>]+(?:property|name)=["\'](?:og|twitter):image'
                                  r'["\'][^>]*content=["\']([^"\']+)', html)
                    if m:
                        async with s.get(m.group(1), headers=headers,
                                         timeout=aiohttp.ClientTimeout(total=12)) as ir:
                            if ir.status == 200 and "image" in (ir.content_type or ""):
                                return await ir.read()
        except Exception as e:
            logger.warning("chart %s: %s", ca[:12], e)
        return None

    @staticmethod
    def to_friendly(raw: str) -> str:
        """raw 0:hex → friendly EQ.../UQ... (bounceable). Без pytoniq возвращает как есть."""
        try:
            from pytoniq_core import Address
            return Address(raw).to_str(is_user_friendly=True, is_bounceable=True)
        except Exception:
            return raw

    # ── курс TON/USD ──────────────────────────────────────────────────────────
    async def ton_usd(self) -> Optional[float]:
        now = time.monotonic()
        if self._ton_usd_cache and self._ton_usd_cache[1] > now:
            return self._ton_usd_cache[0]
        rate = None
        try:
            s = await self._sess()
            async with s.get(f"{config.DEXSCREENER_BASE}/latest/dex/tokens/{self._TON_REF}",
                             timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    pairs = (await r.json(content_type=None)).get("pairs") or []
                    if pairs:
                        usd, nat = float(pairs[0].get("priceUsd") or 0), float(pairs[0].get("priceNative") or 0)
                        if usd > 0 and nat > 0:
                            rate = usd / nat
        except Exception as e:
            logger.warning("ton_usd: %s", e)
        if rate:
            self._ton_usd_cache = (rate, now + 60)
        return rate

    # ── агрегатор: полная картина по токену ───────────────────────────────────
    async def snapshot(self, ca: str, platform_key: str,
                       event: dict | None = None) -> Optional[dict]:
        """Единый снимок токена. Для TopBlast (model=meme) истина — get_meme_data
        (точный raised_funds/supply/graduated), цена — линейная кривая.
        Для Stonks (model=bonding) — get_bonding_data. mcap уточняем из redo.trade.
        Если передан event (распарсенное BuyEvent/SellEvent), берём raised/supply
        из него — это самые свежие данные на момент сделки, без лишнего запроса."""
        plat = config.PLATFORMS.get(platform_key)
        target = plat.target_ton if plat else 1550.0
        model = plat.model if plat else "meme"

        meta = await self.get_meta(ca)
        rate = await self.ton_usd() or 0.0
        redo = await self.redo_data(ca)
        dec = meta["decimals"]

        collected = progress = price = mcap = 0.0
        graduated = False
        curve_addr = None

        if model == "meme":
            md = await self.get_meme_data(ca)
            if md:
                raised = event["raised_funds"] if event and event.get("raised_funds") else md["raised_funds"]
                supply_raw = event["current_supply_raw"] if event and event.get("current_supply_raw") else md["current_supply_raw"]
                collected = raised
                progress = min(raised / target * 100.0, 100.0) if target else 0.0
                graduated = md["is_graduated"] or md["migrated"] or (event or {}).get("is_graduated", False)
                price = self._meme_price_ton(md["alpha"], md["beta"], supply_raw) * rate
        else:  # bonding (Stonks)
            bd = await self.read_bonding(ca)
            if bd:
                curve_addr = bd["curve"]
                progress = min(bd["progress"], 100.0)
                collected = bd["collected"] or (progress * target / 100.0)
                graduated = not bd["presale"] or progress >= 100.0
                jet_res = bd["jet_reserve_raw"] / (10 ** dec)
                if jet_res > 0:
                    price = (bd["ton_reserve"] / jet_res) * rate

        # mcap: redo — истина; иначе считаем от цены и саплая
        if redo and (redo.get("marketCap") or redo.get("mcap")):
            mcap = float(redo.get("marketCap") or redo.get("mcap") or 0)
            price = float(redo.get("price") or redo.get("priceUsd") or price)
        elif price and meta["total_supply"]:
            mcap = price * (meta["total_supply"] / (10 ** dec))

        return {
            "ca": ca, "platform": platform_key,
            "symbol": meta["symbol"], "name": meta["name"], "decimals": dec,
            "image_url": meta["image_url"], "curve_addr": curve_addr,
            "target_ton": target, "collected_ton": collected,
            "progress": progress, "mcap_usd": mcap, "price_usd": price,
            "graduated": graduated,
        }
