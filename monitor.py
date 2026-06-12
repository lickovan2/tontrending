"""Индексатор реального времени (точная on-chain модель TopBlast).

Подписка (TonAPI SSE traces) на ДВА типа адресов:
  • фабрики          → ловим деплой нового токена (op 0x6ff416dc);
  • Meme-контракты   → ловим события сделок BuyEvent/SellEvent
                       (external-out, op 0xa0aa6bc2 / 0x3ab0fccc).

Почему так: трейды происходят на самих Meme-контрактах, а не на фабрике —
поэтому набор подписки динамический: при обнаружении нового токена его адрес
добавляется в подписку и SSE переподключается (debounce).

Точность без эвристик:
  • факт сделки и сторона  → опкод события из out-message Meme-контракта;
  • точный собранный TON    → raisedFunds прямо из тела события (или get_meme_data);
  • трейдер и сумма         → из тела события, фолбэк — из in-message трейда.
Прогресс = raisedFunds / target × 100 → проверка майлстоунов 25/50/75/100%.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import aiohttp

import config
import db
from notify import Notifier
from ton_source import TonSource

logger = logging.getLogger("ogelite.monitor")


def _addr(node: dict | None) -> str:
    return (node or {}).get("address", "") if isinstance(node, dict) else ""


def _op_int(op) -> Optional[int]:
    if op is None:
        return None
    try:
        return int(op, 16) if isinstance(op, str) else int(op)
    except (ValueError, TypeError):
        return None


class Monitor:
    def __init__(self, ton: TonSource, notifier: Notifier) -> None:
        self.ton = ton
        self.notifier = notifier
        self._accounts: set[str] = set(config.FACTORY_TO_PLATFORM.keys())
        self._resub = asyncio.Event()         # сигнал «пора переподключить SSE»
        self._progress: dict[str, float] = {}  # последний прогресс в памяти

    # ── управление подпиской ──────────────────────────────────────────────────
    def _add_account(self, addr: str) -> None:
        if addr and addr not in self._accounts:
            self._accounts.add(addr)
            self._resub.set()
            logger.info("подписка +%s (всего %d)", addr[:12], len(self._accounts))

    async def seed(self) -> None:
        """Стартовый посев: известные токены из БД + скан недавних деплоев фабрик."""
        async with db.Session() as s:
            rows = await s.execute(db.select(db.Token.ca))
            for ca in rows.scalars():
                self._accounts.add(ca)
        # скан событий фабрик → найти живые Meme-контракты и наполнить БД снапшотами,
        # чтобы дашборд показывал токены сразу, не дожидаясь первой сделки.
        for fact, plat in config.FACTORY_TO_PLATFORM.items():
            data = await self.ton._ta_get(f"/accounts/{fact}/events", {"limit": 50})
            for cand in self._extract_addresses(data):
                if not await self._is_meme(cand, plat.key):
                    continue
                self._add_account(cand)
                try:
                    snap = await self.ton.snapshot(cand, plat.key)
                    if snap:
                        async with db.Session() as s:
                            await self._upsert_token(s, snap)
                            self._progress[cand] = snap["progress"]
                            await s.commit()
                except Exception:
                    logger.debug("seed snapshot %s не удался", cand[:12])
        logger.info("seed завершён: %d адресов в подписке", len(self._accounts))

    def _extract_addresses(self, obj, out: set[str] | None = None) -> set[str]:
        out = out if out is not None else set()
        if isinstance(obj, dict):
            a = obj.get("address")
            if a and obj.get("is_wallet") is False and a not in config.FACTORY_TO_PLATFORM:
                out.add(a)
            for v in obj.values():
                self._extract_addresses(v, out)
        elif isinstance(obj, list):
            for v in obj:
                self._extract_addresses(v, out)
        return out

    async def _is_meme(self, addr: str, platform_key: str) -> bool:
        plat = config.PLATFORMS.get(platform_key)
        if plat and plat.model == "meme":
            return await self.ton.get_meme_data(addr) is not None
        return await self.ton.read_bonding(addr) is not None

    # ── главный цикл SSE с переподключением при росте подписки ─────────────────
    async def run(self) -> None:
        await self.seed()
        headers = {"Authorization": f"Bearer {config.TONAPI_KEY}"} if config.TONAPI_KEY else {}
        while True:
            accounts = ",".join(sorted(self._accounts))
            url = f"{config.TONAPI_SSE}/accounts/traces?accounts={accounts}"
            self._resub.clear()
            try:
                logger.info("SSE connect (%d аккаунтов)", len(self._accounts))
                s = await self.ton._sess()
                async with s.get(url, headers=headers,
                                 timeout=aiohttp.ClientTimeout(total=None)) as r:
                    async for raw in r.content:
                        if self._resub.is_set():
                            logger.info("подписка изменилась — переподключение")
                            break
                        line = raw.decode("utf-8", "ignore").strip()
                        if not line.startswith("data:"):
                            continue
                        payload = line[5:].strip()
                        if not payload or payload == "heartbeat":
                            continue
                        import json
                        try:
                            evt = json.loads(payload)
                        except json.JSONDecodeError:
                            continue
                        asyncio.create_task(self._safe_handle(evt.get("tx_hash", "")))
            except Exception as e:
                logger.warning("SSE оборвался: %s — реконнект через 3с", e)
                await asyncio.sleep(3)

    async def _safe_handle(self, tx_hash: str) -> None:
        try:
            if tx_hash:
                await self.on_trace(tx_hash)
        except Exception:
            logger.exception("ошибка обработки трейса %s", tx_hash)

    # ── обработка одного трейса ────────────────────────────────────────────────
    async def on_trace(self, tx_hash: str) -> None:
        trace = await self.ton._ta_get(f"/traces/{tx_hash}")
        if not trace:
            return
        parsed = self._classify(trace)
        if not parsed:
            return
        token_ca, platform_key, kind, trade = parsed

        async with db.Session() as session:
            is_new = (await session.get(db.Token, token_ca)) is None

            # точный снимок (для meme-модели использует event-данные, если есть)
            snap = await self.ton.snapshot(token_ca, platform_key, event=trade)
            if not snap:
                return
            await self._upsert_token(session, snap)
            await session.commit()

            if is_new:                       # новый токен → подписываемся на него
                self._add_account(token_ca)
            await self._check_milestones(session, snap)
            await session.commit()

    # ── классификация трейса ──────────────────────────────────────────────────
    def _classify(self, trace: dict):
        """Возвращает (token_ca, platform_key, kind, trade|None) или None."""
        txs: list[dict] = []
        self._collect_txs(trace, txs)

        deploy_factory: Optional[str] = None
        event: Optional[dict] = None      # распарсенное BuyEvent/SellEvent
        meme_from_event: Optional[str] = None
        action_msg: Optional[dict] = None  # исходное buy/sell сообщение пользователя

        for tx in txs:
            acc = _addr(tx.get("account"))
            in_msg = tx.get("in_msg") or {}
            in_op = _op_int(in_msg.get("op_code"))

            # деплой нового токена на фабрику
            if acc in config.FACTORY_TO_PLATFORM and in_op == config.OP_DEPLOY:
                deploy_factory = acc
            # пользовательское действие (для фолбэка по сумме/трейдеру)
            if in_op in (config.OP_BUY, config.OP_SELL):
                action_msg = in_msg
            # событие сделки в out-сообщениях Meme-контракта
            for om in (tx.get("out_msgs") or []):
                if _op_int(om.get("op_code")) in config.TRADE_EVENT_OPS:
                    meme_from_event = acc
                    body = om.get("raw_body") or om.get("body")
                    if body:
                        event = self.ton.parse_meme_event(body)

        # 1) сделка
        if meme_from_event:
            token_ca = meme_from_event
            platform_key = self._platform_of(token_ca)
            trade = self._build_trade(event, action_msg, trace)
            return token_ca, platform_key, "trade", trade

        # 2) деплой — найдём адрес нового Meme среди аккаунтов трейса
        if deploy_factory:
            plat = config.FACTORY_TO_PLATFORM[deploy_factory]
            for tx in txs:
                acc = _addr(tx.get("account"))
                if acc and acc not in config.FACTORY_TO_PLATFORM:
                    return acc, plat.key, "deploy", None
        return None

    def _collect_txs(self, node, out: list) -> None:
        if isinstance(node, dict):
            if "transaction" in node and isinstance(node["transaction"], dict):
                out.append(node["transaction"])
            elif "account" in node and ("in_msg" in node or "out_msgs" in node):
                out.append(node)
            for child in node.get("children") or []:
                self._collect_txs(child, out)

    def _platform_of(self, ca: str) -> str:
        # точное определение делает snapshot; здесь дефолт meme-площадки
        return "topblast"

    def _build_trade(self, event: Optional[dict], action_msg: Optional[dict],
                     trace: dict) -> dict:
        """Точные данные из события; если pytoniq-core нет — фолбэк на in-message."""
        tx_hash = (trace.get("transaction") or trace).get("hash", "") or \
            trace.get("trace_id", "") or ""
        if event:
            return {**event, "tx_hash": tx_hash, "lt": 0}
        am = action_msg or {}
        side = "buy" if _op_int(am.get("op_code")) == config.OP_BUY else "sell"
        value = int(am.get("value") or 0) / 1e9
        return {
            "side": side,
            "trader": _addr(am.get("source")),
            "ton_amount": max(value - 0.3, 0.0),  # минус ~0.3 TON газа
            "jetton_amount": 0.0,
            "tx_hash": tx_hash, "lt": 0,
        }

    # ── запись в БД ────────────────────────────────────────────────────────────
    async def _upsert_token(self, s, snap: dict) -> None:
        tok = await s.get(db.Token, snap["ca"])
        if tok is None:
            tok = db.Token(ca=snap["ca"])
            s.add(tok)
        for k in ("platform", "symbol", "name", "decimals", "image_url", "curve_addr",
                  "target_ton", "collected_ton", "progress", "mcap_usd", "price_usd", "graduated"):
            setattr(tok, k, snap[k])

    # ── майлстоуны ──────────────────────────────────────────────────────────────
    async def _check_milestones(self, s, snap: dict) -> None:
        ca, progress = snap["ca"], snap["progress"]
        prev = self._progress.get(ca, 0.0)
        self._progress[ca] = progress
        for m in config.MILESTONES:
            if progress >= m > prev:
                if await db.claim_milestone(s, ca, m):
                    await self.notifier.notify_milestone(s, snap, m)
