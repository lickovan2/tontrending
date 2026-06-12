"""Схема БД (async SQLAlchemy 2.0). Только то, что нужно для майлстоунов:

  tokens            — снимок состояния токена (прогресс сбора, mcap, кривая)
  token_milestones  — какие майлстоуны уже отправлены (антидубль)
  group_topics      — привязка «процент сбора → message_thread_id» в супергруппе
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger, Boolean, DateTime, Float, ForeignKey, Integer, String,
    UniqueConstraint, func, select,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import config


class Base(DeclarativeBase):
    pass


class GroupTopic(Base):
    """Привязка майлстоуна сбора к теме (Topic) супергруппы. message_thread_id —
    id ветки, куда уходит уведомление про этот процент. Заполняется /bind."""
    __tablename__ = "group_topics"
    __table_args__ = (UniqueConstraint("chat_id", "milestone", name="uq_chat_milestone"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    milestone: Mapped[int] = mapped_column(Integer)              # 25/50/75/100
    message_thread_id: Mapped[int] = mapped_column(Integer)      # id темы
    label: Mapped[Optional[str]] = mapped_column(String(64))


class Token(Base):
    __tablename__ = "tokens"

    ca: Mapped[str] = mapped_column(String(70), primary_key=True)  # jetton master (friendly)
    platform: Mapped[str] = mapped_column(String(16), index=True)  # topblast/uranus/stonks
    symbol: Mapped[Optional[str]] = mapped_column(String(32))
    name: Mapped[Optional[str]] = mapped_column(String(128))
    decimals: Mapped[int] = mapped_column(Integer, default=9)
    image_url: Mapped[Optional[str]] = mapped_column(String(512))
    curve_addr: Mapped[Optional[str]] = mapped_column(String(70))

    target_ton: Mapped[float] = mapped_column(Float, default=1550.0)
    collected_ton: Mapped[float] = mapped_column(Float, default=0.0)
    progress: Mapped[float] = mapped_column(Float, default=0.0)     # 0..100
    mcap_usd: Mapped[float] = mapped_column(Float, default=0.0)
    price_usd: Mapped[float] = mapped_column(Float, default=0.0)
    graduated: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class TokenMilestone(Base):
    """Антидубль: майлстоун X% по токену уже отправлен."""
    __tablename__ = "token_milestones"
    __table_args__ = (UniqueConstraint("token_ca", "milestone", name="uq_token_milestone"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    token_ca: Mapped[str] = mapped_column(ForeignKey("tokens.ca", ondelete="CASCADE"), index=True)
    milestone: Mapped[int] = mapped_column(Integer)
    reached_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ── Движок / сессии ─────────────────────────────────────────────────────────
engine = create_async_engine(config.DATABASE_URL, echo=False, pool_pre_ping=True)
Session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def topic_for_milestone(s: AsyncSession, chat_id: int, milestone: int) -> Optional[int]:
    """message_thread_id темы для данного процента (или None)."""
    return await s.scalar(
        select(GroupTopic.message_thread_id).where(
            GroupTopic.chat_id == chat_id, GroupTopic.milestone == milestone
        )
    )


async def claim_milestone(s: AsyncSession, token_ca: str, milestone: int) -> bool:
    """Атомарно «забирает» майлстоун. True — впервые (нужно слать), False — уже был."""
    exists = await s.scalar(
        select(TokenMilestone.id).where(
            TokenMilestone.token_ca == token_ca, TokenMilestone.milestone == milestone
        )
    )
    if exists:
        return False
    s.add(TokenMilestone(token_ca=token_ca, milestone=milestone))
    return True
