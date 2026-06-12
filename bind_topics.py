"""Разовый скрипт: прописать привязки тем группы → майлстоунам в БД.
Запуск: .venv/Scripts/python.exe bind_topics.py
Соответствие веток @GramTrendTon: 3→25%, 5→50%, 7→75%, 9→100% (бонд)."""
import asyncio
import config
import db

MAPPING = {25: 3, 50: 5, 75: 7, 100: 9}


async def main() -> None:
    await db.init_db()
    async with db.Session() as s:
        for milestone, thread_id in MAPPING.items():
            existing = await s.scalar(
                db.select(db.GroupTopic).where(
                    db.GroupTopic.chat_id == config.GROUP_CHAT_ID,
                    db.GroupTopic.milestone == milestone,
                )
            )
            if existing:
                existing.message_thread_id = thread_id
            else:
                s.add(db.GroupTopic(chat_id=config.GROUP_CHAT_ID,
                                    milestone=milestone, message_thread_id=thread_id))
        await s.commit()
        rows = await s.execute(db.select(db.GroupTopic))
        for g in rows.scalars():
            print(f"chat={g.chat_id} {g.milestone}% → thread {g.message_thread_id}")


if __name__ == "__main__":
    asyncio.run(main())
