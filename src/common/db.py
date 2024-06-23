from datetime import datetime
from typing import Optional
from sqlalchemy import ForeignKey, select, func, ARRAY, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.dialects.postgresql import Insert


class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "item"

    id: Mapped[int] = mapped_column(primary_key=True)
    deleted: Mapped[bool] = mapped_column(default=False)
    type: Mapped[str]
    by: Mapped[Optional[str]] = mapped_column(nullable=True)
    time: Mapped[int]
    dead: Mapped[bool] = mapped_column(default=False)
    parent: Mapped[Optional[int]] = mapped_column(ForeignKey("item.id"), nullable=True)
    kids: Mapped[list[int]] = mapped_column(ARRAY(Integer), default=[])
    url: Mapped[Optional[str]] = mapped_column(nullable=True)
    score: Mapped[Optional[int]] = mapped_column(nullable=True)
    title: Mapped[Optional[str]] = mapped_column(nullable=True)  # null for comment
    text: Mapped[Optional[str]] = mapped_column(nullable=True)  # maybe null for story
    descendants: Mapped[Optional[int]] = mapped_column(default=0)


class TopStories(Base):
    __tablename__ = "top_stories"

    story_item_id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        primary_key=True, server_default=func.now()
    )


async def connect_db(postgres_uri: str) -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(postgres_uri)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    return async_sessionmaker(engine, expire_on_commit=False)


async def save_items(
    *, async_session: async_sessionmaker[AsyncSession], items: list[dict | None]
) -> list[Item]:
    if len(items) == 0:
        return []
    filtered_items: list[dict] = []
    for item in items:
        # removing failed request items
        if item is None:
            continue
        # remove item with no type
        if item.get("type") is None:
            continue
        # not interested in jobs and polls
        if item.get("type") != "story" and item.get("type") != "comment":
            continue
        if item.get("type") == "story":
            # removing spams
            if item.get("score", 0) >= 3:
                filtered_items.append(item)
        if item.get("type") == "comment":
            filtered_items.append(item)
    hn_items = [
        dict(
            id=f_item["id"],
            type=f_item["type"],
            time=f_item["time"],
            by=f_item.get("by", None),
            dead=f_item.get("dead", False),
            deleted=f_item.get("deleted", False),
            descendants=f_item.get("descendants", 0),
            kids=f_item.get("kids", []),
            parent=f_item.get("parent", None),
            score=f_item.get("score", None),
            text=f_item.get("text", None),
            title=f_item.get("title", None),
            url=f_item.get("url", None),
        )
        for f_item in filtered_items
    ]
    async with async_session() as session:
        async with session.begin():
            stmt = Insert(Item).values(hn_items)
            result = await session.scalars(
                stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "type": stmt.excluded.type,
                        "time": stmt.excluded.time,
                        "by": stmt.excluded.by,
                        "dead": stmt.excluded.dead,
                        "deleted": stmt.excluded.deleted,
                        "descendants": stmt.excluded.descendants,
                        "kids": stmt.excluded.kids,
                        "parent": stmt.excluded.parent,
                        "score": stmt.excluded.score,
                        "text": stmt.excluded.text,
                        "title": stmt.excluded.title,
                        "url": stmt.excluded.url,
                    },
                ).returning(Item)
            )
            return list(result.all())


async def get_item(*, async_session: async_sessionmaker[AsyncSession], item_id: int):
    async with async_session() as session:
        stmt = select(Item).where(Item.id == item_id)
        items = await session.scalars(stmt)
        return items.first()


async def save_top_stories(
    *, async_session: async_sessionmaker[AsyncSession], stories: list[int]
):
    top_stories = [TopStories(story_item_id=story_item_id) for story_item_id in stories]
    async with async_session() as session:
        async with session.begin():
            session.add_all(top_stories)


async def bulk_find_items(
    *, async_session: async_sessionmaker[AsyncSession], items: list[int]
):
    async with async_session() as session:
        async with session.begin():
            result = await session.scalars(select(Item).where(Item.id.in_(items)))
            return list(result.all())
