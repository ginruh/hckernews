import asyncio
from urllib.parse import urljoin
from sqlalchemy import Engine

from .db import Item, get_item, save_items
from .utils import fetch_url
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession


class HackerNews:
    base_url: str = "https://hacker-news.firebaseio.com"
    session: async_sessionmaker[AsyncSession]

    @classmethod
    def set_session(cls, session: async_sessionmaker[AsyncSession]) -> None:
        cls.session = session

    @classmethod
    async def get_latest_item(cls) -> int | None:
        try:
            return await fetch_url(url=urljoin(cls.base_url, "v0/maxitem.json"))
        except Exception:
            return None

    @classmethod
    async def fetch_item(cls, item_id: int):
        try:
            return await fetch_url(urljoin(cls.base_url, f"v0/item/{item_id}.json"))
        except Exception:
            return None

    @classmethod
    async def fetch_story_items(
        cls, *, start_item: int = 0, end_item: int, batch_size: int = 20000
    ):
        for i in range(start_item, end_item, batch_size):
            # check for items already saved
            batched_items = []
            for new_item in [i + c for c in range(batch_size)]:
                item_instance = await get_item(
                    async_session=cls.session, item_id=new_item
                )
                if item_instance is None:
                    batched_items.append(new_item)
            # fetch items not saved
            items_tasks = [
                asyncio.create_task(cls.fetch_item(item_id))
                for item_id in batched_items
            ]
            items_responses = await asyncio.gather(*items_tasks)
            # filter story items
            story_items = [
                story_item
                for story_item in items_responses
                if story_item is not None and story_item["type"] == "story"
            ]
            # save them
            items_instances = await save_items(
                async_session=cls.session, items=story_items
            )
            # fetch comments
            await cls.fetch_comments(items=items_instances)
            print(f"Saved stories: {[story_item.id for story_item in items_instances]}")

    @classmethod
    async def bulk_fetch_items(cls, *, item_ids: list[int], batch_size: int = 20):
        start_item = 0
        end_item = len(item_ids)
        fetched_items = []
        for i in range(start_item, end_item, batch_size):
            batched_items = [i + c for c in range(batch_size)]
            items_tasks = [
                asyncio.create_task(cls.fetch_item(item_ids[i])) for i in batched_items
            ]
            items = await asyncio.gather(*items_tasks)
            fetched_items.extend(items)
        return fetched_items

    @classmethod
    async def fetch_comments(cls, *, items: list[Item], batch_size=20000):
        # doing sort of breadth traversal and batching them
        if len(items) == 0:
            return
        comment_item_ids = []
        for item in items:
            comment_item_ids.extend(item.kids)
        for i in range(0, len(comment_item_ids), batch_size):
            batched_comments = [
                comment_item_ids[c_i]
                for c_i in range(i, i + batch_size)
                if c_i < len(comment_item_ids)
            ]
            comment_tasks = [
                asyncio.create_task(cls.fetch_item(item_id))
                for item_id in batched_comments
            ]
            comment_responses = await asyncio.gather(*comment_tasks)
            comment_items = await save_items(
                async_session=cls.session, items=comment_responses
            )
            await cls.fetch_comments(items=comment_items)

    @classmethod
    async def fetch_top_stories(cls) -> list[int]:
        return await fetch_url(urljoin(cls.base_url, f"v0/topstories.json"))

    @classmethod
    async def save_items(cls, items: list[dict | None]):
        return await save_items(async_session=cls.session, items=items)
