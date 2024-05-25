import asyncio
import aiohttp
from urllib.parse import urljoin
from odmantic import AIOEngine

from src.db import Item, get_item, save_items


async def fetch_url(url: str):
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as res:
            return await res.json()


class HackerNews:
    base_url: str = "https://hacker-news.firebaseio.com"
    engine: AIOEngine

    @classmethod
    def set_engine(cls, engine: AIOEngine) -> None:
        cls.engine = engine

    @classmethod
    async def get_latest_item(cls) -> int | None:
        try:
            return await fetch_url(url=urljoin(cls.base_url, "v0/maxitem.json"))
        except Exception:
            return None

    @classmethod
    async def listen_updates(cls, batch_size=20):
        await asyncio.sleep(30)
        while True:
            response = await fetch_url(url=urljoin(cls.base_url, "v0/updates.json"))
            items = response["items"]
            for i in range(0, len(items), batch_size):
                batched_items = [
                    items[item_i]
                    for item_i in range(i, i + batch_size)
                    if item_i < len(items)
                ]
                items_task = [
                    asyncio.create_task(cls.fetch_item(item_id))
                    for item_id in batched_items
                ]
                items_responses = await asyncio.gather(*items_task)
                story_items = [
                    story_item
                    for story_item in items_responses
                    if story_item is not None and story_item["type"] == "story"
                ]
                items_instances = await save_items(engine=cls.engine, items=story_items)
                await cls.fetch_comments(items=items_instances)
                print(
                    f"Saved updated stories: {[story_item.id for story_item in items_instances]}"
                )
            await asyncio.sleep(60)

    @classmethod
    async def fetch_item(cls, item_id: int):
        try:
            return await fetch_url(urljoin(cls.base_url, f"v0/item/{item_id}.json"))
        except Exception:
            return None

    @classmethod
    async def fetch_story_items(
        cls, *, start_item: int = 1700000, end_item: int, batch_size: int = 20000
    ):
        for i in range(start_item, end_item, batch_size):
            # check for items already saved
            batched_items = []
            for new_item in [i + c for c in range(batch_size)]:
                item_instance = await get_item(engine=cls.engine, item_id=new_item)
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
            items_instances = await save_items(engine=cls.engine, items=story_items)
            # fetch comments
            await cls.fetch_comments(items=items_instances)
            print(f"Saved stories: {[story_item.id for story_item in items_instances]}")

    @classmethod
    async def fetch_comments(cls, *, items: list[Item], batch_size=20000):
        # doing sort of breadth traversal and batching them
        if len(items) == 0:
            return
        comment_item_ids = []
        for item in items:
            for c_id in item.kids:
                c_instance = await get_item(engine=cls.engine, item_id=c_id)
                if c_instance is None:
                    comment_item_ids.append(c_id)
        print(f"Total comments found: {len(comment_item_ids)}")
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
            comment_items = await save_items(engine=cls.engine, items=comment_responses)
        await cls.fetch_comments(items=comment_items)
