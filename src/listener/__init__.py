import asyncio
from typing import cast
from urllib.parse import urljoin
from dotenv import load_dotenv
from os import getenv

from src.common.db import connect_db
from src.common.hackernews import HackerNews
from src.common.utils import fetch_url


def main():
    asyncio.run(run())


async def run():
    load_dotenv()
    session = await connect_db(
        postgres_uri=cast(str, getenv("POSTGRES_URI")),
    )
    HackerNews.set_session(session=session)
    await listen_updates()


async def listen_updates(batch_size=20):
    while True:
        response = await fetch_url(url=urljoin(HackerNews.base_url, "v0/updates.json"))
        items = response["items"]
        for i in range(0, len(items), batch_size):
            batched_items = [
                items[item_i]
                for item_i in range(i, i + batch_size)
                if item_i < len(items)
            ]
            items_task = [
                asyncio.create_task(HackerNews.fetch_item(item_id))
                for item_id in batched_items
            ]
            items_responses = await asyncio.gather(*items_task)
            story_items = [
                story_item
                for story_item in items_responses
                if story_item is not None and story_item["type"] == "story"
            ]
            items_instances = await HackerNews.save_items(items=story_items)
            await HackerNews.fetch_comments(items=items_instances)
            print(
                f"Saved updated stories: {[story_item.id for story_item in items_instances]}"
            )
        await asyncio.sleep(60)
