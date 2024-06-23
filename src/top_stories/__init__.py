import asyncio
from typing import cast
from dotenv import load_dotenv
from os import getenv

from src.common.db import bulk_find_items, connect_db, save_top_stories
from src.common.hackernews import HackerNews


def main():
    asyncio.run(run())


async def run():
    load_dotenv()
    session = await connect_db(
        postgres_uri=cast(str, getenv("POSTGRES_URI")),
    )
    HackerNews.set_session(session=session)
    await sync_top_stories()


async def sync_top_stories():
    top_stories = await HackerNews.fetch_top_stories()
    top_story_items = await HackerNews.bulk_fetch_items(item_ids=top_stories)
    story_items = [
        tsi for tsi in top_story_items if tsi is not None and tsi.get("type") == "story"
    ]
    filtered_items = await bulk_find_items(
        async_session=HackerNews.session, items=[i.get("id") for i in story_items]
    )
    await save_top_stories(
        async_session=HackerNews.session, stories=[item.id for item in filtered_items]
    )
    print("Top stories synced successfully")
