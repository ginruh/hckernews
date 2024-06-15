import asyncio
from typing import cast
from dotenv import load_dotenv
from os import getenv
from src.db import connect_db
from src.hackernews import HackerNews


async def main():
    load_dotenv()
    engine = connect_db(
        mongodb_uri=cast(str, getenv("MONGODB_URI")),
        database=cast(str, getenv("MONGODB_DATABASE")),
    )
    HackerNews.set_engine(engine=engine)
    latest_item_id = await HackerNews.get_latest_item()
    print(f"Latest item ID: {latest_item_id}")
    if latest_item_id is None:
        raise Exception("Unable to fetch latest item_id. Exiting")
    tasks = [
        # asyncio.create_task(HackerNews.fetch_story_items(end_item=latest_item_id)),
        asyncio.create_task(HackerNews.listen_updates()),
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())
