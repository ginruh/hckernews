import asyncio
from typing import cast
from dotenv import load_dotenv
from os import getenv

from src.common.db import connect_db
from src.common.hackernews import HackerNews


def main():
    asyncio.run(run())


async def run():
    load_dotenv()
    engine = connect_db(
        mongodb_uri=cast(str, getenv("MONGODB_URI")),
        database=cast(str, getenv("MONGODB_DATABASE")),
    )
    HackerNews.set_engine(engine=engine)
    await fetch_top_stories()


async def fetch_top_stories():
    top_stories = await HackerNews.fetch_top_stories()
    print(top_stories)
