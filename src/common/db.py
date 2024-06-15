from typing import Optional
from odmantic import AIOEngine, Field, Model
from motor.motor_asyncio import AsyncIOMotorClient


class Item(Model):
    id: int = Field(primary_field=True)
    deleted: Optional[bool] = False
    type: str
    by: Optional[str] = None
    time: int
    dead: Optional[bool] = False
    parent: Optional[int]
    kids: list[int]
    url: Optional[str] = None
    score: Optional[int] = None
    title: Optional[str] = None  # null for comment
    text: Optional[str] = None  # maybe null for story
    descendants: Optional[int] = 0


def connect_db(mongodb_uri: str, database: str) -> AIOEngine:
    client = AsyncIOMotorClient(mongodb_uri)
    return AIOEngine(client=client, database=database)


async def save_items(*, engine: AIOEngine, items: list[dict | None]) -> list[Item]:
    filtered_items: list[dict] = []
    for item in items:
        # removing failed request items
        if item is None:
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
    return await engine.save_all(
        [
            Item(
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
    )


async def get_item(*, engine: AIOEngine, item_id: int):
    return await engine.find_one(Item, Item.id == item_id)
