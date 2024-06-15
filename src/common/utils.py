import aiohttp


async def fetch_url(url: str):
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as res:
            return await res.json()