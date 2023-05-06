import httpx

API_BASE_URL = "https://coins.llama.fi"


async def base_request(path):
    async with httpx.AsyncClient() as client:
        r = await client.get(API_BASE_URL + path)
    return r.json()


async def get_historical_prices(timestamp, coins):
    path = "/prices/historical/{timestamp}/{coins}"
    return await base_request(path.format(timestamp=timestamp, coins=coins))


async def get_current_prices(coins):
    path = "/prices/current/{coins}"
    return await base_request(path.format(coins=coins))
