import httpx
import asyncio
import json
import logging
from httpx import HTTPStatusError
import asyncio
from functools import wraps
import logging


def retry_on_rate_limit(max_retries=5, rate_limit_status_code=429):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for i in range(max_retries):
                response = await func(*args, **kwargs)
                if response.status_code != rate_limit_status_code:
                    return response

                retry_after = int(response.headers.get("Retry-After", 1))
                logging.info(
                    f"Rate limit exceeded. Retrying after {retry_after} seconds."
                )
                await asyncio.sleep(retry_after)
            return response

        return wrapper

    return decorator


class RateLimitError(Exception):
    pass


class RequestError(Exception):
    pass


class LlamaAPIClient:
    MAX_CONCURRENT_REQUESTS = 50
    LLAMA_API_URL = "https://coins.llama.fi/batchHistorical"
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    def __init__(self):
        self.client = httpx.AsyncClient()

    @retry_on_rate_limit()
    async def _get(self, url, **kwargs):
        async with self.semaphore:
            try:
                r = await self.client.get(url, **kwargs)
                r.raise_for_status()
            except HTTPStatusError as e:
                print(e.response.text)
                raise RequestError from e
            return r.json()

    async def single_request(self, batch_coins, search_width):
        response = await self._get(
            self.LLAMA_API_URL,
            params={"coins": json.dumps(batch_coins), "searchWidth": search_width},
        )
        return response

    async def batch_request(self, coins_dict, search_width=300, batch_size=50):
        tasks = []

        for token, timestamps in coins_dict.items():
            n = len(timestamps)
            for i in range(0, n, batch_size):
                batch_timestamps = timestamps[i : i + batch_size]
                batch_coins = {token: batch_timestamps}
                logging.info(
                    f"Adding task for {token} with {len(batch_timestamps)} items"
                )
                tasks.append(self.single_request(batch_coins, search_width))

        logging.info(f"Starting batch request with {len(tasks)} tasks...")
        try:
            results = await asyncio.gather(*tasks)
        except Exception as e:
            logging.info(f"Batch request failed with exception: {e}")
            raise e

        logging.info(f"Batch request finished. {len(results)} results received.")
        return results
