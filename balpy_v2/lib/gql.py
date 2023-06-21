import json

import httpx


class GraphQLError(Exception):
    pass


import logging


async def gql(url, query, variables={}):
    logging.debug(f"Executing query: {query[:15]}")
    logging.debug(f"URL: {url}")
    logging.debug(f"Variables: {variables}")
    async with httpx.AsyncClient() as client:
        r = await client.post(
            url,
            json=dict(query=query, variables=variables),
        )
        logging.debug(f"Response status: {r.status_code}")
        logging.debug(f"Response body: {r.text}")
        r.raise_for_status()

    try:
        return r.json().get("data", r.json())
    except KeyError:
        print(json.dumps(r.json(), indent=2))
        raise GraphQLError
