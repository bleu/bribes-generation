# bots/lib/gql.py
import json

import httpx


class GraphQLError(Exception):
    pass


async def gql(url, query, variables={}):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            url,
            json=dict(query=query, variables=variables),
        )

    try:
        return r.json()["data"]
    except KeyError:
        print(json.dumps(r.json(), indent=2))
        raise GraphQLError
