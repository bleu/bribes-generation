import re
from pprint import pprint
import pandas as pd
import asyncio

import logging
from pprint import pprint


import pandas as pd
from balpy_v2.lib import flatten_json

# Important assumption:
# USD value here is considered form Balancer, not from Coingecko or Llama
# A major improvement would be to use Coingecko or Llama to get instantaneous the USD value
from balpy_v2.lib.gql import gql

from joblib import Memory

from fees_reporting.cycle import generate_cycles_until_now

cachedir = ".balpy_cache"
memory = Memory(cachedir, verbose=0)

logging.basicConfig(level=logging.INFO)
from balpy_v2.subgraphs.client import GraphQLClient
from balpy_v2.subgraphs.query import GraphQLQuery
from balpy_v2.lib import Chain

BASE_URL = "https://api.thegraph.com/subgraphs/name/bleu-studio"

BALANCER_MAINNET_SUBGRAPH_URL_MAP = {
    Chain.mainnet: BASE_URL + "/balancer-mainnet-v2",
    Chain.polygon: BASE_URL + "/balancer-polygon-v2",
    Chain.arbitrum: BASE_URL + "/balancer-arbitrum-v2",
    Chain.gnosis: BASE_URL + "/balancer-gnosis-v2",
    Chain.optimism: BASE_URL + "/balancer-optimism-v2",
    # TODO: missing subgraph link in https://docs.balancer.fi/reference/subgraph/
    # avalanche="",
}


class BalancerSubgraph(GraphQLClient):
    def get_url(self, chain):
        return BALANCER_MAINNET_SUBGRAPH_URL_MAP[chain]


class BalancerSubgraphQuery(GraphQLQuery):
    def get_client(self):
        return BalancerSubgraph(self.chain)


class SwapsQuery(BalancerSubgraphQuery):
    def get_query(self):
        return """query MySwaps ($skip: Int, $block: Int, $after: Int, $before: Int, $poolId: ID!) {
  swaps(first:1000, skip: $skip, where:{timestamp_lt: $before, timestamp_gt: $after, poolId: $poolId, swapFeesUSD_not: "0"}, orderBy: timestamp) {
    id
    valueUSD
    swapFeesUSD
    timestamp
    tokenAmountIn
    tokenAmountOut
    tokenIn
    tokenOut
    tx
    pool: poolId {
      id
    }
  }
}"""


class JoinsQuery(BalancerSubgraphQuery):
    def get_query(self):
        return """query JoinExits ($skip: Int, $block: Int, $after: Int, $before: Int, $poolId: ID!) {
  joinExits(first: 1000, skip: $skip, where:{timestamp_lt: $before, timestamp_gt: $after, pool: $poolId, protocolFeeUSD_not: "0"}, orderBy: timestamp) {
    id
    protocolFeeUSD
    protocolFeeAmounts
    amounts
    valueUSD
    timestamp
    tx
    pool {
      id
      tokensList
    }
  }
}"""


import asyncio
import datetime
import math
from balpy_v2.subgraphs.blocks import get_block_number_by_timestamp


MAX_RETRIES = 3  # define a maximum number of retries


async def execute_query(query, chain, variables):
    if query == "JOINS_QUERY":
        return await JoinsQuery(chain, variables).execute()
    else:
        return await SwapsQuery(chain, variables).execute()


def extract_items_from_response(response):
    if "swaps" in response:
        return response["swaps"]
    elif "joinExits" in response:
        return response["joinExits"]
    elif "errors" in response:
        logging.info(response["errors"])
        logging.info("Retrying query")
        return []
    else:
        pprint(response)
        raise ValueError("Invalid query type")


async def get_query_data(query, pool_id_chain, after, before, current_skip):
    pool_id, chain = pool_id_chain
    while True:
        variables = dict(after=after, before=before, skip=current_skip, poolId=pool_id)
        response = await execute_query(query, chain, variables)
        if "errors" in response:
            max_skip = check_skip_error(response)
            if max_skip is not None:
                return max_skip, []
        else:
            return None, extract_items_from_response(response)


def check_skip_error(response):
    for error in response["errors"]:
        if "skip" in error["message"]:
            max_value_match = re.search(r"between 0 and (\d+)", error["message"])
            if max_value_match:
                logging.info(f"Max value for skip is {max_value_match.group(1)}")
                return int(max_value_match.group(1))
    return None


async def get_paginated_data(
    query,
    pool_id_chain,
    after,
    before,
    page_size=1000,
    pages_per_group=1,
    max_pages=1000,
):
    data = []
    skip = 0
    max_skip = 100_000_000
    while True:
        logging.info(
            f"{query[:15]} Fetching data for {before} - {after} with skip {skip}, page size {page_size}, pages per group {pages_per_group}, max pages {max_pages}"
        )
        tasks = []
        for i in range(pages_per_group):
            current_skip = skip + page_size * i
            if current_skip >= max_skip:
                current_skip = max_skip

            max_skip_new, items = await get_query_data(
                query, pool_id_chain, after, before, current_skip
            )
            if max_skip_new is not None:
                max_skip = max_skip_new
                break
            tasks.append(items)

        for items in tasks:
            data += items
            if len(items) < page_size:
                break

        logging.info(f"{query[:15]} Total items: {len(data)}")
        skip += page_size * pages_per_group
        logging.info(
            f"{query[:15]} Skip: {skip}, max skip: {max_skip}, tasks: {len(tasks)}"
        )
        if len(tasks) == 0 or len(tasks[-1]) < page_size:
            break

    return data


@memory.cache
async def fetch_data(query, pool_id_chain, cycles):
    data = await asyncio.gather(
        *[
            get_paginated_data(query, pool_id_chain, cycle.start, cycle.end)
            for cycle in cycles
        ]
    )
    return data


def create_dataframes(swaps_data, join_exits_data):
    swaps_df = pd.json_normalize(swaps_data)
    join_exits_df = pd.json_normalize(join_exits_data)
    return swaps_df, join_exits_df


def split_and_process_data(df, query_type, cycles):
    if df.empty:
        return df
    result = []
    for idx, cycle in enumerate(cycles):
        cycle_data = df[
            (df["timestamp"] >= cycle.start) & (df["timestamp"] <= cycle.end)
        ].copy()
        cycle_data["Cycle"] = idx + 1

        if query_type == "SWAPS_QUERY":
            cycle_data["type"] = "swap"
        elif query_type == "JOINS_QUERY":
            cycle_data["type"] = "joinExit"

        result.append(cycle_data)

    result_df = pd.concat(result, ignore_index=True)
    return result_df


@memory.cache
async def fetch_data_for_pool(pool_id_chain, cycles):
    swaps_data, join_exits_data = await asyncio.gather(
        fetch_data("SWAPS_QUERY", pool_id_chain, cycles),
        fetch_data("JOINS_QUERY", pool_id_chain, cycles),
    )
    swaps_data = [item for sublist in swaps_data for item in sublist]
    join_exits_data = [item for sublist in join_exits_data for item in sublist]

    swaps_df, join_exits_df = create_dataframes(swaps_data, join_exits_data)

    cycles = generate_cycles_until_now()
    swaps_result = split_and_process_data(swaps_df, "SWAPS_QUERY", cycles)
    join_exits_result = split_and_process_data(join_exits_df, "JOINS_QUERY", cycles)

    return swaps_result, join_exits_result


@memory.cache
async def generate_reports(pool_ids_chains, cycles=None):
    all_swaps = []
    all_join_exits = []

    results = await asyncio.gather(
        *[
            fetch_data_for_pool(pool_id_chain, cycles)
            for pool_id_chain in pool_ids_chains
        ]
    )

    for idx, _ in enumerate(pool_ids_chains):
        swaps_result, join_exits_result = results[idx]
        all_swaps.append(swaps_result)
        all_join_exits.append(join_exits_result)

    all_swaps_df = pd.concat(all_swaps, ignore_index=True)
    all_join_exits_df = pd.concat(all_join_exits, ignore_index=True)

    return all_swaps_df, all_join_exits_df
