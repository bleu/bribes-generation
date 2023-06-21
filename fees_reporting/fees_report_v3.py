import asyncio
import json
import logging
import pandas as pd
import httpx
from pprint import pprint
from fees_reporting.fees_report_v2 import generate_reports
import logging
from .fees_report_v2 import memory
import asyncio
import httpx
import json
import logging
from joblib import Memory
from typing import Dict, List

MAX_CONCURRENT_REQUESTS = 10  # Define max number of concurrent requests

logging.basicConfig(level=logging.INFO)
memory = Memory(".cache", verbose=0)

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

LLAMA_API_URL = "https://coins.llama.fi/batchHistorical"


@memory.cache
async def get(url, **kwargs):
    async with semaphore:
        async with httpx.AsyncClient() as client:
            try:
                r = await client.get(url, **kwargs)
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", 1))
                    logging.info(
                        f"Rate limit exceeded. Retrying after {retry_after} seconds."
                    )
                    await asyncio.sleep(retry_after)
                    return await get(url, **kwargs)
                else:
                    print(e.response.text)
                    raise e
            return r.json()


@memory.cache
async def single_request(url, batch_coins, search_width):
    response = await get(
        url, params={"coins": json.dumps(batch_coins), "searchWidth": search_width}
    )
    return response


@memory.cache
async def batch_request(url, coins_dict, search_width=300, batch_size=50):
    tasks = []

    for token, timestamps in coins_dict.items():
        n = len(timestamps)
        for i in range(0, n, batch_size):
            batch_timestamps = timestamps[i : i + batch_size]
            batch_coins = {token: batch_timestamps}
            logging.info(f"Adding task for {token} with {len(batch_timestamps)} items")
            tasks.append(single_request(url, batch_coins, search_width))

    logging.info(f"Starting batch request with {len(tasks)} tasks...")
    try:
        results = await asyncio.gather(*tasks)
    except Exception as e:
        logging.info(f"Batch request failed with exception: {e}")
        raise e

    logging.info(f"Batch request finished. {len(results)} results received.")
    return results


@memory.cache
def process_tokens(df, col_name):
    df[col_name] = df[col_name].apply(
        lambda x: [t if ":" in t else t for t in x]
        if isinstance(x, list)
        else x
        if ":" in x
        else x
    )

    if df[col_name].apply(lambda x: isinstance(x, list)).any():
        tokens_agg = (
            df.explode(col_name).groupby(col_name).agg(list).to_dict(orient="index")
        )
    else:
        tokens_agg = (
            df[[col_name, "timestamp"]]
            .groupby(col_name)
            .agg(list)
            .to_dict(orient="index")
        )

    return {k: v["timestamp"] for k, v in tokens_agg.items()}


@memory.cache
def merge_results(results):
    merged_list = []
    for result in results:
        for token_address, token_data in result["coins"].items():
            token_timestamps = [
                (token_address, t["timestamp"], t["price"], t["confidence"])
                for t in token_data["prices"]
            ]
            merged_list.extend(token_timestamps)

    return merged_list


@memory.cache
def find_closest_timestamp_and_price(swaps, df, key):
    swaps_copy = swaps.copy()
    df_copy = df.copy()
    tokens = swaps_copy[key].unique()

    df_copy = df_copy[df_copy["token"].isin(tokens)].sort_values("timestamp")
    swaps_copy = swaps_copy.sort_values("timestamp")

    result = pd.merge_asof(
        swaps_copy,
        df_copy,
        left_by=key,
        right_by="token",
        left_on="timestamp",
        right_on="timestamp",
        direction="nearest",
        suffixes=("_swaps", "_rates"),
    )

    return result


@memory.cache
async def get_all_tokens_rates(swaps, joins):
    # Process tokenIn and tokenOut columns
    all_tokens_dict = {}
    if not swaps.empty:
        all_tokens_dict.update(process_tokens(swaps, "tokenIn"))
        all_tokens_dict.update(process_tokens(swaps, "tokenOut"))

    if not joins.empty:
        pool_tokens_dict = process_tokens(joins, "pool.tokensList")
        all_tokens_dict.update(
            (k, all_tokens_dict.get(k, []) + v) for k, v in pool_tokens_dict.items()
        )

    # Perform batch requests for all tokens
    all_tokens_response = await batch_request(LLAMA_API_URL, all_tokens_dict)

    # Merge results and create a DataFrame
    df = pd.DataFrame(
        merge_results(all_tokens_response),
        columns=["token", "timestamp", "price", "confidence"],
    )
    df = df.drop_duplicates()
    return df


async def fetch_and_prepare_data(pool_ids_chains, cycles=None):
    logging.info("Fetching swaps and joins data...")
    swaps_df, joins_df = await generate_reports(pool_ids_chains, cycles=cycles)
    logging.info("Fetching tokens rates data...")
    if swaps_df.empty and joins_df.empty:
        logging.info("No swaps or joins data found.")
        return swaps_df, joins_df, pd.DataFrame()
    df = await get_all_tokens_rates(swaps_df, joins_df)
    return swaps_df, joins_df, df


def process_swaps(swaps_df, df):
    logging.info("Processing swaps data...")
    if swaps_df.empty:
        return pd.DataFrame()
    swaps = swaps_df.copy()
    # swaps["tokenIn"] = "ethereum:" + swaps["tokenIn"].str.lower()
    swaps["timestamp"] = swaps["timestamp"].astype(int)
    df["timestamp"] = df["timestamp"].astype(int)

    logging.info("Finding closest timestamp and price for tokenIn...")
    df_tokenIn = find_closest_timestamp_and_price(swaps, df, "tokenIn")
    logging.info(f"df_tokenIn:\n{df_tokenIn.head()}")

    df_tokenIn = df_tokenIn.sort_values("timestamp")
    swaps = pd.merge_asof(
        swaps.sort_values("timestamp"),
        df_tokenIn,
        left_on="timestamp",
        right_on="timestamp",
        left_by="tokenIn",
        right_by="token",
        suffixes=("_tokenIn", "_tokenIn_rate"),
    )
    logging.info(f"Swaps after merging:\n{swaps.head()}")

    swaps["swapFeeTokenAmount"] = swaps["tokenAmountIn_tokenIn"].astype(float) * 0.0004
    swaps["swapFees"] = swaps["swapFeeTokenAmount"].astype(float) * swaps[
        "price"
    ].astype(float)
    swaps[["cycle", "poolId", "token"]] = swaps[
        ["Cycle_tokenIn", "pool.id_tokenIn", "tokenIn_tokenIn"]
    ]
    return swaps


def process_joins(joins_df, df):
    if joins_df.empty:
        return pd.DataFrame()
    logging.info("Processing joins data...")
    joins = joins_df.copy()
    joins = (
        joins.explode(["pool.tokensList", "amounts", "protocolFeeAmounts"])
        .reset_index(drop=True)
        .sort_values("timestamp")
    )
    logging.info(f"Joins after exploding:\n{joins.head()}")

    # joins["pool.tokensList"] = "ethereum:" + joins["pool.tokensList"].str.lower()
    logging.info("Finding closest timestamp and price for pool tokensList...")
    df_tokens = find_closest_timestamp_and_price(joins, df, "pool.tokensList")
    logging.info(f"df_tokens:\n{df_tokens.head()}")

    df_tokens = df_tokens.sort_values("timestamp")
    joins = pd.merge_asof(
        joins,
        df_tokens,
        left_on="timestamp",
        right_on="timestamp",
        left_by="pool.tokensList",
        right_by="token",
        suffixes=("_tokens", "_tokens_rate"),
    )
    logging.info(f"Joins after merging:\n{joins.head()}")

    joins["price"] = joins["price"].astype(float)
    joins["protocolFeeAmounts_tokens"] = joins["protocolFeeAmounts_tokens"].astype(
        float
    )
    joins["protocolFeeAmountsUSD_tokens"] = (
        joins["protocolFeeAmounts_tokens"] * joins["price"]
    )
    joins["amountsUSD"] = joins["amounts_tokens"].astype(float) * joins["price"]

    joins[["cycle", "poolId", "token", "joinExitFeeTokenAmount"]] = joins[
        [
            "Cycle_tokens",
            "pool.id_tokens",
            "pool.tokensList_tokens",
            "protocolFeeAmounts_tokens",
        ]
    ]
    return joins


async def analyze_pool(pool_ids_chains, cycles=None):
    logging.info("Starting pool analysis...")
    swaps_df, joins_df, df = await fetch_and_prepare_data(pool_ids_chains, cycles)
    if swaps_df.empty and joins_df.empty:
        return pd.DataFrame()

    swaps = process_swaps(swaps_df, df)
    joins = process_joins(joins_df, df)

    logging.info("Aggregating fees per cycle and poolId...")

    per_cycle = []
    new_columns = []
    if not swaps.empty:
        new_columns.extend(
            [
                "swapFeeUSD",
                "swapFeeTokenAmount",
            ]
        )
        per_cycle.extend(
            [
                swaps.groupby(["cycle", "poolId", "token"])[["swapFees"]].sum(),
                swaps.groupby(["cycle", "poolId", "token"])[
                    ["swapFeeTokenAmount"]
                ].sum(),
            ]
        )
    if not joins.empty:
        new_columns.extend(
            [
                "joinExitFeeUSD",
                "joinExitFeeTokenAmount",
            ]
        )

        per_cycle.extend(
            [
                joins.groupby(["cycle", "poolId", "token"])[
                    ["protocolFeeAmountsUSD_tokens"]
                ].sum(),
                joins.groupby(["cycle", "poolId", "token"])[
                    ["joinExitFeeTokenAmount"]
                ].sum(),
            ]
        )

    per_cycle = pd.concat(
        per_cycle,
        axis=1,
    )

    per_cycle.columns = new_columns
    # only add app when the columns exist
    import numpy as np

    per_cycle["totalUSD"] = np.zeros(per_cycle.shape[0])
    per_cycle["totalToken"] = np.zeros(per_cycle.shape[0])
    if not swaps.empty:
        per_cycle["totalUSD"] += per_cycle["swapFeeUSD"]
        per_cycle["totalToken"] += per_cycle["swapFeeTokenAmount"].astype(float)
    if not joins.empty:
        per_cycle["totalUSD"] += per_cycle["joinExitFeeUSD"]
        per_cycle["totalToken"] += per_cycle["joinExitFeeTokenAmount"].astype(float)

    logging.info("Pool analysis complete.")
    return (swaps, joins, df, per_cycle)
