from unittest import result
import pandas as pd
from balpy_v2.lib.flatten_json import flatten_json
from balpy_v2.lib.gql import gql

GRAPH_URL = "https://api.thegraph.com/subgraphs/name/bleu-studio/balancer-mainnet-v2"
QUERY = """
query token($block: Int!) {
    poolTokens(block: {number: $block},
               first: 1000,
               where:{
                    paidProtocolFees_not:"0"
               },
                orderBy: paidProtocolFees,
                orderDirection: desc
        ){
    id
    symbol
    address
    paidProtocolFees
    token {
      latestUSDPrice
    }
	pool: poolId {
      id
      address
      symbol
    }
  }
}
"""

import datetime
import asyncio
from balpy_v2.subgraphs.blocks import get_block_number_by_timestamp

REPORT_PERIOD_START_DATE = datetime.datetime(2022, 7, 11, 0, 0, 0).timestamp()

# 2 weeks in seconds
NOW = datetime.datetime.utcnow().timestamp()
REPORT_PERIOD_DURATION = datetime.timedelta(weeks=2).total_seconds()

import math

REPORT_CYCLES_UNTIL_NOW = math.ceil(
    (NOW - REPORT_PERIOD_START_DATE) / REPORT_PERIOD_DURATION
)

REPORT_CYCLES_START_DATES = [
    REPORT_PERIOD_START_DATE + REPORT_PERIOD_DURATION * i
    for i in range(REPORT_CYCLES_UNTIL_NOW)
]

CYCLES_NAMES_FROM_DATES = [
    f"""{datetime.datetime.fromtimestamp(REPORT_PERIOD_START_DATE + REPORT_PERIOD_DURATION * i).strftime('%Y.%m.%d')}\
    -\
    {datetime.datetime.fromtimestamp(REPORT_PERIOD_START_DATE + REPORT_PERIOD_DURATION * (i + 1)).strftime('%Y.%m.%d')}""".replace(
        " ", ""
    )
    for i in range(REPORT_CYCLES_UNTIL_NOW)
]


async def report_cycles_block_numbers():
    return await asyncio.gather(
        *[
            get_block_number_by_timestamp(timestamp=int(ts))
            for ts in REPORT_CYCLES_START_DATES
        ]
    )


async def report_cycles_data():
    blocks = await report_cycles_block_numbers()
    # blocks = list(filter(lambda x: x < 16253377, blocks))
    results = await asyncio.gather(
        *[
            gql(
                GRAPH_URL,
                QUERY,
                variables=dict(block=block),
            )
            for block in blocks
        ]
    )

    return [
        (result, blocks[idx], REPORT_CYCLES_START_DATES[idx])
        for idx, result in enumerate(results)
    ]


def period_analysis(start, end):
    period_start_data, start_block, start_date = start
    period_end_data, end_block, end_date = end
    period_start_df = (
        pd.DataFrame([flatten_json(x) for x in period_start_data["poolTokens"]])
        .set_index(["id"])
        .add_suffix("_start")
    )
    period_start_df["block_start"] = start_block
    period_start_df["date_start"] = start_date
    period_end_df = (
        pd.DataFrame([flatten_json(x) for x in period_end_data["poolTokens"]])
        .set_index("id")
        .add_suffix("_end")
    )
    period_end_df["block_end"] = end_block
    period_end_df["date_end"] = end_date
    merged = period_start_df.merge(period_end_df, left_index=True, right_index=True)
    # drop duplicated columns
    merged = (
        merged.loc[:, ~merged.columns.duplicated()]
        .copy()
        .set_index("date_start", append=True)
    )
    merged["address_start"] = "ethereum:" + merged["address_start"]
    merged[
        [
            "token_latestUSDPrice_end",
            "token_latestUSDPrice_start",
            "paidProtocolFees_end",
            "paidProtocolFees_start",
        ]
    ] = merged[
        [
            "token_latestUSDPrice_end",
            "token_latestUSDPrice_start",
            "paidProtocolFees_end",
            "paidProtocolFees_start",
        ]
    ].astype(
        float
    )
    merged["paidProtocolFees_diff"] = (
        merged["paidProtocolFees_end"] - merged["paidProtocolFees_start"]
    )
    paid_fees = merged[merged["paidProtocolFees_diff"] != 0].sort_values(
        "paidProtocolFees_diff", ascending=False
    )
    paid_fees["paidProtocolFees_inUSD_end"] = (
        paid_fees["paidProtocolFees_diff"] * paid_fees["token_latestUSDPrice_end"]
    )
    paid_fees["paidProtocolFees_inUSD_start"] = (
        paid_fees["paidProtocolFees_diff"] * paid_fees["token_latestUSDPrice_start"]
    )
    return paid_fees


async def full_analysis():
    data = await report_cycles_data()
    cycles_data = list(zip(data, data[1:] + data[:1]))[:-1]
    return pd.concat([period_analysis(*x) for x in cycles_data])
