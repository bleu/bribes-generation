# bots/lib/balancer/subgraphs/pools.py
import asyncio

from balpy_v2.lib import Chain
from balpy_v2.subgraphs.balancer import BALANCER_MAINNET_SUBGRAPH_URL_MAP
from balpy_v2.subgraphs.blocks import get_block_number_by_timestamp
from balpy_v2.lib.gql import gql
from balpy_v2.subgraphs.client import (
    BalancerSubgraph,
    SubgraphBaseClient,
    SubgraphClient,
)
from balpy_v2.subgraphs.balancer import BalancerSubgraphQuery


class BalancersQuery(BalancerSubgraphQuery):
    def get_query(self):
        return """
query($block: Block_height) {
  balancers(block: $block) {
    totalLiquidity
    totalSwapVolume
  }
}
"""


async def get_total_liquidity(chain=Chain.mainnet):
    r = await BalancersQuery(chain=chain).execute()
    return float(r["balancers"][0]["totalLiquidity"])


async def get_current_cross_chain_liquidity():
    return sum(
        [
            await get_total_liquidity(chain)
            for chain in BALANCER_MAINNET_SUBGRAPH_URL_MAP.keys()
        ]
    )


async def get_current_total_swap_volume(chain=Chain.mainnet):
    r = await BalancersQuery(chain=chain).execute()
    return float(r["balancers"][0]["totalSwapVolume"])


async def get_t_minus_24h_total_swap_volume(chain=Chain.mainnet):
    t_minus_24h_block_number = await get_block_number_by_timestamp(chain=chain)
    r = await BalancersQuery(
        chain=chain, variables=dict(block=dict(number=t_minus_24h_block_number))
    ).execute()
    return float(r["balancers"][0]["totalSwapVolume"])


async def get_last_24h_total_swap_volume(chain=Chain.mainnet):
    return await get_current_total_swap_volume(
        chain=chain
    ) - await get_t_minus_24h_total_swap_volume(chain=chain)


async def get_cross_chain_last_24h_total_swap_volume():
    chains = BALANCER_MAINNET_SUBGRAPH_URL_MAP.keys()
    asyncs = await asyncio.gather(
        *[get_last_24h_total_swap_volume(chain=chain) for chain in chains]
    )

    return sum(asyncs)
