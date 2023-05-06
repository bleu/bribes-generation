from balpy_v2.lib import Chain
from balpy_v2.subgraphs.balancer import (
    BALANCER_MAINNET_SUBGRAPH_URL_MAP,
    BalancerSubgraph,
)
from balpy_v2.lib.gql import gql
from balpy_v2.subgraphs.balancer import BalancerSubgraphQuery


class TokensQuery(BalancerSubgraphQuery):
    def get_query(self):
        return """
            query($address_list:[String!]) {
              tokens(
                where:{address_in:$address_list}
              ) {
                id
                latestUSDPrice
              }
            }"""


async def async_token_query(chain=Chain.mainnet, address_list=[]):
    response = await TokensQuery(
        chain=chain, variables=dict(address_list=address_list)
    ).execute()
    return {token["id"]: token["latestUSDPrice"] for token in response["tokens"]}
