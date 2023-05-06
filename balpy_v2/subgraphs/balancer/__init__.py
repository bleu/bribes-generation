from balpy_v2.lib import Chain
from balpy_v2.subgraphs.client import SubgraphBaseClient
from balpy_v2.subgraphs.query import GraphQLQuery

BASE_URL = "https://api.thegraph.com/subgraphs/name/balancer-labs"

BALANCER_MAINNET_SUBGRAPH_URL_MAP = {
    Chain.mainnet: BASE_URL + "/balancer-v2",
    Chain.polygon: BASE_URL + "/balancer-polygon-v2",
    Chain.arbitrum: BASE_URL + "/balancer-arbitrum-v2",
    Chain.gnosis: BASE_URL + "/balancer-gnosis-chain-v2",
    # TODO: missing subgraph link in https://docs.balancer.fi/reference/subgraph/
    # optimism="",
    # avalanche="",
}


class BalancerSubgraph(SubgraphBaseClient):
    def get_url(self, chain):
        return BALANCER_MAINNET_SUBGRAPH_URL_MAP[chain]


class BalancerSubgraphQuery(GraphQLQuery):
    def get_client(self):
        return BalancerSubgraph(self.chain)
