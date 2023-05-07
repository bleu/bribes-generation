from balpy_v2.subgraphs.client import GraphQLClient
from balpy_v2.subgraphs.query import GraphQLQuery
from balpy_v2.lib import Chain

BASE_URL = "https://api.thegraph.com/subgraphs/name/balancer-labs"

BALANCER_MAINNET_GAUGES_SUBGRAPH_URL_MAP = {
    Chain.mainnet: BASE_URL + "/balancer-gauges",
    Chain.polygon: BASE_URL + "/balancer-gauges-polygon",
    Chain.arbitrum: BASE_URL + "/balancer-gauges-arbitrum",
    Chain.gnosis: BASE_URL + "/balancer-gauges-gnosis-chain",
}

DEPLOYED_CHAINS = BALANCER_MAINNET_GAUGES_SUBGRAPH_URL_MAP.keys()


class GaugesSubgraph(GraphQLClient):
    def get_url(self, chain):
        return BALANCER_MAINNET_GAUGES_SUBGRAPH_URL_MAP[chain]


class GaugesSubgraphQuery(GraphQLQuery):
    def get_client(self):
        return GaugesSubgraph(self.chain)
