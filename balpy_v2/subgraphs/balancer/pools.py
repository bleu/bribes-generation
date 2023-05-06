from balpy_v2.lib import Chain
from balpy_v2.subgraphs.balancer import BalancerSubgraphQuery


class PoolQuery(BalancerSubgraphQuery):
    def get_query(self):
        return """
        query($id_list:[ID!]){
          pools(where:{id_in:$id_list}) {
            id
            symbol
          }
        }
        """


async def pool_query(chain=Chain.mainnet, pool_id_list=[]):
    response = await PoolQuery(
        chain=chain, variables={"id_list": pool_id_list}
    ).execute()
    return {pool["id"]: pool["symbol"] for pool in response["pools"]}
