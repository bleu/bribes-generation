from abc import ABC, abstractmethod

from balpy_v2.lib import Chain
from balpy_v2.subgraphs.client import GraphQLClient


class GraphQLQuery(ABC):
    def __init__(self, chain=Chain.mainnet, variables=dict()):
        self.chain = chain
        self.variables = variables

    @abstractmethod
    def get_query(self):
        pass

    @abstractmethod
    def get_client(self) -> GraphQLClient:
        pass

    async def execute(self):
        query = self.get_query()
        client = self.get_client()
        return await client.__class__.query(self.chain, query, self.variables)
