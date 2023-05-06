# balpy_v2/lib/__init__.py
from enum import Enum


class Chain(Enum):
    mainnet = 1
    polygon = 137
    arbitrum = 42161
    gnosis = 100
    optimism = 10
    goerli = 5

    def __init__(self, id):
        self.id = id
