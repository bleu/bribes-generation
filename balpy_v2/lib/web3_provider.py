import os
from typing import Dict
import web3
from balpy_v2.config import DEFAULT_PROVIDER_NETWORK_MAPPING

from balpy_v2.lib import Chain


class Web3Provider:
    """
    A singleton class that manages web3 instances for different chains.

    :ivar _instances: A dictionary to store web3 instances for each chain.
    """

    _instances: Dict[Chain, web3.AsyncWeb3] = {}

    @classmethod
    def get_instance(cls, chain: Chain) -> web3.AsyncWeb3:
        """
        Retrieves or creates a web3 instance for the specified chain.

        :param chain: The Chain object representing the blockchain.
        :return: An AsyncWeb3 instance for the specified chain.
        """
        if chain not in cls._instances:
            cls._instances[chain] = web3.AsyncWeb3(
                web3.AsyncHTTPProvider(DEFAULT_PROVIDER_NETWORK_MAPPING[chain])
            )
        return cls._instances[chain]
