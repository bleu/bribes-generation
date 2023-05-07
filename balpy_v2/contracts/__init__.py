import json
import os
from functools import cache

import web3


@cache
def get_web3_instance():
    return web3.AsyncWeb3(
        web3.AsyncHTTPProvider(
            f"https://eth.llamarpc.com/rpc/{os.getenv('LLAMA_PROJECT_ID')}"
        )
    )


@cache
def get_contract_abi(abi_file_name):
    file_path = os.path.join(
        "bots", "lib", "balancer", "contracts", "abis", abi_file_name
    )
    with open(file_path) as f:
        return json.load(f)


@cache
def get_web3_contract(contract_address, abi_file_name):
    w3 = get_web3_instance()
    return w3.eth.contract(
        address=w3.to_checksum_address(contract_address),
        abi=get_contract_abi(abi_file_name),
    )
