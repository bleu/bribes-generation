import os
import json
from functools import cache
from balpy_v2.lib import CaseInsensitiveDict, Chain
import logging
from balpy_v2.lib.web3_provider import Web3Provider
from balpy_v2.cache import memory


@memory.cache
def load_deployment_addresses(chain: Chain):
    """
    Loads the deployment addresses for the specified chain from a JSON file.

    :param chain: The Chain object representing the blockchain.
    :return: A CaseInsensitiveDict containing the deployment addresses.
    """
    file_path = os.path.join(
        "balpy_v2", "deployments", "addresses", f"{chain.name}.json"
    )
    with open(file_path) as f:
        return CaseInsensitiveDict(json.load(f))


@memory.cache
def load_all_deployments_artifacts():
    """
    Loads all deployment artifacts by going through each folder in the tasks folders
    and digging all files from the artifact folder if it exists.

    :return: A dictionary whose key is the deployment task from the task folder name and value
    the deployment tasks artifacts.
    """
    artifacts = {}
    # Temporarily move all /deprecated subfolders from tasks to the root
    if os.path.exists(os.path.join("balpy_v2", "deployments", "tasks", "deprecated")):
        for task in os.listdir(
            os.path.join("balpy_v2", "deployments", "tasks", "deprecated")
        ):
            os.rename(
                os.path.join("balpy_v2", "deployments", "tasks", "deprecated", task),
                os.path.join("balpy_v2", "deployments", "tasks", task),
            )
        # os.rmdir(os.path.join("balpy_v2", "deployments", "tasks", "deprecated"))

    for task in os.listdir(os.path.join("balpy_v2", "deployments", "tasks")):
        if not os.path.exists(
            os.path.join("balpy_v2", "deployments", "tasks", task, "build-info")
        ):
            continue
        for artifact in os.listdir(
            os.path.join("balpy_v2", "deployments", "tasks", task, "build-info")
        ):
            with open(
                os.path.join(
                    "balpy_v2", "deployments", "tasks", task, "build-info", artifact
                )
            ) as f:
                data = json.load(f)["output"]["contracts"]
                data = [
                    {"contractName": name, "abi": contract_data["abi"]}
                    for contract_file in data.values()
                    for (name, contract_data) in contract_file.items()
                    if len(contract_data["abi"]) > 0
                ]
                for contract in data:
                    if contract.get("abi") is None:
                        logging.warning(
                            f"ABI not found for {contract.get('contractName')} in {task}"
                        )
                    if artifacts.get(contract.get("contractName")) is not None:
                        logging.debug(
                            f"Duplicate artifact {contract.get('contractName')} found in {task} and {artifacts[contract.get('contractName')]['task']}"
                        )
                    artifacts[contract.get("contractName")] = dict(
                        name=contract.get("contractName"),
                        abi=contract.get("abi"),
                        task=task,
                    )
                # if artifacts.get(artifact) is not None:
                #     logging.warning(
                #         f"Duplicate artifact {artifact} found in {task} and {artifacts[artifact]['task']}"
                #     )
                # artifacts[artifact] = dict(
                #     name=data.get("contractName"), abi=data.get("abi"), task=task
                # )
    return artifacts


@cache
def load_deployment_address_task(network, address):
    """
    Loads the deployment address task for a given network and address.

    :param network: The network the deployment address belongs to.
    :param address: The deployment address to look up.
    :return: A dictionary containing the deployment address task.
    """
    address_book = load_deployment_addresses(network)
    return address_book.get(address)


@cache
def load_task_artifact(task, name):
    """
    Loads a task artifact with the given task and name from a JSON file.

    :param task: The task identifier.
    :param name: The name of the artifact.
    :return: A dictionary containing the artifact data.
    """
    file_path = os.path.join(
        "balpy_v2", "deployments", "tasks", task, "artifact", f"{name}.json"
    )
    if not os.path.exists(file_path):
        return None
    with open(file_path) as f:
        return json.load(f)


@cache
def load_abi_from_address(network, address):
    """
    Loads the ABI for a contract deployed on a given network and address.

    :param network: The network the contract is deployed on.
    :param address: The address of the contract.
    :return: A list containing the ABI data.
    """
    task = load_deployment_address_task(network, address)
    if not task:
        return None
    output = load_task_artifact(task["task"], task["name"])
    if not output:
        return None
    return output["abi"]


class ContractLoader:
    """
    A utility class to load contract ABIs and create web3 contract instances.
    """

    def __init__(self, network):
        """
        Initializes a ContractLoader instance for a specified network.

        :param network: The network the contract loader is associated with.
        """
        self.network = network
        self._abis = {}

    def load_abi_from_file(self, abi_file_name):
        """
        Loads the ABI data from a JSON file with the given file name.

        :param abi_file_name: The name of the ABI file.
        :return: A list containing the ABI data.
        """
        file_path = os.path.join("balpy_v2", "abis", abi_file_name)
        with open(file_path) as f:
            return json.load(f)

    def get_contract_abi(self, address, abi_file_name=None):
        """
        Retrieves the ABI for a contract with a given address or ABI file name.

        :param address: The address of the contract.
        :param abi_file_name: The file name of the ABI, optional.
        :return: A list containing the ABI data.
        """
        if abi_file_name:
            return self.load_abi_from_file(abi_file_name)

        if address not in self._abis:
            self._abis[address] = load_abi_from_address(self.network, address)
        return self._abis[address]

    def get_web3_contract(self, contract_address, abi_file_name=None, abi=None):
        """
        Creates a web3 contract instance for the specified contract address and ABI.

        :param contract_address: The address of the contract.
        :param abi_file_name: The file name of the ABI, optional.
        :return: A web3 contract instance.
        """
        w3 = Web3Provider.get_instance(self.network)

        return w3.eth.contract(
            address=w3.to_checksum_address(contract_address),
            abi=abi or self.get_contract_abi(contract_address, abi_file_name),
        )
