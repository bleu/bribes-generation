from balpy_v2.contracts.utils.contract_loader import (
    ContractLoader,
    load_abi_from_address,
    load_deployment_addresses,
)
from balpy_v2.lib import Chain
import asyncio


class BaseContract:
    """
    A base class for Balancer contracts that implements common functionality.

    This class uses a singleton pattern to ensure that there's only one instance
    of the contract for each contract address and chain combination.

    :ivar _instances: A dictionary to store instances of the BaseContract class.
    """

    _instances = {}

    def __new__(cls, contract_address, chain: Chain):
        key = (cls, contract_address, chain)
        if key not in cls._instances:
            cls._instances[key] = super().__new__(cls)
        return cls._instances[key]

    def __init__(self, contract_address, chain: Chain, abi_file_name=None):
        """
        Initializes the BaseContract with a contract address, chain, and optionally an ABI file name.

        :param contract_address: The address of the contract on the specified chain
        :param chain: The chain the contract is deployed on
        :param abi_file_name: The ABI file name of the contract, optional
        """
        if not "_initialized" in self.__dict__:
            self.contract_loader = ContractLoader(chain)
            self.web3_contract = self.contract_loader.get_web3_contract(
                contract_address, abi_file_name
            )
            self._initialized = True

    @property
    def contract_address(self):
        return self.web3_contract.address

    def _function_exists_in_abi(self, function_name):
        """
        Checks if a function exists in the ABI of the contract.

        :param function_name: The name of the function to check for
        :return: True if the function exists, False otherwise
        """
        for item in self.web3_contract.abi:
            if item.get("type") == "function" and item.get("name") == function_name:
                return True
        return False

    def __getattr__(self, name):
        """
        Makes contract functions directly accessible as attributes of the BaseContract.

        :param name: The name of the attribute being accessed
        :return: The wrapped contract function if it exists, raises AttributeError otherwise
        """
        if self._function_exists_in_abi(name):
            function = getattr(self.web3_contract.functions, name)

            async def wrapped_async_function(*args, **kwargs):
                return await function(*args, **kwargs).call()

            def wrapped_sync_function(*args, **kwargs):
                return function(*args, **kwargs).call()

            if asyncio.iscoroutinefunction(function):
                return wrapped_async_function
            else:
                return wrapped_sync_function

        raise AttributeError(f"{self.__class__.__name__} has no attribute {name}")


class BalancerContractFactory:
    _contract_classes = {}

    @classmethod
    def get_contract_class(cls, contract_name, chain: Chain):
        """
        Retrieves the contract class for a given contract name and chain, creating it if it doesn't exist.

        :param contract_name: The name of the contract
        :param chain: The chain the contract is deployed on
        :return: The contract class for the given contract name and chain
        """
        key = (contract_name, chain)
        if key not in cls._contract_classes:
            # Load the deployment address for the contract
            address_book = load_deployment_addresses(chain)
            contract_address = next(
                k
                for k, v in address_book.items()
                if v["name"].casefold() == contract_name.casefold()
            )

            # Load the ABI from the deployment address
            abi = load_abi_from_address(chain, contract_address)

            # Dynamically create the contract class
            contract_class = type(
                f"{contract_name}", (BaseContract,), {"ABI_FILE_NAME": abi}
            )
            cls._contract_classes[key] = contract_class

        return cls._contract_classes[key]

    @classmethod
    def create(cls, chain: Chain, contract_identifier=None):
        """
        Creates an instance of the contract class for a given contract identifier (name or address) and chain.

        :param chain: The chain the contract is deployed on
        :param contract_identifier: The name or address of the contract on the specified chain, optional
        :return: An instance of the contract class for the given contract identifier and chain
        """
        address_book = load_deployment_addresses(chain)

        if contract_identifier is None:
            raise ValueError(
                "A contract identifier (name or address) must be provided."
            )

        # Check if the contract_identifier is an address or a name
        is_address = (
            contract_identifier.startswith("0x") and len(contract_identifier) == 42
        )

        if is_address:
            contract_address = contract_identifier
            contract_name = address_book.get(contract_address, {}).get("name")
            if not contract_name:
                raise ValueError(
                    f"Contract address {contract_address} not found in the address book."
                )
        else:
            contract_name = contract_identifier
            contract_address = next(
                k
                for k, v in address_book.items()
                if v["name"].casefold() == contract_name.casefold()
            )

        contract_class = cls.get_contract_class(contract_name, chain)
        return contract_class(contract_address, chain)
