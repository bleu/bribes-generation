import asyncclick as click

from balpy_v2.contracts.base_contract import BalancerContractFactory
from balpy_v2.lib import Chain

import logging


def _network_autocompletion(ctx, args, incomplete):
    networks = ["mainnet", "polygon"]
    return [n for n in networks if n.startswith(incomplete)]


def _vault_function_autocompletion(ctx, args, incomplete):
    chain = Chain.mainnet if "mainnet" in args else Chain.polygon
    vault = BalancerContractFactory.create(chain, "Vault")
    functions = [f["name"] for f in vault.web3_contract.abi if f["type"] == "function"]
    return [fn for fn in functions if fn.startswith(incomplete)]


def _contract_function_autocompletion(ctx, args, incomplete):
    chain = Chain.mainnet if "mainnet" in args else Chain.polygon

    contract_address_key = "contract"
    contract_address_index = (
        args.index(contract_address_key) + 1 if contract_address_key in args else None
    )
    contract_address = args[contract_address_index] if contract_address_index else None

    if not contract_address:
        return []

    contract = BalancerContractFactory.create(chain, contract_address)
    functions = [
        f["name"] for f in contract.web3_contract.abi if f["type"] == "function"
    ]
    return [fn for fn in functions if fn.startswith(incomplete)]


def echo_argument(argument):
    "Prints an argument if it has a name, otherwiwse only prints the type"
    if not argument.get("name"):
        click.echo(click.style(f"      - Type: {argument['type']}", fg="white"))
        return
    click.echo(
        click.style(
            f"      - Name: {argument['name']}, Type: {argument['type']}",
            fg="white",
        )
    )


def print_function_info(function):
    click.echo(click.style("  - Function name:", fg="cyan") + f" {function['name']}")

    inputs = function["inputs"]
    if len(inputs) > 0:
        click.echo(click.style("    - Input arguments:", fg="magenta"))

        for input_arg in function["inputs"]:
            echo_argument(input_arg)

    outputs = function["outputs"]

    if len(outputs) > 0:
        click.echo(click.style("    - Output arguments:", fg="magenta"))
        for output_arg in function["outputs"]:
            echo_argument(output_arg)

    click.echo()


def get_read_and_write_functions(contract):
    read_functions = []
    write_functions = []

    for function in contract.web3_contract.abi:
        if function["type"] == "function":
            if "stateMutability" in function and function["stateMutability"] == "view":
                read_functions.append(function)
            else:
                write_functions.append(function)

    return read_functions, write_functions


def print_contract_details(contract):
    title = contract.__class__.__name__
    click.echo(click.style(f"{title}:", fg="green"))

    read_functions, write_functions = get_read_and_write_functions(contract)

    click.echo(click.style("Read functions:", fg="cyan"))
    for function in read_functions:
        print_function_info(function)

    click.echo(click.style("Write functions:", fg="cyan"))
    for function in write_functions:
        print_function_info(function)
