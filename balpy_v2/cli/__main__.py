from click import command

import asyncclick as click

from balpy_v2.contracts.base_contract import BalancerContractFactory
from balpy_v2.lib import Chain
from balpy_v2.cli.helpers import (
    _network_autocompletion,
    _contract_function_autocompletion,
    _vault_function_autocompletion,
    print_function_info,
    get_read_and_write_functions,
    print_contract_details,
)
import logging


# Add these new functions after imports
def get_chain_from_context(ctx):
    network = ctx.obj["network"]
    return Chain.mainnet if network == "mainnet" else Chain.polygon


@click.group()
@click.option(
    "--network",
    type=click.Choice([x.name for x in Chain], case_sensitive=False),
    help="Specify the network for the contract.",
    shell_complete=_network_autocompletion,
)
@click.option(
    "-v", "--verbose", count=True, help="Increase verbosity (e.g., -v or -vv)."
)
@click.pass_context
def balpy(ctx, network, verbose):
    log_level = max(logging.WARNING - 10 * verbose, logging.DEBUG)
    logging.basicConfig(level=log_level)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["network"] = Chain[network] if network else Chain.mainnet


# Create a new function that creates a contract from the current context
def create_contract_from_context(ctx):
    network = ctx.obj["network"]
    chain = Chain.mainnet if network == "mainnet" else Chain.polygon
    contract = BalancerContractFactory.create(chain, ctx.obj["contract_identifier"])
    return contract


# Shared helper function to display contract details
def display_contract_info(ctx):
    if ctx.obj["verbose"] > 0:
        logging.info("Entering info command")

    contract = create_contract_from_context(ctx)
    print_contract_details(contract)


# Replace the vault group with this group
@balpy.group("vault")
@click.pass_context
def vault(ctx):
    ctx.obj["contract_identifier"] = "Vault"


# Replace the contract group with this group
@balpy.group("contract")
@click.argument("identifier")
@click.pass_context
def contract(ctx, identifier):
    ctx.obj["contract_identifier"] = identifier


# Separate info commands for vault and contract groups
@vault.command(name="info", help="Display details of the vault.")
@click.pass_context
def vault_info(ctx):
    display_contract_info(ctx)


@contract.command(name="info", help="Display details of the contract.")
@click.pass_context
def contract_info(ctx):
    display_contract_info(ctx)


@vault.command("fn")
@click.argument("function_name", shell_complete=_vault_function_autocompletion)
@click.option("--list", is_flag=True, help="List all available functions.")
@click.option(
    "--filter", default=None, help="Filter functions based on read, write or regex."
)
@click.pass_context
async def vault_fn(ctx, function_name, list, filter):
    network = ctx.obj["network"]
    chain = Chain.mainnet if network == "mainnet" else Chain.polygon
    vault = BalancerContractFactory.create(chain, "Vault")

    if list:
        print_contract_details(vault)
    elif function_name:
        logging.debug(f"Entering execute command for function {function_name}")
        try:
            function = getattr(vault, function_name)
        except AttributeError:
            click.echo(
                click.style(
                    f"Function '{function_name}' not found in the Vault contract.",
                    fg="red",
                )
            )
            return

        result = await function()

        click.echo(click.style(f"Result of {function_name}:", fg="cyan"))
        click.echo(click.style(f"  {result}", fg="white"))
        pass
    elif filter:
        # Filter functions based on read, write or regex
        pass


@contract.command("fn")
@click.argument("function_name", shell_complete=_contract_function_autocompletion)
@click.argument("args", nargs=-1)
@click.pass_context
async def contract_fn(ctx, function_name, args):
    logging.debug("Entering fn command")

    contract = create_contract_from_context(ctx)

    try:
        function = getattr(contract, function_name)
        result = await function(*args)
        click.echo(click.style(f"Result: {result}", fg="green"))
    except AttributeError:
        click.echo(click.style(f"Function '{function_name}' not found.", fg="red"))
    except Exception as e:
        click.echo(click.style(f"Error: {e}", fg="red"))


if __name__ == "__main__":
    balpy(_anyio_backend="asyncio")
