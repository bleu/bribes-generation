from unittest.mock import AsyncMock

import pytest

from balpy_v2.subgraphs.blocks import (
    BLOCKS_QUERY,
    BLOCKS_SUBGRAPH_URL_MAP,
    Chain,
    get_block_number_by_timestamp,
)
from balpy_v2.lib.time import get_time_24h_ago, get_timestamps


@pytest.fixture(autouse=True)
def clear_cache():
    get_block_number_by_timestamp.cache_clear()


@pytest.mark.asyncio
async def test_get_block_number_by_timestamp(monkeypatch):
    gql_mock = AsyncMock(return_value={"blocks": [{"number": 123456}]})
    monkeypatch.setattr("bots.lib.blocks.subgraph.gql", gql_mock)

    block_number = await get_block_number_by_timestamp(chain=Chain.mainnet)
    assert block_number == 123456
    gql_mock.assert_called_once_with(
        BLOCKS_SUBGRAPH_URL_MAP[Chain.mainnet],
        BLOCKS_QUERY,
        variables=get_timestamps(get_time_24h_ago()),
    )


@pytest.mark.asyncio
async def test_get_block_number_by_timestamp_cache(monkeypatch):
    gql_mock = AsyncMock(return_value={"blocks": [{"number": 123456}]})
    monkeypatch.setattr("bots.lib.blocks.subgraph.gql", gql_mock)

    block_number1 = await get_block_number_by_timestamp(chain=Chain.mainnet)
    block_number2 = await get_block_number_by_timestamp(chain=Chain.mainnet)

    assert block_number1 == 123456
    assert block_number2 == 123456
    gql_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_block_number_by_timestamp_no_cache_with_different_t(monkeypatch):
    t1 = 10
    t2 = 20

    async def mock_gql(url, query, variables={}):
        if variables["timestamp_gte"] == str(t1):
            return {"blocks": [{"number": 123456}]}
        elif variables["timestamp_gte"] == str(t2):
            return {"blocks": [{"number": 654321}]}
        else:
            raise ValueError("Unexpected timestamp")

    monkeypatch.setattr("bots.lib.blocks.subgraph.gql", mock_gql)

    block_number1 = await get_block_number_by_timestamp(
        chain=Chain.mainnet, timestamp=t1
    )
    block_number2 = await get_block_number_by_timestamp(
        chain=Chain.mainnet, timestamp=t2
    )

    assert block_number1 == 123456
    assert block_number2 == 654321
