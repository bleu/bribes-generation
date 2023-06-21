import asyncio
import datetime
import math

from balpy_v2.subgraphs.blocks import get_block_number_by_timestamp


import asyncio
import logging
import re
from pprint import pprint

import pandas as pd

from balpy_v2.lib import flatten_json

# Important assumption:
# USD value here is considered form Balancer, not from Coingecko or Llama
# A major improvement would be to use Coingecko or Llama to get instantaneous the USD value
from balpy_v2.lib.gql import gql

REPORT_PERIOD_START_DATE = datetime.datetime(2022, 7, 11, 0, 0, 0).timestamp()
REPORT_PERIOD_DURATION = datetime.timedelta(weeks=2).total_seconds()


class Cycle:
    def __init__(self, start, duration=REPORT_PERIOD_DURATION):
        self.start = int(start)
        self.duration = duration
        self.end = int(self.start + self.duration)
        self.name = self.generate_cycle_name()
        self.start_block = None
        self.end_block = None

    async def get_blocks(self):
        if self.start_block is None or self.end_block is None:
            self.start_block, self.end_block = await asyncio.gather(
                *[
                    get_block_number_by_timestamp(timestamp=int(ts))
                    for ts in [self.start, self.end]
                ]
            )
        return self.start_block, self.end_block

    def generate_cycle_name(self):
        return f"{datetime.datetime.fromtimestamp(self.start).strftime('%Y.%m.%d')}\
                -\
                {datetime.datetime.fromtimestamp(self.end).strftime('%Y.%m.%d')}".replace(
            " ", ""
        )

    def cycle_iteration(self):
        return math.ceil((self.start - REPORT_PERIOD_START_DATE) / self.duration)

    def __str__(self):
        return f"Cycle {self.cycle_iteration()}: {self.name}"


def generate_cycles_until_now():
    now = datetime.datetime.utcnow().timestamp()
    cycles_until_now = math.ceil(
        (now - REPORT_PERIOD_START_DATE) / REPORT_PERIOD_DURATION
    )
    return [
        Cycle(
            REPORT_PERIOD_START_DATE + REPORT_PERIOD_DURATION * i,
            REPORT_PERIOD_DURATION,
        )
        for i in range(cycles_until_now)
    ]
