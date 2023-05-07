import os
from dotenv import load_dotenv

from balpy_v2.lib import Chain

load_dotenv()

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
LLAMA_PROJECT_ID = os.getenv("LLAMA_PROJECT_ID")


DEFAULT_PROVIDER_NETWORK_MAPPING = {
    Chain.mainnet: "https://eth.llamarpc.com/rpc/{}".format(LLAMA_PROJECT_ID),
    Chain.polygon: "https://polygon.llamarpc.com/rpc/{}".format(LLAMA_PROJECT_ID),
}
