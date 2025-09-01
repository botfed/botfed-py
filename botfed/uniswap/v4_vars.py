from web3 import Web3
from decimal import Decimal, getcontext
import os
import dotenv
dotenv.load_dotenv()


getcontext().prec = 40

Q96 = Decimal(2**96)

STATE_VIEW_ADDR = Web3.to_checksum_address("0x86e8631A016F9068C3f085fAF484Ee3F5fDee8f2")
POOL_ID_HEX = "0x3258f413c7a88cda2fa8709a589d221a80f6574f63df5a5b6774485d8acc39d9"
POOL_ID = bytes.fromhex(POOL_ID_HEX[2:])  # convert hex string to bytes

USDC_ADDR = Web3.to_checksum_address("0x078D782b760474a361dDA0AF3839290b0EF57AD6")

POOL_MANAGER_ADDR = Web3.to_checksum_address(
    "0x1f98400000000000000000000000000000000004"
)

UNI_TEST_BLOCK_NUM = 19506992

w3 = Web3(
    Web3.HTTPProvider(os.environ.get("UNI_RPC_URL", "https://unichain.drpc.org"))
)

assert w3.is_connected(), "Failed to connect to RPC"