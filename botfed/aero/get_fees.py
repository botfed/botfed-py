from typing import Mapping, List
from .abis.position_manager import ABI as POS_MANAGER_ABI
from .abis.multicall3 import ABI as MULTICALL3_ABI
from .vars import get_w3, POS_MANAGER_ADDR, MULTICALL3_ADDR
from .helpers import fetch_info_from_pool_id, CLMMSnap
from .abis.pool_clmm import ABI as POOL_ABI
from .positions import get_pools_batch, get_positions_batch
from .positions import _encode_call

UINT_128_MAX = 2**128 - 1


def fetch_pool_infos(w3, positions, block):
    pool_data = [(pos[1][2], pos[1][3], pos[1][4]) for pos in positions]
    pool_addresses = get_pools_batch(w3, pool_data)
    pool_infos: Mapping[str, CLMMSnap] = {}
    # load pool infos
    for idx in range(len(positions)):
        pool_id = pool_addresses[idx]
        if pool_id not in pool_infos:
            pool_infos[pool_id] = fetch_info_from_pool_id(
                w3, pool_id, block_number=block
            )
    return pool_infos


def compute_position_key(w3, owner: str, tick_lower: int, tick_upper: int) -> bytes:
    """
    Compute keccak256(abi.encodePacked(owner, tickLower, tickUpper))
    using pure web3.py without eth_abi.

    :param owner: Address string (with 0x prefix)
    :param tick_lower: Signed int24 value
    :param tick_upper: Signed int24 value
    :return: 32-byte keccak256 hash (bytes32)
    """
    owner_bytes = w3.to_bytes(hexstr=owner)
    tick_lower_bytes = tick_lower.to_bytes(3, byteorder="big", signed=True)
    tick_upper_bytes = tick_upper.to_bytes(3, byteorder="big", signed=True)
    packed = owner_bytes + tick_lower_bytes + tick_upper_bytes
    return w3.keccak(packed)


def get_claimable_fees(
    w3,
    block: int,
    pool_infos: Mapping[str, CLMMSnap],
    positions,
    pool_addresses: List[str],
    eoa: str,
):
    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)
    pos_manager = w3.eth.contract(address=POS_MANAGER_ADDR, abi=POS_MANAGER_ABI)
    pool_contracts = {}
    calls = []
    for idx, (pos_id, pos) in enumerate(positions):
        pool_id = pool_addresses[idx]
        if pool_id not in pool_contracts:
            pool_contracts[pool_id] = w3.eth.contract(
                address=w3.to_checksum_address(pool_id), abi=POOL_ABI
            )
        tick_lower, tick_upper = pos[5], pos[6]
        pos_key = (
            "0x"
            + compute_position_key(
                w3, pool_infos[pool_id].gauge, tick_lower, tick_upper
            ).hex()
        )
        print(pool_id, pos_id, pos_key)
        pos_key = "0x" + compute_position_key(w3, eoa, tick_lower, tick_upper).hex()
        print(pool_id, pos_id, pos_key)
        calls.append(
            (
                w3.to_checksum_address(pool_infos[pool_id].gauge),
                _encode_call(pool_contracts[pool_id], "positions", pos_key),
            )
        )
    _, return_data = multicall.functions.aggregate(calls).call(block_identifier=block)
    rewards = [
        {
            "rewardAmt": w3.codec.decode(["uint256"], result)[0] / 1e18,
            "position_id": positions[idx][0],
        }
        for idx, result in enumerate(return_data)
    ]
    return rewards


if __name__ == "__main__":
    import os
    import dotenv

    dotenv.load_dotenv()

    eoa = os.environ["LP_ADDR"]
    w3 = get_w3()
    block = w3.eth.block_number
    position_ids = [
        16848700,
    ]
    positions = get_positions_batch(w3, position_ids, block)
    print(positions)
    pool_infos = fetch_pool_infos(w3, positions, block)
    pool_data = [(pos[1][2], pos[1][3], pos[1][4]) for pos in positions]
    pool_addresses = get_pools_batch(w3, pool_data)
    get_claimable_fees(w3, block, pool_infos, positions, pool_addresses, eoa)
