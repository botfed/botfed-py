import json
import pandas as pd
from typing import Mapping, List, Dict
from web3 import Web3
from ..logger import get_logger
from .abis.position_manager import ABI as POS_MANAGER_ABI
from .abis.multicall3 import ABI as MULTICALL3_ABI
from .abis.pool_factory import ABI as POOL_FACTORY_ABI
from .abis.gauge import ABI as GAUGE_ABI
from .abis.erc20 import ABI as ERC20_ABI
from .vars import get_w3, POS_MANAGER_ADDR, MULTICALL3_ADDR, POOL_FACTORY_ADDR
from .helpers import (
    fetch_pool_infos,
    fetch_pool_infos_w_tvl,
    CLMMSnapNoToken,
    fetch_token_infos,
    Token,
)

logger = get_logger(__name__)


def get_position(w3, position_id: int, block: int):
    pos_manager = w3.eth.contract(abi=POS_MANAGER_ABI, address=POS_MANAGER_ADDR)
    pos = pos_manager.functions.positions(position_id).call(block_identifier=block)
    return pos


def get_positions_batch(w3, position_ids, block):
    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)
    pos_manager = w3.eth.contract(abi=POS_MANAGER_ABI, address=POS_MANAGER_ADDR)

    calls = [
        (POS_MANAGER_ADDR, pos_manager.encodeABI(fn_name="positions", args=[pid]))
        for pid in position_ids
    ]

    return_data = multicall.functions.tryAggregate(False, calls).call(
        block_identifier=block
    )

    decoded_positions = []
    for idx, (suc, raw) in enumerate(return_data):
        if suc:
            pos = w3.codec.decode(
                [
                    "uint96",  # nonce
                    "address",  # operator
                    "address",  # token0
                    "address",  # token1
                    "uint24",  # fee / tickSpacing
                    "int24",  # tickLower
                    "int24",  # tickUpper
                    "uint128",  # liquidity
                    "uint256",  # feeGrowthInside0LastX128
                    "uint256",  # feeGrowthInside1LastX128
                    "uint128",  # tokensOwed0
                    "uint128",  # tokensOwed1
                ],
                raw,
            )
            decoded_positions.append((position_ids[idx], pos))
        else:
            decoded_positions.append((position_ids[idx], None))

    return decoded_positions


def get_pool(w3, token0, token1, tick_spacing):
    factory = w3.eth.contract(address=POOL_FACTORY_ADDR, abi=POOL_FACTORY_ABI)
    return factory.functions.getPool(
        w3.to_checksum_address(token0), w3.to_checksum_address(token1), tick_spacing
    ).call()


def get_pools_batch(w3, pool_list, block=None):
    """
    Batch fetch pool addresses for multiple (token0, token1, tick_spacing) tuples.
    """

    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)
    factory = w3.eth.contract(address=POOL_FACTORY_ADDR, abi=POOL_FACTORY_ABI)

    calls = []
    for token0, token1, tick_spacing in pool_list:
        call_data = factory.encodeABI(
            fn_name="getPool",
            args=[
                w3.to_checksum_address(token0),
                w3.to_checksum_address(token1),
                tick_spacing,
            ],
        )
        calls.append((POOL_FACTORY_ADDR, call_data))

    _, return_data = multicall.functions.aggregate(calls).call(block_identifier=block)

    pools = [
        w3.to_checksum_address(w3.codec.decode(["address"], raw)[0])
        for raw in return_data
    ]
    return pools


def get_rewards(
    w3,
    block: int,
    pool_infos: Mapping[str, CLMMSnapNoToken],
    position_ids: List[int],
    position_owners: List[str],
    pool_address: List[str],
):
    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)
    gauges = {}
    calls = []

    for idx, pos_id in enumerate(position_ids):
        pool_id = pool_address[idx]
        if pool_id not in gauges:
            gauges[pool_id] = w3.eth.contract(
                address=w3.to_checksum_address(pool_infos[pool_id].gauge), abi=GAUGE_ABI
            )
        calls.append(
            (
                w3.to_checksum_address(pool_infos[pool_id].gauge),
                gauges[pool_id].encodeABI(
                    fn_name="earned", args=[position_owners[idx], pos_id]
                ),
            )
        )

    # Use tryAggregate: returns [(success, returnData), ...]
    results = multicall.functions.tryAggregate(False, calls).call(
        block_identifier=block
    )

    rewards = []
    for idx, (success, result) in enumerate(results):
        if success:
            reward_amt = w3.codec.decode(["uint256"], result)[0] / 1e18
        else:
            reward_amt = 0
        rewards.append(
            {
                "rewardAmt": reward_amt,
                "position_id": position_ids[idx],
            }
        )

    return rewards


def compute_exposures(
    w3: Web3,
    positions: List,
    block: int,
    extra_tokens: List[str] = [],
    pool_tokens=None,
):
    extra_tokens = [w3.to_checksum_address(el) for el in extra_tokens]
    exps = []
    pool_data = [(pos[1][2], pos[1][3], pos[1][4]) for pos in positions]
    pool_addresses = get_pools_batch(w3, pool_data, block=block)
    if pool_tokens:
        pool_infos: List[CLMMSnapNoToken] = fetch_pool_infos_w_tvl(
            w3, pool_addresses, pool_tokens, block
        )
    else:
        pool_infos: List[CLMMSnapNoToken] = fetch_pool_infos(w3, pool_addresses, block)
    pool_infos: Mapping[str, CLMMSnapNoToken] = {
        pool.pool_id: pool for pool in pool_infos
    }
    token_addrs = set(extra_tokens)
    for pi in pool_infos.values():
        token_addrs.add(pi.token0)
        token_addrs.add(pi.token1)
    token_addrs = list(token_addrs)
    token_infos: List[Token] = fetch_token_infos(w3, token_addrs, block)
    token_infos: Mapping[str, Token] = {token.address: token for token in token_infos}

    for idx, (pos_id, pos) in enumerate(positions):
        if pos is None:
            continue
        pool_id = pool_addresses[idx]
        pool_info: CLMMSnapNoToken = pool_infos[pool_id]
        active_tick = pool_info.slot0.activeTick
        liquidity = pos[7]
        tick_lower = pos[5]
        tick_upper = pos[6]
        sqrt_pb = 1.0001 ** (tick_upper / 2)
        sqrt_p0 = 1.0001 ** (active_tick / 2)
        sqrt_pa = 1.0001 ** (tick_lower / 2)

        amt0 = 0
        amt1 = 0

        if active_tick <= tick_lower:
            amt0 = liquidity * (1 / sqrt_pa - 1 / sqrt_pb)
        elif active_tick < tick_upper:
            amt0 = liquidity * (1 / sqrt_p0 - 1 / sqrt_pb)
            amt1 = liquidity * (sqrt_p0 - sqrt_pa)
        else:
            amt1 = liquidity * (sqrt_pb - sqrt_pa)

        amt0 = amt0 / 10 ** token_infos[pool_info.token0].decimals
        amt1 = amt1 / 10 ** token_infos[pool_info.token1].decimals

        exps.append(
            {
                "position_id": pos_id,
                token_infos[pool_info.token0].symbol: amt0,
                token_infos[pool_info.token1].symbol: amt1,
            }
        )

    return exps, pool_infos, token_infos, positions, pool_addresses


def get_exposures(
    w3, position_ids, block, extra_tokens: List[str] = [], pool_tokens=None
):
    exps = []
    positions = get_positions_batch(w3, position_ids, block)
    positions = [pos for pos in positions if pos[1] is not None]
    exps, pool_infos, token_infos, positions, pool_addresses = compute_exposures(
        w3, positions, block, extra_tokens=extra_tokens, pool_tokens=pool_tokens
    )
    return (
        pd.DataFrame(exps).fillna(0).set_index("position_id"),
        pool_infos,
        token_infos,
        positions,
        pool_addresses,
        block,
    )


def get_wallet_exp(w3, eoa, tokens, block):
    balances = {}
    for token in tokens.values():
        contract = w3.eth.contract(address=token.address, abi=ERC20_ABI)
        balances[token.symbol] = (
            contract.functions.balanceOf(eoa).call(block_identifier=block)
            / 10**token.decimals
        )
    return balances


def get_balances(w3: Web3, eoa: str, tokens: Mapping[str, Token], block: int):
    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)

    # Encode balanceOf(eoa) for each token
    calls = []
    for token in tokens.values():
        contract = w3.eth.contract(address=token.address, abi=ERC20_ABI)
        call_data = contract.encodeABI(fn_name="balanceOf", args=[eoa])
        calls.append((token.address, call_data))

    # Format for multicall: list of tuples -> list of dicts
    call_structs = [{"target": addr, "callData": data} for addr, data in calls]

    # Call Multicall3.aggregate
    _, return_data = multicall.functions.aggregate(call_structs).call(
        block_identifier=block
    )

    # Decode each return value as uint256 and divide by decimals
    balances = {}
    for token, raw in zip(tokens.values(), return_data):
        balance = int.from_bytes(raw[-32:], byteorder="big")
        balances[token.address] = balance


    return balances


def get_position_exposures(
    w3: Web3,
    positions: Mapping[int, Dict],
    block: int,
    safe_tokens: List[str],
    pool_tokens=None,
):
    position_ids = [el for el in positions.keys()]
    df, pool_infos, _, pos_raw, pool_addresses, block = get_exposures(
        w3,
        position_ids,
        block,
        extra_tokens=safe_tokens,
        pool_tokens=pool_tokens,
    )
    position_ids = [p[0] for p in pos_raw]
    pos_owners = [positions[pid]["tokenOwner"] for pid in position_ids]
    rewards = get_rewards(
        w3, block, pool_infos, position_ids, pos_owners, pool_addresses
    )
    rewards = pd.DataFrame(rewards).set_index("position_id")
    if "AERO" not in df.columns:
        df["AERO"] = 0
    df["AERO"] += rewards["rewardAmt"]
    return df, rewards, pool_infos


def get_exp_with_rewards(
    w3, eoa, position_ids, block, safe_tokens, wallet=True, pool_tokens=None
):
    df, pool_infos, token_infos, positions, pool_addresses, block = get_exposures(
        w3,
        position_ids,
        block,
        extra_tokens=safe_tokens.keys(),
        pool_tokens=pool_tokens,
    )
    if wallet:
        balances = get_balances(w3, eoa, token_infos, block)
        balances = {
            token_infos[addr].symbol: val / 10 ** token_infos[addr].decimals
            for addr, val in balances.items()
        }
    else:
        balances = None
    position_ids = [p[0] for p in positions]
    position_owners = [eoa for p in positions]
    rewards = get_rewards(w3, block, pool_infos, position_ids, position_owners, pool_addresses)
    rewards = pd.DataFrame(rewards).set_index("position_id")
    if "AERO" not in df.columns:
        df["AERO"] = 0
    df["AERO"] += rewards["rewardAmt"]
    if balances:
        new_row = pd.DataFrame(balances, index=["wallet"])
        df = pd.concat([df, new_row])
    return df, rewards, pool_infos


if __name__ == "__main__":
    import os
    import dotenv

    dotenv.load_dotenv()

    eoa = os.environ["LP_ADDR"]

    position_ids = [
        17795318,
        17796076,
        16848700,
        17807290,
        17808028,
    ]

    w3 = get_w3()
    block = w3.eth.block_number
    df, rewards = get_exp_with_rewards(w3, eoa, position_ids, block)
    print(rewards)
    print(df.sum())
