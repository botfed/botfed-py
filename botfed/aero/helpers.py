from decimal import Decimal
from typing import List, Dict
from dataclasses import dataclass
from web3 import Web3

from .abis.pool_clmm import ABI as POOL_ABI
from .abis.multicall3 import ABI as MULTICALL3_ABI
from .abis.erc20 import ABI as ERC20_ABI
from .vars import TEST_POOL_ID, get_w3, TEST_BLOCK, MULTICALL3_ADDR

QUOTE_TOKENS = [
    "USDC",
    "USDT",
    "USD+",
    "cbBTC",
    "WBTC",
    "tBTC",
    "WETH",
]


@dataclass
class Token:
    address: str
    name: str
    symbol: str
    decimals: int


@dataclass
class Slot0:
    sqrtPriceX96: int
    activeTick: int
    obsIdx: int
    obsCard: int
    obsCardNext: int
    unlocked: bool


@dataclass
class CLMMSnapNoToken:
    pool_id: str
    token0: str
    token1: str
    fee: int
    tickSpacing: int
    liquidity: int
    stakedLiquidity: int
    rewardRate: int
    gauge: str
    slot0: Slot0
    block_number: int
    amt0: int = 0
    amt1: int = 0

    def name(self) -> str:
        return f"CL{self.tickSpacing}"


@dataclass
class CLMMSnap:
    pool_id: str
    token0: Token
    token1: Token
    fee: int
    tickSpacing: int
    liquidity: int
    stakedLiquidity: int
    rewardRate: int
    gauge: str
    slot0: Slot0
    block_number: int

    def name(self) -> str:
        return f"{self.token0.symbol}/{self.token1.symbol}CL{self.tickSpacing}"


def get_quote_token(symbol0, symbol1):

    quote_curr = None
    quote_curr_pos = None
    for token in QUOTE_TOKENS:
        if token.upper() == symbol0.upper():
            quote_curr = token
            base_curr = symbol1
            quote_curr_pos = 0
            break
        elif token.upper() == symbol1.upper():
            quote_curr = token
            base_curr = symbol0
            quote_curr_pos = 1
            break
    return quote_curr, base_curr, quote_curr_pos


def fetch_pool_infos_w_tvl(
    w3: Web3, pool_ids: List[str], tokens: Dict, block_number: int | str = "latest"
) -> List[CLMMSnap]:
    if block_number == "latest":
        block_number = w3.eth.block_number
    calls = []
    types = []
    len_pd = 11
    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)
    for pool_id in pool_ids:
        pool = w3.eth.contract(address=pool_id, abi=POOL_ABI)
        token0 = w3.eth.contract(address=tokens[pool_id]["token0"], abi=ERC20_ABI)
        token1 = w3.eth.contract(address=tokens[pool_id]["token1"], abi=ERC20_ABI)
        calls_, types_ = make_pool_info_w_tvl_calls(pool, token0, token1)
        assert len(types_) == len_pd
        calls += calls_
        types += types_

    aggregate_call = multicall.functions.tryAggregate(
        False, [(pool_id, call) for pool_id, call in calls]
    )
    results = aggregate_call.call(block_identifier=block_number)
    pool_infos = []
    for odx, pool_id in enumerate(pool_ids):
        pool_data_raw = []
        for idx, type_ in enumerate(types[0:len_pd]):
            success, data = results[odx * len_pd + idx]
            if not success:
                pool_data_raw.append(None)
                continue
            result = w3.codec.decode(type_, data)
            if len(result) == 1:
                result = result[0]
            pool_data_raw.append(result)
        if all([val is not None for val in pool_data_raw]):
            pool_info = CLMMSnapNoToken(
                pool_id,
                *pool_data_raw[0:8],
                Slot0(*pool_data_raw[-3]),
                block_number,
                pool_data_raw[-2],
                pool_data_raw[-1],
            )
            pool_info.token0 = w3.to_checksum_address(pool_info.token0)
            pool_info.token1 = w3.to_checksum_address(pool_info.token1)
            pool_infos.append(pool_info)
    return pool_infos


def fetch_pool_infos(
    w3: Web3, pool_ids: List[str], block_number: int | str = "latest"
) -> List[CLMMSnapNoToken]:
    if block_number == "latest":
        block_number = w3.eth.block_number
    calls = []
    types = []
    len_pd = 9
    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)
    for pool_id in pool_ids:
        pool = w3.eth.contract(address=pool_id, abi=POOL_ABI)
        calls_, types_ = make_pool_info_calls(pool)
        assert len(types_) == len_pd
        calls += [(pool_id, c) for c in calls_]
        types += types_

    aggregate_call = multicall.functions.aggregate(
        [(pool_id, call) for pool_id, call in calls]
    )
    _, return_data = aggregate_call.call(block_identifier=block_number)
    pool_infos = []
    for odx, pool_id in enumerate(pool_ids):
        pool_data_raw = []
        for idx, type_ in enumerate(types[0:len_pd]):
            result = w3.codec.decode(type_, return_data[odx * len_pd + idx])
            if len(result) == 1:
                result = result[0]
            pool_data_raw.append(result)
        pool_info = CLMMSnapNoToken(
            pool_id, *pool_data_raw[0:8], Slot0(*pool_data_raw[-1]), block_number
        )
        pool_info.token0 = w3.to_checksum_address(pool_info.token0)
        pool_info.token1 = w3.to_checksum_address(pool_info.token1)
        pool_infos.append(pool_info)
    return pool_infos


def fetch_token_infos(
    w3: Web3, token_addrs: List[str], block_number: int | str = "latest"
) -> List[Token]:
    results: List[Token] = []
    if block_number == "latest":
        block_number = w3.eth.block_number
    calls = []
    types = []
    len_pd = 3
    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)
    for addr in token_addrs:
        contract = w3.eth.contract(address=w3.to_checksum_address(addr), abi=ERC20_ABI)
        calls_, types_ = make_token_info_calls(contract)
        assert len(types_) == len_pd
        calls += [(addr, c) for c in calls_]
        types += types_

    aggregate_call = multicall.functions.aggregate(
        [(addr, call) for addr, call in calls]
    )
    _, return_data = aggregate_call.call(block_identifier=block_number)
    for odx, addr in enumerate(token_addrs):
        results_ = []
        for idx, type_ in enumerate(types[0:len_pd]):
            result = w3.codec.decode(type_, return_data[odx * len_pd + idx])
            if len(result) == 1:
                result = result[0]
            results_.append(result)
        info = Token(addr, *results_)
        results.append(info)
    return results


def fetch_info_from_pool_id(
    w3: Web3, pool_id: str, block_number: int | str = "latest"
) -> CLMMSnap:
    if block_number == "latest":
        block_number = w3.eth.block_number
    pool = w3.eth.contract(address=pool_id, abi=POOL_ABI)
    multicall = w3.eth.contract(address=MULTICALL3_ADDR, abi=MULTICALL3_ABI)
    pool_data = fetch_pool_info(w3.codec, multicall, pool, block_number)
    token0 = w3.eth.contract(
        address=Web3.to_checksum_address(pool_data[0]), abi=ERC20_ABI
    )
    token1 = w3.eth.contract(
        address=Web3.to_checksum_address(pool_data[1]), abi=ERC20_ABI
    )
    token0_data = fetch_token_info(w3.codec, multicall, token0, block_number)
    token1_data = fetch_token_info(w3.codec, multicall, token1, block_number)
    token0_info, token1_info = Token(token0.address, *token0_data), Token(
        token1.address, *token1_data
    )
    slot0 = Slot0(*pool_data[-1])
    pool_info = CLMMSnap(
        pool_id, token0_info, token1_info, *pool_data[2:-1], slot0, block_number
    )
    return pool_info


def fetch_token_info(codec, multicall, contract, block):
    calls, types = make_token_info_calls(contract)
    aggregate_call = multicall.functions.aggregate(
        [(contract.address, call) for call in calls]
    )
    _, return_data = aggregate_call.call(block_identifier=block)
    results = []
    for idx, type_ in enumerate(types):
        result = codec.decode(type_, return_data[idx])
        if len(result) == 1:
            result = result[0]
        results.append(result)
    return results


def make_token_info_calls(contract):
    calls = [
        contract.functions.name()._encode_transaction_data(),
        contract.functions.symbol()._encode_transaction_data(),
        contract.functions.decimals()._encode_transaction_data(),
    ]
    types = [
        ["string"],
        ["string"],
        ["uint8"],
    ]
    return calls, types


def make_pool_info_calls(pool_contract):
    calls = [
        pool_contract.functions.token0()._encode_transaction_data(),
        pool_contract.functions.token1()._encode_transaction_data(),
        pool_contract.functions.fee()._encode_transaction_data(),
        pool_contract.functions.tickSpacing()._encode_transaction_data(),
        pool_contract.functions.liquidity()._encode_transaction_data(),
        pool_contract.functions.stakedLiquidity()._encode_transaction_data(),
        pool_contract.functions.rewardRate()._encode_transaction_data(),
        pool_contract.functions.gauge()._encode_transaction_data(),
        pool_contract.functions.slot0()._encode_transaction_data(),
    ]
    types = [
        ["address"],
        ["address"],
        ["uint24"],
        ["int24"],
        ["uint128"],
        ["uint128"],
        ["uint256"],
        ["address"],
        ["uint160", "int24", "uint16", "uint16", "uint16", "bool"],
    ]
    return calls, types


def make_pool_info_w_tvl_calls(pool_contract, token0, token1):
    calls = [
        pool_contract.functions.token0()._encode_transaction_data(),
        pool_contract.functions.token1()._encode_transaction_data(),
        pool_contract.functions.fee()._encode_transaction_data(),
        pool_contract.functions.tickSpacing()._encode_transaction_data(),
        pool_contract.functions.liquidity()._encode_transaction_data(),
        pool_contract.functions.stakedLiquidity()._encode_transaction_data(),
        pool_contract.functions.rewardRate()._encode_transaction_data(),
        pool_contract.functions.gauge()._encode_transaction_data(),
        pool_contract.functions.slot0()._encode_transaction_data(),
        token0.functions.balanceOf(pool_contract.address)._encode_transaction_data(),
        token1.functions.balanceOf(pool_contract.address)._encode_transaction_data(),
    ]
    calls = [(pool_contract.address, c) for c in calls[:-2]] + [
        (token0.address, calls[-2]),
        (token1.address, calls[-1]),
    ]
    types = [
        ["address"],
        ["address"],
        ["uint24"],
        ["int24"],
        ["uint128"],
        ["uint128"],
        ["uint256"],
        ["address"],
        ["uint160", "int24", "uint16", "uint16", "uint16", "bool"],
        ["uint256"],
        ["uint256"],
    ]
    return calls, types


def fetch_pool_info(codec, multicall, pool_contract, block):
    calls, types = make_pool_info_calls(pool_contract)
    aggregate_call = multicall.functions.aggregate(
        [(pool_contract.address, call) for call in calls]
    )
    _, return_data = aggregate_call.call(block_identifier=block)
    results = []
    for idx, type_ in enumerate(types):
        result = codec.decode(type_, return_data[idx])
        if len(result) == 1:
            result = result[0]
        results.append(result)
    return results


def liq_to_quote_amt(tick_spacing, active_liq, active_tick, quote_curr_pos):
    """
    Compute amount of quote token in active tick range of CLMM.

    Args:
        ticks: Array of tick boundaries.
        tick_spacing: Tick spacing (int).
        liq_curve: Array of liquidity at each tick.
        active_tick: Current active tick (int).
        quote_curr_pos: 0 if quote token is token0, 1 if token1.

    Returns:
        Amount of quote token in active range.
    """

    tick_lower = tick_spacing * (active_tick // tick_spacing)
    liq = active_liq

    sqrt_p_a = Decimal(1.0001 ** (tick_lower / 2))
    sqrt_p_b = Decimal(1.0001 ** ((tick_lower + tick_spacing) / 2))

    if quote_curr_pos == 1:
        # Quote token is token1, amount1 formula:
        amt = liq * (sqrt_p_b - sqrt_p_a)
    else:
        # Quote token is token0, amount0 formula:
        amt = liq * (1 / sqrt_p_a - 1 / sqrt_p_b)

    return amt


if __name__ == "__main__":
    from .vars import watch_pools

    w3 = get_w3()
    pool_id = TEST_POOL_ID
    pool_info = fetch_info_from_pool_id(w3, pool_id)
    print(pool_info)
    pool_infos = fetch_pool_infos(w3, watch_pools)
    for p in pool_infos:
        print(p)
    token_addrs = set([])
    for pi in pool_infos:
        token_addrs.add(w3.to_checksum_address(pi.token0))
        token_addrs.add(w3.to_checksum_address(pi.token1))
    token_addrs = list(token_addrs)
    tokens = fetch_token_infos(w3, token_addrs)
    for token in tokens:
        print(token)
