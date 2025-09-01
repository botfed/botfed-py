import numpy as np
from eth_abi import encode
from eth_utils import keccak
from web3 import Web3
import os
from decimal import Decimal
import dotenv

dotenv.load_dotenv()
from .abis.state_view import ABI as STATE_VIEW_ABI

from .v4_vars import POOL_ID, Q96, STATE_VIEW_ADDR


def get_w3(chain="UNI"):
    w3 = Web3(
        Web3.HTTPProvider(os.environ.get("UNI_RPC_URL", "https://unichain.drpc.org"))
    )
    assert w3.is_connected(), "Failed to connect to RPC"
    return w3


def decode_sqrtPriceX96(
    sqrt_price_x96: int, decimals_token0: int, decimals_token1: int
) -> Decimal:
    sqrt_price = Decimal(sqrt_price_x96) / (1 << 96)
    price = sqrt_price**2

    # Adjust for token decimals: price = token1/token0
    decimal_adjustment = Decimal(10 ** (decimals_token0 - decimals_token1))
    return price * decimal_adjustment


def compute_pool_id(currency0, currency1, fee, tick_spacing, hooks):
    encoded = encode(
        ["address", "address", "uint24", "int24", "address"],
        [currency0, currency1, fee, tick_spacing, hooks],
    )
    return keccak(encoded)


def get_contract(w3: Web3, contract_addr, abi):
    return w3.eth.contract(address=contract_addr, abi=abi)


def get_pool_state(state_view, pool_id: bytes, block="latest"):
    slot0 = state_view.functions.getSlot0(pool_id).call(block_identifier=block)
    return slot0


def compute_pool_tvl(sqrt_price_x96, liquidity, price_token0_usd, price_token1_usd):
    sqrt_price = Decimal(sqrt_price_x96) / Q96

    amount0 = Decimal(liquidity) / sqrt_price  # token0 amount
    amount1 = Decimal(liquidity) * sqrt_price  # token1 amount

    tvl = (amount0 * Decimal(price_token0_usd)) + (amount1 * Decimal(price_token1_usd))
    return {
        "amount0": float(amount0),
        "amount1": float(amount1),
        "TVL_USD_M": float(tvl) / 1e6,
    }


FEE_TO_SPACING = {100: 1, 500: 10, 3000: 60, 10000: 200}


def decode_tick_bitmap(bitmap_int, word_pos, tick_spacing):
    initialized_ticks = []
    for bit_index in range(256):
        if (bitmap_int >> bit_index) & 1:
            tick_idx = (word_pos * 256 + bit_index) * tick_spacing
            initialized_ticks.append(tick_idx)
    return initialized_ticks


def get_sqrt_price_x96_from_tick(tick: int) -> int:
    sqrt_price = Decimal(1.0001) ** (Decimal(tick) / 2)
    sqrt_price_x96 = sqrt_price * Q96
    return int(sqrt_price_x96)


def compute_tvl_in_range(state_view, pool_id, range_pct=(-10, 10), block="latest"):
    start_liquidity = state_view.functions.getLiquidity(pool_id).call(
        block_identifier=block
    )
    slot0 = get_pool_state(state_view, pool_id, block=block)
    fee = slot0[-1]
    tick_spacing = FEE_TO_SPACING[fee]
    active_tick = slot0[1]
    # each tick covers one bip
    tick_lower = active_tick + int(1e2 * range_pct[0])
    tick_upper = active_tick + int(1e2 * range_pct[1])
    word_lower = tick_lower // tick_spacing // 256
    word_upper = tick_upper // tick_spacing // 256
    initialized_ticks = []
    print(word_upper, word_lower)
    for word_pos in range(word_lower, word_upper + 1):
        bitmap = state_view.functions.getTickBitmap(pool_id, word_pos).call(
            block_identifier=block
        )
        initialized_ticks += decode_tick_bitmap(bitmap, word_pos, tick_spacing)
    right_ticks = [t for t in initialized_ticks if t >= active_tick and t <= tick_upper]
    left_ticks = [t for t in initialized_ticks if t < active_tick and t >= tick_lower]
    liq_curve = [(active_tick, start_liquidity)]
    liquidity = start_liquidity
    for tick in right_ticks:
        print("r", tick)
        tick_info = state_view.functions.getTickInfo(pool_id, tick).call(
            block_identifier=block
        )
        liq_net = tick_info[1]
        liquidity += liq_net
        liq_curve.append((tick, liquidity))

    liquidity = start_liquidity
    left_liq_curve = []
    for tick in left_ticks[::-1]:
        print("l", tick)
        tick_info = state_view.functions.getTickInfo(pool_id, tick).call(
            block_identifier=block
        )
        liq_net = tick_info[1]
        liquidity -= liq_net
        left_liq_curve.append((tick, liquidity))

    liq_curve = left_liq_curve[::-1] + liq_curve

    return liq_curve, active_tick, start_liquidity


def compute_amounts(liq_curve, active_tick):
    amount0 = 0
    amount1 = 1
    for tick, liq in liq_curve:
        p_a = 1.0001 ** (tick)
        p_b = 1.0001 ** (tick + 1)
        if tick < active_tick:
            amount0 += liq * (np.sqrt(p_b) - np.sqrt(p_a))
        else:
            amount1 += liq * (1 / np.sqrt(p_a) - 1 / np.sqrt(p_b))
    return amount0, amount1


if __name__ == "__main__":
    import json

    w3 = get_w3()
    state_view = get_contract(w3, STATE_VIEW_ADDR, STATE_VIEW_ABI)

    block = w3.eth.block_number
    block = 19506992
    tick_spacing = 10
    quote_curr = 0
    dec0, dec1 = 6, 18
    quote_dec, base_dec = (dec0, dec1) if quote_curr == 0 else (dec1, dec0)
    slot0 = get_pool_state(state_view, POOL_ID, block=block)
    active_price = float(decode_sqrtPriceX96(slot0[0], base_dec, quote_dec))
    print(active_price)

    range_pct = 15

    fpath = f"./datasets/liq_curve_{block}_r{int(range_pct)}.json"
    try:
        data = json.load(open(fpath, "r"))
        liq_curve, active_tick, active_liq = (
            data["liq_curve"],
            data["active_tick"],
            data["active_liq"],
        )
    except Exception as e:
        print(e)
        liq_curve, active_tick, active_liq = compute_tvl_in_range(
            state_view, POOL_ID, block=block, range_pct=(-range_pct, range_pct)
        )
        json.dump(
            {
                "liq_curve": liq_curve,
                "active_tick": active_tick,
                "active_liq": active_liq,
            },
            open(fpath, "w"),
        )
    amt0, amt1 = compute_amounts(liq_curve, active_tick)
    p_b = 1.0001 ** (active_tick + tick_spacing)
    p_a = 1.0001 ** (active_tick)
    amt0_in_range = active_liq * (np.sqrt(p_b) - np.sqrt(p_a))

    tvl_in_range = (amt0 if quote_curr == 0 else amt1) / 10**quote_dec + float(
        amt1 if quote_curr == 0 else amt0
    ) * active_price / 10**base_dec
    tvl_active = amt0_in_range / 10**quote_dec
    print(
        f"Amt0={amt0 / 10**dec0:,.2f}, Amt1={amt1 / 10**dec1:,.2f}, Active TVL=${tvl_active:,.0f}"
    )
    print(f"TVL in Range: ${tvl_in_range:,.0f}")
