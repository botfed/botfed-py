from decimal import Decimal
from math import sqrt

Q96 = Decimal(2**96)


def tick_to_sqrtPriceX96(tick: int) -> Decimal:
    return Decimal(1.0001) ** (Decimal(tick) / 2) * Q96


def get_liquidity_from_amounts(amount0, amount1, sqrtP, sqrtA, sqrtB):
    sqrtP = Decimal(sqrtP)
    sqrtA = Decimal(sqrtA)
    sqrtB = Decimal(sqrtB)
    if sqrtA > sqrtB:
        sqrtA, sqrtB = sqrtB, sqrtA

    if sqrtP <= sqrtA:
        return (amount0 * sqrtA * sqrtB) / (sqrtB - sqrtA)
    elif sqrtP < sqrtB:
        liquidity0 = (amount0 * sqrtP * sqrtB) / (sqrtB - sqrtP)
        liquidity1 = amount1 / (sqrtP - sqrtA)
        return min(liquidity0, liquidity1)
    else:
        return amount1 / (sqrtB - sqrtA)


def liquidity_share_for_1_dollar(
    tick_lower,
    tick_upper,
    active_tick,
    active_liquidity,
    token0_price_usd,
    token1_price_usd,
):
    sqrtP = tick_to_sqrtPriceX96(active_tick)
    sqrtA = tick_to_sqrtPriceX96(tick_lower)
    sqrtB = tick_to_sqrtPriceX96(tick_upper)
    price = (sqrtP / Q96) ** 2

    # Split $1 into token0 and token1 proportionally to value at current price
    p0 = Decimal(token0_price_usd)
    p1 = Decimal(token1_price_usd)
    total = p0 + p1 * price
    amount0 = Decimal(1) * p0 / total
    amount1 = Decimal(1) * p1 / total

    L_you = get_liquidity_from_amounts(amount0, amount1, sqrtP, sqrtA, sqrtB)
    share = L_you / (Decimal(active_liquidity) + L_you)
    return float(share), float(L_you)


if __name__ == "__main__":
    tick_lower, tick_upper, active_tick = 96300, 101300, 98982
    active_liq = 1008041765206755982935004
    token0_price, token1_price = 2950, 0.1461
    share, l_me = liquidity_share_for_1_dollar(
        tick_lower, tick_upper, active_tick, active_liq, token0_price, token1_price
    )
    print(share, l_me)
