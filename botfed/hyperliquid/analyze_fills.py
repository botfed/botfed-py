import datetime as dt


def pnl_from_fills(fills):
    fills = sorted(fills, key=lambda x: x["time"])
    fees = 0
    ntl_buy = 0
    ntl_sell = 0
    total_buy = 0
    total_sell = 0
    for fill in fills:
        fees += float(fill["fee"]) if fill['feeToken'] == 'USDC' else float(fill["fee"]) * float(fill["px"])
        if fill["side"] == "A":
            ntl_sell += float(fill["px"]) * float(fill["sz"])
            total_sell += float(fill["sz"])
        else:
            ntl_buy += float(fill["px"]) * float(fill["sz"])
            total_buy += float(fill["sz"])
    avg_buy = ntl_buy / total_buy
    avg_sell = ntl_sell / total_sell
    pnl = (avg_sell - avg_buy) * (total_sell + total_buy) - fees
    return {
        "ts_start": dt.datetime.fromtimestamp(fills[0]["time"]/1000).strftime("%Y-%m-%d %H:%M:%S"),
        "ts_end": dt.datetime.fromtimestamp(fills[-1]["time"]/1000).strftime("%Y-%m-%d %H:%M:%S"),
        "pnl": pnl,
        "avg_buy": avg_buy,
        "avg_sell": avg_sell,
        "fees": fees,
        "total_buy": ntl_buy,
        "total_sell": ntl_sell,
    }
