import requests


def get_ohlcv(coin, start_time, end_time, interval="1h"):
    """Example paylaod:
    {
    "req": {
        "coin": "PURR/USDC",
        "endTime": 1713620664000,
        "interval": "1h",
        "startTime": 1712436264000
    },
    "type": "candleSnapshot"
    }
    """
    payload = {
        "req": {
            "coin": coin,
            "endTime": end_time,
            "interval": interval,
            "startTime": start_time,
        },
        "type": "candleSnapshot",
    }

    url = "https://api-ui.hyperliquid.xyz/info"

    response = requests.post(url, json=payload)

    return response.json()


if __name__ == "__main__":

    data = get_ohlcv("PURR/USDC", 1712436264000, 1713620664000)
    print(data)

    data = get_ohlcv("ETH", 1712436264000, 1713620664000)
    print(data)
