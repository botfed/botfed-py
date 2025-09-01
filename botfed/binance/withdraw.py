import requests
import hmac
import hashlib
import time
import json
import os
import dotenv


dotenv.load_dotenv()

# Replace with your actual Binance API key and secret
api_key = os.getenv("BIN_KEY")
api_secret = os.getenv("BIN_SECRET")


# Binance API endpoints
base_url = "https://api.binance.com"
withdraw_endpoint = "/sapi/v1/capital/withdraw/apply"


def sign_request(params, secret):
    query_string = "&".join(
        [f"{key}={params[key]}" for key in params]
    )  # sorted(params)])
    print(query_string)
    signature = hmac.new(
        secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return signature


def withdraw(asset, address, network, amount, withdraw_order_id):
    timestamp = int(time.time() * 1000)
    params = {
        "coin": asset,
        "address": address,
        "network": network,
        "amount": amount,
        # 'withdrawOrderId': withdraw_order_id,
        "timestamp": timestamp,
    }

    signature = sign_request(params, api_secret)
    print(signature)
    headers = {"X-MBX-APIKEY": api_key}

    params["signature"] = signature
    print(params)
    response = requests.post(
        base_url + withdraw_endpoint, headers=headers, params=params
    )

    if response.status_code == 200:
        print("Withdrawal request successful!")
        print("Response:\n", json.dumps(response.json(), indent=2))
    else:
        print("Error with withdrawal request.")
        print("Status Code:", response.status_code)
        print("Response:\n", json.dumps(response.json(), indent=2))


def get_all():
    endpoint = "/sapi/v1/capital/config/getall"
    timestamp = int(time.time() * 1000)
    params = {"timestamp": timestamp}

    signature = sign_request(params, api_secret)
    print(signature)
    headers = {"X-MBX-APIKEY": api_key}

    params["signature"] = signature
    print(params)
    response = requests.get(base_url + endpoint, headers=headers, params=params)

    if response.status_code == 200:
        print("Withdrawal request successful!")
        print("Response:\n", json.dumps(response.json(), indent=2))
    else:
        print("Error with withdrawal request.")
        print("Status Code:", response.status_code)
        print("Response:\n", json.dumps(response.json(), indent=2))


def test_sig():
    api_key = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
    api_secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
    params = {
        "symbol": "LTCBTC",
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": 1,
        "price": 0.1,
        "recvWindow": 5000,
        "timestamp": 1499827319559,
    }
    signature = sign_request(params, api_secret)
    print(signature)
    assert (
        signature == "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--coin",
        type=str,
        help="Binance coin symbol (ie: USDT, USDC, BNB, etc)",
        required=True,
    )
    parser.add_argument(
        "--chain",
        type=str,
        help="EVM compatible chain (ie: BSC, ETH, ARBITRUM, etc)",
        required=True,
    )
    parser.add_argument("--amount", type=float, help="amount")
    parser.add_argument(
        "--to", type=str, help="to address", default=os.getenv("BIN_WITHDRAW_TO")
    )
    args = parser.parse_args()

    # get_all()
    # Execute the withdrawal
    withdraw(args.coin, args.to, args.chain, args.amount, None)
    # test_sig()
