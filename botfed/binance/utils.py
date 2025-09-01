import requests


def get_binance_last_price(symbol):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    response = requests.get(url)
    data = response.json()
    if 'code' in data:
        print(f"Error: {data['msg']}")
        return None
    return float(data['price'])

def get_binance_open_interest(symbol):
    url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
    response = requests.get(url)
    data = response.json()
    price = get_binance_last_price(symbol)
    if "code" in data:
        print(f"Error: {data['msg']}")
        return None
    return int(float(data["openInterest"]) * price) 


def main():
    symbol = "WIFUSDT"  # Change this to the symbol of the futures contract you want to monitor
    open_interest = get_binance_open_interest(symbol)
    if open_interest is not None:
        print(f"Open Interest for {symbol}: {open_interest}")


if __name__ == "__main__":
    main()
