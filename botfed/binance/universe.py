"""Some universe utilities"""


def binance_contract_to_coin(contract):
    base_curr = contract.lower().replace("usdt", "").replace("usdc", "").upper()
    return base_curr.replace("1000", "k")


def coin_to_binance_contract(coin):
    if coin.upper() in ["WETH"]:
        coin = "ETH"
    elif coin.upper() in ["CBBTC", "WBTC", "TBTC"]:
        coin = "BTC"
    elif coin.upper() in ["USUI"]:
        coin = "SUI"
    try:
        contract = coin
        if contract[0] == "k":
            contract = "1000" + contract[1:]
        if contract[-4:].lower() != "usdt":
            contract = contract + "usdt"
        return contract.upper()
    except Exception as e:
        print("error parsing coin", coin)
        raise e


def coin_to_coin(coin):
    if coin.upper() in ["WETH"]:
        coin = "ETH"
    elif coin.upper() in ["CBBTC", "WBTC", "TBTC"]:
        coin = "BTC"
    elif coin.upper() in ["USUI"]:
        coin = "SUI"
    return coin.upper()
