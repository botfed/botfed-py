import logging
import time
from .info import Info
from web3 import Web3
from .abis import CALCULATOR_ABI, STORAGE_CONFIG_ABI

CALCULATOR_CONTRACT_ADDRESS = "0x0FdE910552977041Dc8c7ef652b5a07B40B9e006"
STORAGE_CONFIG = "0xF4F7123fFe42c4C90A4bCDD2317D397E0B7d7cc0"


class HyperLiquidApi:

    def __init__(self):
        self.info = Info(skip_ws=True)

    def add_w3(self, w3: Web3):
        self.calculator_contract = w3.eth.contract(
            address=CALCULATOR_CONTRACT_ADDRESS, abi=CALCULATOR_ABI
        )
        self.config_contract = w3.eth.contract(
            address=STORAGE_CONFIG, abi=STORAGE_CONFIG_ABI
        )

    def init(self):
        self.market_configs = self.get_market_configs()
        self.markets = self.get_markets()

    def get_fills(self, user_addr, ts_start, ts_end=None):
        # timestamps are in miliseconds since epoch
        payload = {"type": "userFillsByTime", "user": user_addr, "startTime": ts_start}
        if ts_end:
            payload["endTime"] = int(ts_end)
        results = []
        hashes = {}
        while True:
            print(payload)
            tmp = self.info.post("/info", payload)
            if len(tmp) == 0:
                break
            results += tmp
            payload["startTime"] = int(tmp[-1]["time"]) + 1
            if ts_end and payload["startTime"] > ts_end:
                break
        unique = []
        for el in results:
            if el["hash"] in hashes:
                continue
            hashes[el["hash"]] = True
            unique.append(el)
        return unique

    def get_markets(self, retry=3):
        count = 0
        while count < retry:
            try:
                return self.info.meta()
            except Exception as e:
                logging.debug(f"Error obtaining markets: {e}")
                count += 1
                time.sleep(count**2)
        return {"universe": []}

    def get_market_stats(self):
        try:
            meta = self.info.post("/info", {"type": "metaAndAssetCtxs"})
            if meta is None:
                return []
            universe = meta[0]["universe"]
            stats = meta[1]
            if universe is None or stats is None:
                return []
            for idx, market in enumerate(universe):
                coin = market["name"]
                stat = stats[idx]
                stat["name"] = coin
            return stats
        except Exception as e:
            logging.debug(f"Error obtaining market stats: {e}")
            return []

    def get_market_configs(self):
        results = self.config_contract.functions.getMarketConfigs().call()
        configs = []
        for _, result in enumerate(results):
            ticker = result[0].replace(b"\x00", b"").decode("utf-8") + "-USD"
            configs.append([ticker] + [el for el in result[1:]])
        return configs

    def get_last_funding_rate(self, ticker):
        try:
            end_time = int(time.time() * 1000)  # Current time
            start_time = end_time - 24 * 60 * 60 * 1000  # 24 hours ago
            response = self.info.funding_history(
                coin=ticker, startTime=start_time, endTime=end_time
            )
            if response:
                return response[-1]
            return None
        except Exception as e:
            # print(f"Error obtaining funding rate for {ticker}: {e}")
            return None

    def get_funding_rate_velocity(self, market_index):
        return self.calculator_contract.functions.getFundingRateVelocity(
            market_index
        ).call()
