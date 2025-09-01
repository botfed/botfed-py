from . import hyp
from . import bin
from ..binance.universe import binance_contract_to_coin
from .coinmarketcap import cm_load_uni

# hyp_uni = hyp.load_uni()
bin_uni = bin.load_uni("perps")
#
coins_all = [binance_contract_to_coin(el) for el in bin_uni["id"].unique()]
coins_all = [el for el in coins_all if el is not None and el.strip() != ""]
# coins_hyp = hyp_uni['name'].unique()
#
# coins_hyp_bin = [el for el in coins_hyp if el in coins_bin]
#
# coins_10 = coins_hyp_bin[0:10]
# coins_20 = coins_hyp_bin[0:20]
# coins_30 = coins_hyp_bin[0:30]
# coins_40 = coins_hyp_bin[0:40]
# coins_50 = coins_hyp_bin[0:50]


coins_1 = ["kPEPE"]
coins_10 = sorted(
    list(
        set(
            [
                "BTC",
                "ETH",
                "SOL",
                "kPEPE",
                "TON",
                "ZK",
                "WIF",
                "DOGE",
                "kBONK",
                "AVAX",
                "LDO",
            ]
        )
    )
)
coins_20 = sorted(
    list(
        set(
            coins_10
            + ["FET", "MKR", "TIA", "CRV", "ENS", "BCH", "NEAR", "WLD", "BNB", "ARB"]
        )
    )
)
coins_30 = sorted(
    list(
        set(
            coins_20
            + [
                "LINK",
                "STX",
                "INJ",
                "JTO",
                "NOT",
                "PENDLE",
                "FTM",
                "IO",
                "MANTA",
                "ORDI",
            ]
        )
    )
)
coins_40 = sorted(
    list(
        set(
            coins_30
            + ["ZRO", "JUP", "ONDO", "ENA", "AR", "STRK", "RNDR", "MYRO", "BOME", "OP"]
        )
    )
)

coins_100 = coins_all[0:100]

# assert len(coins_40) == 40
# assert len(coins_50) == 50


cm_uni = cm_load_uni()
# Create a regex pattern that matches any of the substrings
substrings = ['ethereum', 'meme', 'defi', 'solana', 'pow']
pattern = '|'.join(substrings)
coins_filt = cm_uni[cm_uni["tags"].str.contains(pattern)]["symbol"].unique()
coins_filt = [c for c in coins_all if c in coins_filt]
