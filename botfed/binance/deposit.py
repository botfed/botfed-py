import os
from web3 import Web3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to BSC
bsc_rpc_url = "https://bsc-dataseed.binance.org/"
web3 = Web3(Web3.HTTPProvider(bsc_rpc_url))

# Configuration from environment variables
private_key = os.getenv("SQUID_PRIVATE_KEY")
recipient_address = os.getenv("BIN_DEPOSIT_ADDRESS")
# Get the sender address from the private key
account = web3.eth.account.from_key(private_key)
sender_address = account.address

# Other configuration
coins = {
    "USDT": {
        "BSC": {
            "addr": "0x55d398326f99059fF775485246999027B3197955",
            "decimals": 18,
        },
    }
}

# ERC20 Contract ABI (simplified for transfer function)
erc20_abi = [
    {
        "constant": False,
        "inputs": [
            {"name": "_to", "type": "address"},
            {"name": "_value", "type": "uint256"},
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function",
    }
]


def deposit(recipient_address: str, coin: str, chain: str, amount: float):
    # Check connection
    if not web3.is_connected():
        raise Exception("Failed to connect to BSC")

    # Get nonce
    nonce = web3.eth.get_transaction_count(sender_address)

    contract_address = coins[coin][chain]["addr"]
    contract = web3.eth.contract(address=contract_address, abi=erc20_abi)

    # Get gas price
    gas_price = web3.eth.gas_price
    decimals = coins[coin][chain]["decimals"]
    amount_to_send = int(amount) * 10**decimals

    # Create transaction
    transaction = contract.functions.transfer(
        web3.to_checksum_address(recipient_address), amount_to_send
    ).build_transaction(
        {
            "chainId": 56,  # BSC mainnet chain ID
            "gas": 200000,  # You might need to adjust the gas limit based on contract requirements
            "gasPrice": gas_price,
            "nonce": nonce,
        }
    )
    print(transaction)

    # Sign transaction
    signed_tx = web3.eth.account.sign_transaction(transaction, private_key)

    # Send transaction
    tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)

    # Output transaction hash
    print(f"Transaction sent with hash: {web3.to_hex(tx_hash)}")

    # Optional: Wait for the transaction receipt
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    print(f"Transaction receipt: {receipt}")


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
        "--to", type=str, help="to address", default=os.getenv("BIN_DEPOSIT_ADDRESS")
    )
    args = parser.parse_args()

    deposit(args.to, args.coin, args.chain, args.amount)
