from web3 import Web3
from typing import List, Dict, Any, Optional
import json
from .vars import get_w3, POS_MANAGER_ADDR


# Minimal ABI for Transfer event
TRANSFER_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "from", "type": "address"},
        {"indexed": True, "name": "to", "type": "address"},
        {"indexed": True, "name": "tokenId", "type": "uint256"},
    ],
    "name": "Transfer",
    "type": "event",
}


def get_aerodrome_nft_transfers(
    web3: Web3,
    contract_address: str,
    to_address: str,
    from_block: int = "latest",
    to_block: int = "latest",
) -> List[Dict]:
    """
    Get NFT Transfer events from Aerodrome contract filtered by recipient address.
    Uses the contract events API for better handling of large result sets.

    Args:
        web3: Web3 instance connected to a provider
        contract_address: The Aerodrome NFT contract address
        to_address: Filter by recipient address (required)
        from_block: Starting block number or 'latest'
        to_block: Ending block number or 'latest'

    Returns:
        List of transfer event dictionaries
    """

    print(contract_address, from_block, to_block)

    # Create contract instance with just the Transfer event ABI
    contract = web3.eth.contract(
        address=Web3.to_checksum_address(contract_address), abi=[TRANSFER_EVENT_ABI]
    )

    # Create event filter for Transfer events to the specified address
    event_filter = contract.events.Transfer.create_filter(
        fromBlock=from_block,
        toBlock=to_block,
        argument_filters={"to": Web3.to_checksum_address(to_address)},
    )


    # Get all matching events
    events = event_filter.get_all_entries()

    # Parse events into a clean format
    transfers = []
    for event in events:
        transfer = {
            "from": event["args"]["from"],
            "to": event["args"]["to"],
            "tokenId": event["args"]["tokenId"],
            "blockNumber": event["blockNumber"],
            "transactionHash": event["transactionHash"].hex(),
            "logIndex": event["logIndex"],
            "address": event["address"],
        }
        transfers.append(transfer)

    return transfers


def get_aerodrome_nft_transfers_batch(
    web3: Web3,
    contract_address: str,
    to_address: str,
    from_block: int,
    to_block: int,
    batch_size: int = 5,
) -> List[Dict]:
    """
    Get NFT Transfer events in batches to avoid RPC limits.

    Args:
        web3: Web3 instance connected to a provider
        contract_address: The Aerodrome NFT contract address
        to_address: Filter by recipient address (required)
        from_block: Starting block number
        to_block: Ending block number
        batch_size: Number of blocks per batch

    Returns:
        List of transfer event dictionaries
    """
    all_transfers = []
    current_block = from_block

    while current_block <= to_block:
        batch_end = min(current_block + batch_size - 1, to_block)

        try:
            transfers = get_aerodrome_nft_transfers(
                web3=web3,
                contract_address=contract_address,
                to_address=to_address,
                from_block=current_block,
                to_block=batch_end,
            )
            all_transfers.extend(transfers)
            print(
                f"Fetched {len(transfers)} transfers from blocks {current_block} to {batch_end}"
            )
        except Exception as e:
            print(f"Error fetching blocks {current_block} to {batch_end}: {e}")
            # Optionally reduce batch size and retry

        current_block = batch_end + 1

    return all_transfers


# Example usage
if __name__ == "__main__":
    import os
    import dotenv

    dotenv.load_dotenv()
    # Initialize Web3 (replace with your provider)
    w3 = get_w3()

    to_address = os.getenv("AERO_MANAGER_ADDRESS")

    # Uniswap V3 NonfungiblePositionManager contract address
    UNISWAP_V3_NFT_MANAGER = POS_MANAGER_ADDR

    # Define block range (example: last 1000 blocks from a recent block)
    mid_block = 33898677
    latest_block = mid_block + 50
    to_block = latest_block
    from_block = to_block - 100

    print(f"Searching for mint events from block {from_block} to {to_block}")

    # Get mint events
    mint_events = get_aerodrome_nft_transfers_batch(
        w3,
        UNISWAP_V3_NFT_MANAGER,
        to_address,
        from_block,
        to_block,
    )

    # Print results
    for event in mint_events[:5]:  # Show first 5 events
        print(json.dumps(event, indent=2))
        print("-" * 50)
