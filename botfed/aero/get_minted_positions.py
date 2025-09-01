from .abis.pool_clmm import ABI as POOL_ABI

def get_minted_positions_by_address(w3, pool_ids, owner_address, from_block, to_block):
    position_ids = []
    owner_address = w3.to_checksum_address(owner_address)
    print(owner_address)
    block_spacing = 3 * 60 * 60 // 2
    for pool_id in pool_ids:
        print(pool_id)
        pool = w3.eth.contract(abi=POOL_ABI, address=pool_id)
        block = from_block
        while block < to_block:
            event_filter = pool.events.Mint.create_filter(
                fromBlock=block,
                toBlock=block + block_spacing,
                argument_filters={"owner": owner_address},
            )
            logs = event_filter.get_all_entries()
            if logs:
                print(logs)
            position_ids += [log["args"]["tokenId"] for log in logs]
            block += block_spacing
    return position_ids


if __name__ == "__main__":
    import dotenv
    import pandas as pd

    dotenv.load_dotenv()
    from .vars import watch_pools as pool_ids, get_w3

    w3 = get_w3()
    block = w3.eth.block_number
    from_block = block - int(30 * 24 * 60 * 60 / 2)