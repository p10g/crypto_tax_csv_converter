import requests
from typing import Optional

base_url = "http://api.solana.fm/"
transfers_endpoint_url = "v0/transfers/"

ACTION_BURN_CHECKED = "burnChecked"

def get_burnt_nft_id(tx_id: str) -> Optional[str]:
    print("Getting details of tx: {}".format(tx_id))
    url = base_url + transfers_endpoint_url + tx_id
    response = requests.get(url)

    data = response.json()  # Can throw requests.exceptions.JSONDecodeError. Lets the caller handle it
    result = data.get('result', {})
    transactions = result.get('data', [])
    for tx in transactions:
        if ACTION_BURN_CHECKED == tx.get("action"):
            return tx.get("token")

    return None