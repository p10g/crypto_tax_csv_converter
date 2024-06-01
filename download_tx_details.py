import requests
from typing import Optional

base_url = "http://api.solana.fm/"
transfers_endpoint_url = "v0/transfers/"
wallet_address = "Dztd3HYBoiBHVLcSZwPfDUwaaCyohqhLGJ4YTy2funoM"
withdraw_transaction_hash = "5thVFibt1pDMRdMnPFYdbquUyXe3mx136YYQwbHcdnX6NXQw14Rr8W2YnanEwiPVVmLVDzZuJaGqwsXn2mHvAiY2"
harvest_transaction_hash = "f8QPHeEusirLAokFMWAvtZdKpUFX33yc672qq5zb3gpvmH1CLFkYYBfB19xMjKjLG2WCL1r88p65WCiGLGLfcBV"

ACTION_BURN_CHECKED = "burnChecked"

def get_burnt_nft_id(tx_id: str) -> Optional[str]:
    print("Getting details about tx: {}".format(tx_id))
    url = base_url + transfers_endpoint_url + tx_id
    response = requests.get(url)

    data = response.json()  # Can throw requests.exceptions.JSONDecodeError. Lets the caller handle it
    result = data.get('result', {})
    transactions = result.get('data', [])
    for tx in transactions:
        if ACTION_BURN_CHECKED == tx.get("action"):
            return tx.get("token")

    return None