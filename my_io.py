import json
import os

from typing import Dict, Optional

_FILE_PATH = "data/tx_to_nft_dict.json"

def save_tx_nft_dictionary_to_file(dict: Dict[str, Optional[str]]) -> None:
    with open(_FILE_PATH, "w") as file:
        json.dump(dict, file)

def load_tx_nft_dictionary_from_file() -> Dict[str, Optional[str]]:
    if not os.path.exists(_FILE_PATH):
        return {}
    
    with open(_FILE_PATH, "r") as file:
        data = json.load(file)
        return data