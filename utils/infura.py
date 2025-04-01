
import os
import json
from dotenv import load_dotenv
from web3 import Web3
import logging

load_dotenv()

print("📄 Запущен скрипт: infura.py")
logging.info("📄 Запущен скрипт: infura.py")


INFURA_URL = os.getenv("INFURA_URL")
w3 = Web3(Web3.HTTPProvider(INFURA_URL))


def get_eth_balance(address):
    checksum_address = Web3.to_checksum_address(address)
    balance_wei = w3.eth.get_balance(checksum_address)
    balance_eth = w3.from_wei(balance_wei, "ether")
    return float(balance_eth)


def get_recent_transfers(token_contract_address, blocks_back=1000):
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(token_contract_address),
        abi=[
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "name": "from", "type": "address"},
                    {"indexed": True, "name": "to", "type": "address"},
                    {"indexed": False, "name": "value", "type": "uint256"},
                ],
                "name": "Transfer",
                "type": "event",
            }
        ],
    )

    current_block = w3.eth.block_number

    events = contract.events.Transfer().create_filter(
        from_block=current_block - blocks_back,
        to_block="latest"
    ).get_all_entries()

    return events
