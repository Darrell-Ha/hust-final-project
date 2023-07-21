from core.base_extractor import BaseExtractor
from core.core import get_logger
from .models import MoonbeansCollectionData

import traceback
import json
import requests
import datetime


def get_current_timestamp_utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')

class NftCollectionDataExtractor(BaseExtractor):

    def fetch(self):
        link = "https://api.moonbeans.io/collection"
        header = {
            "Accept": "*/*"
        }
        r = requests.get(link,  headers=header)
        
        return r.json()

    def extract(self):
        fetch_data = self.fetch()
        collection_data = []
        for record in fetch_data:
            collection_data.append(
                {
                    "contractAddress": record.get("contractAddress", "").lower(),
                    "links": json.dumps(record.get("links", {})),
                    "title": record.get("title"),
                    "headerSubtitle": record.get("headerSubtitle"),
                    "fullDescription": record.get("fullDescription"),
                    "startBlock": record.get("startBlock"),
                    "owner": record.get("owner"),
                    "status": record.get("status"),
                    "chain": record.get("chain"),
                    "enableMetaverse": record.get("enableMetaverse"),
                    "enableRarity": record.get("enableRarity"),
                    "enableBreeding": record.get("enableBreeding"),
                    "enableMint": record.get("enableMint"),
                    "enableAttributes": record.get("enableAttributes"),
                    "convertIPFS": record.get("convertIPFS"),
                    "maxSupply": record.get("maxSupply"),
                    "totalSupply": record.get("totalSupply"),
                    "mintCostText": record.get("mintCostText"),
                    "mintBeganText": record.get("mintBeganText")
                }
            )
        
        return collection_data

    def init_table(self):
        ## Create table if not exists
        try:
            if not MoonbeansCollectionData.table_exists():
                MoonbeansCollectionData.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def insert(self, record: dict):
        get_logger().debug("insert " + json.dumps(record))
        try:
            created_time = get_current_timestamp_utc()
            MoonbeansCollectionData.create(
                contractAddress=record.get("contractAddress"),
                links=record.get("links"),
                title=record.get("title"),
                headerSubtitle=record.get("headerSubtitle"),
                fullDescription=record.get("fullDescription"),
                startBlock=record.get("startBlock"),
                owner=record.get("owner"),
                status=record.get("status"),
                chain=record.get("chain"),
                enableMetaverse=record.get("enableMetaverse"),
                enableRarity=record.get("enableRarity"),
                enableBreeding=record.get("enableBreeding"),
                enableMint=record.get("enableMint"),
                enableAttributes=record.get("enableAttributes"),
                convertIPFS=record.get("convertIPFS"),
                maxSupply=record.get("maxSupply"),
                totalSupply=record.get("totalSupply"),
                mintCostText=record.get("mintCostText"),
                mintBeganText=record.get("mintBeganText"),
                is_archived=False,
                created_time=created_time,
                updated_time=created_time
            )
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def update(self, record: dict):
        get_logger().debug("update " + json.dumps(record))
        comming_address = record.get("contractAddress")
        record.update(
            {"updated_time": get_current_timestamp_utc()})
        try:
            update_query = MoonbeansCollectionData\
                .update(record)\
                .where(MoonbeansCollectionData.contractAddress == comming_address)
            update_query.execute()
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()


    def exist(self, record: dict) -> bool:
        record_exists = False
        comming_address = record.get("contractAddress", None)
        if comming_address:
            record_exists = MoonbeansCollectionData\
                .select("contractAddress")\
                .where(MoonbeansCollectionData.contractAddress == comming_address)\
                .exists()
        else:
            raise ValueError("contractAddress is empty!!")
        return record_exists
