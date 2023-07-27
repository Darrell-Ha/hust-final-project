from core.base_extractor import BaseExtractor
from core.core import get_logger
from core.config import DATETIME_FORMATTER
from .models import MoonbeansCollectionData

import traceback
import json
import requests
import datetime
import peewee


def get_current_timestamp_utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime(DATETIME_FORMATTER)

class NftCollectionDataExtractor(BaseExtractor):

    def fetch(self):
        link = "https://api.moonbeans.io/collection"
        header = {
            "Accept": "*/*"
        }
        r = requests.get(link, headers=header)
        
        return r.json()

    def extract(self):
        fetch_data = self.fetch()
        created_time = get_current_timestamp_utc()
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
                    "mintBeganText": record.get("mintBeganText"),
                    "is_archived": False,
                    "created_time": created_time,
                    "updated_time": created_time
                }
            )
        get_logger().debug(f"extracted: {len(fetch_data)} records")
        return collection_data

    def init_table(self):
        ## Create table if not exists
        try:
            if not MoonbeansCollectionData.table_exists():
                MoonbeansCollectionData.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def bulk_upsert(self, records: list[dict]):
        try:
            query = MoonbeansCollectionData.insert_many(records)
            query = query.on_conflict(
                        conflict_target=[MoonbeansCollectionData.contractAddress, MoonbeansCollectionData.chain],
                        update={
                            "links": peewee.EXCLUDED.links,
                            "title": peewee.EXCLUDED.title,
                            "headerSubtitle": peewee.EXCLUDED.headerSubtitle,
                            "fullDescription": peewee.EXCLUDED.fullDescription,
                            "startBlock": peewee.EXCLUDED.startBlock,
                            "owner": peewee.EXCLUDED.owner,
                            "status": peewee.EXCLUDED.status,
                            "enableMetaverse": peewee.EXCLUDED.enableMetaverse,
                            "enableRarity": peewee.EXCLUDED.enableRarity,
                            "enableBreeding": peewee.EXCLUDED.enableBreeding,
                            "enableMint": peewee.EXCLUDED.enableMint,
                            "enableAttributes": peewee.EXCLUDED.enableAttributes,
                            "convertIPFS": peewee.EXCLUDED.convertIPFS,
                            "maxSupply": peewee.EXCLUDED.maxSupply,
                            "totalSupply": peewee.EXCLUDED.totalSupply,
                            "mintCostText": peewee.EXCLUDED.mintCostText,
                            "mintBeganText": peewee.EXCLUDED.mintBeganText,
                            "updated_time": peewee.EXCLUDED.updated_time
                        })
            query.execute()
            get_logger().debug(f"upsert: {len(records)} records")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

