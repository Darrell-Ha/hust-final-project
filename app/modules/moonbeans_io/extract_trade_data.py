from core.base_extractor import BaseExtractor
from core.core import get_logger
from core.config import DATETIME_FORMATTER
from .models import MoonbeansTradeData, MoonbeansCollectionData
from modules.subscan.api import getinfo

import datetime
import traceback
import requests
import peewee


def standard_address(ca: str):
    return ca.lower()

def extract_seller_from_subscan(record: dict):
    tid = record.get("id")
    contractAddress = standard_address(record.get("collectionId"))
    collection_record = (MoonbeansCollectionData\
                        .select(MoonbeansCollectionData.contractAddress, MoonbeansCollectionData.chain)\
                        .where(MoonbeansCollectionData.contractAddress == contractAddress))
    tx = tid.split("-")[2]
    chain = collection_record[0].chain
    try:
        r = getinfo(network=chain, endpoint="/evm/transaction", json_data={"hash": tx}, version=1)
        transfer_info = r.get("token_transfers", [{}])
        if transfer_info != []:
            return {
                "seller": transfer_info[0].get("from"),
                "tx": tx 
            }
        else:
            return {
                "tx": tx
            }
    except:
        print(tx)
    


class MoonbeansTradeDataExtractor(BaseExtractor):

    def fetch(self,
              start_time = datetime.datetime.now() - datetime.timedelta(days=7),
              end_time = datetime.datetime.now()):
            #   start_time = datetime.datetime(2021, 10, 1),
            #   end_time = datetime.datetime(2023, 10, 1)):
        link = "https://graphql.moonbeans.io/graphql"
        header = {
            "Accept": "*/*",
            "Content-Type": "application/json"
        }
        payload = """
            query ALL_TRADES{
                allFills(
                    first: 1000,
                    orderBy: TIMESTAMP_DESC
                ){
                    nodes{
                        id
                        buyer
                        timestamp
                        value
                        collectionId
                    }
                }
            }
        """
        r = requests.post(link, json={"query": payload,},  headers=header)
        raw_res = r.json().get('data', {}).get('allFills', {}).get('nodes', [])
        fetch_data = []
        for record in raw_res:
            record_time = datetime.datetime.fromtimestamp(int(record.get("timestamp")))
            if record_time <= end_time and record_time >= start_time:
                more_info = extract_seller_from_subscan(record)
                record.update(more_info)
                record.update({"timestamp": record_time})
                fetch_data.append(record)
        return fetch_data

    def extract(self):
        extracted_data = []
        fetch_data = self.fetch()
        for record in fetch_data:
            try:
                extracted_data.append(
                    {
                        "tid": record.get("id"),
                        "ttype": record.get("type"),
                        "contractAddress": standard_address(record.get("collectionId", "")),
                        "tokenId": int(record.get("id", "").split("-")[1]),
                        "buyer": standard_address(record.get("buyer", "")),
                        "seller": standard_address(record.get("seller", "")),
                        "value": int(record.get("value", 0))/1e18,
                        "timestamp": record.get("timestamp").strftime(DATETIME_FORMATTER),
                        "tx": record.get("tx")
                    }
                )
            except Exception as e:
                print(record.get("tx"))
                get_logger().error(e)
                traceback.print_exc()
        get_logger().debug(f"extracted: {len(fetch_data)} records")
        return extracted_data
    
    def init_table(self):
        ## Create table if not exists
        try:
            if not MoonbeansTradeData.table_exists():
                MoonbeansTradeData.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def bulk_upsert(self, records: list[dict]):
        try:
            query = MoonbeansTradeData.insert_many(records)
            query = query.on_conflict(
                        conflict_target=[MoonbeansTradeData.tid,],
                        update={
                            "ttype": peewee.EXCLUDED.ttype,
                            "contractAddress": peewee.EXCLUDED.contractAddress,
                            "tokenId": peewee.EXCLUDED.tokenId,
                            "buyer": peewee.EXCLUDED.buyer,
                            "seller": peewee.EXCLUDED.seller,
                            "tx": peewee.EXCLUDED.tx,
                            "value": peewee.EXCLUDED.value,
                            "timestamp": peewee.EXCLUDED.timestamp
                                        
                        })
            query.execute()
            get_logger().debug(f"upsert: {len(records)} records")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()


