from core.base_extractor import BaseExtractor
from core.core import get_logger
from .models import MoonbeansTradeData, MoonbeansCollectionData
from modules.subscan.api import getinfo

import datetime
import traceback
import json
import requests


def standard_address(ca: str):
    return ca.lower()

def extract_more_in_subscan(record: dict):
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
              start_time = datetime.datetime.now() - datetime.timedelta(days=1),
              end_time = datetime.datetime.now()):
            #   start_time = datetime.datetime(2021, 10, 1),
            #   end_time = datetime.datetime(2021, 12, 1)):
        link = "https://graphql.moonbeans.io/graphql"
        header = {
            "Accept": "*/*",
            "Content-Type": "application/json"
        }
        payload = "{\"query\":\"query ALL_TRADES {   allFills(first: 1000, orderBy: TIMESTAMP_DESC) {     nodes {       type       buyer       id       timestamp       value       collectionId       __typename     }     __typename   } }\",\"variables\":\"{}\"}"
        # payload = "{\"query\":\"query ALL_TRADES {   allFills(orderBy: TIMESTAMP_DESC) {     nodes {       type       buyer       id       timestamp       value       collectionId       __typename     }     __typename   } }\",\"variables\":\"{}\"}"
        r = requests.post(link, data=payload,  headers=header)
        raw_res = r.json().get('data', {}).get('allFills', {}).get('nodes', [])
        fetch_data = []
        for record in raw_res:
            record_time = datetime.datetime.fromtimestamp(int(record.get("timestamp")))
            if record_time <= end_time and record_time >= start_time:
                more_info = extract_more_in_subscan(record)
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
                        "collectionId": standard_address(record.get("collectionId", "")),
                        "tokenId": int(record.get("id", "").split("-")[1]),
                        "buyer": standard_address(record.get("buyer", "")),
                        "seller": standard_address(record.get("seller", "")),
                        "value": int(record.get("value", 0))/1e18,
                        "timestamp": record.get("timestamp").strftime('%Y-%m-%d %H:%M:%S.%f'),
                        "tx": record.get("tx")
                    }
                )
            except Exception as e:
                print(record.get("tx"))
                get_logger().error(e)
                traceback.print_exc()
        return extracted_data
    
    def init_table(self):
        ## Create table if not exists
        try:
            if not MoonbeansTradeData.table_exists():
                MoonbeansTradeData.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def insert(self, record: dict):
        get_logger().debug("insert " + json.dumps(record))
        try:
            MoonbeansTradeData.create(
                tid=record.get("tid"),
                ttype=record.get("type"),
                collectionId=record.get("collectionId"),
                tokenId=record.get("tokenId"),
                buyer=record.get("buyer"),
                seller=record.get("seller"),
                value=record.get("value"),
                tx=record.get("tx"),
                timestamp=record.get("timestamp")
            )
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def update(self, record: dict):
        get_logger().debug("update " + json.dumps(record))
        tid = record.get("tid")
        try:
            update_query = MoonbeansTradeData\
                .update(record)\
                .where(MoonbeansTradeData.tid == tid)
            update_query.execute()
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def exist(self, record: dict) -> bool:
        record_exists = False
        tid = record.get("tid", None)
        if tid:
            record_exists = MoonbeansTradeData\
                .select("tid")\
                .where(MoonbeansTradeData.tid == tid)\
                .exists()
        else:
            raise ValueError("tid is empty!!")
        return record_exists


