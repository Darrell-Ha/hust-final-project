from core.base_extractor import BaseExtractor
from core.core import get_logger
from .models import NftradeData

import traceback
import json
import datetime
import requests


def convert_z_time(str_time: str) -> datetime.datetime:
    return datetime.datetime.strptime(str_time, '%Y-%m-%dT%H:%M:%S.%fZ') if str_time else ''


class NftradeDataExtractor(BaseExtractor):

    def fetch(self, 
              start_time: datetime.datetime = datetime.datetime.now() - datetime.timedelta(days=1), 
              end_time: datetime.datetime = datetime.datetime.now()):
        limit = 50
        link = f"https://api.nftrade.com/api/v1/activities?limit={limit}&skip=%d&chainId=1284"
        header = {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://nftrade.com/",
            "Host": "api.nftrade.com",
            "Connection": "Close"
        }
        data_fetch = []
        has_next = True
        for offset in range(0, limit * 100 + 1, limit):
            if has_next:
                r = requests.get(link % offset,  headers=header)
                for record in r.json():
                    createdAt = convert_z_time(record.get("createdAt", ''))
                    if createdAt <= end_time and createdAt >= start_time:
                        process_rec = {
                            "idTrade": record.get("id", None),
                            "chainId": record.get("chainId"),
                            "typeRec": record.get("type"),
                            "fromUser": record.get("from"),
                            "toUser": record.get("to"),
                            "createdAt": createdAt.strftime('%Y-%m-%d %H:%M:%S.%f'),
                            "price": record.get("price"),
                            "contractAddress": record.get("activityTokens", [{}])[0].get("contractAddress"),
                            "contractName": record.get("activityTokens", [{}])[0].get("contractName"),
                            "tokenId": record.get("activityTokens", [{}])[0].get("tokenID", None),
                            "tokenName": record.get("activityTokens", [{}])[0].get("tokenName", None),
                            "tokenImage": record.get("activityTokens", [{}])[0].get("tokenImage", None),
                            "side": record.get("activityTokens", [{}])[0].get("side")
                        }
                        data_fetch.append(process_rec)
                    else:
                        has_next = False
        print(data_fetch)
        return data_fetch

    def extract(self):
        return self.fetch()
    
    def init_table(self):
        ## Create table if not exists
        try:
            if not NftradeData.table_exists():
                NftradeData.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def insert(self, record: dict):
        get_logger().debug("insert " + json.dumps(record))
        try:
            NftradeData.create(
                idTrade=record.get("idTrade"),
                chainId=record.get("chainId"),
                typeRec=record.get("typeRec"),
                fromUser=record.get("fromUser"),
                toUser=record.get("toUser"),
                createdAt=record.get("createdAt"),
                price=record.get("price"),
                contractAddress=record.get("contractAddress"),
                contractName=record.get("contractName"),
                tokenId=record.get("tokenId"),
                tokenName=record.get("tokenName"),
                tokenImage=record.get("tokenImage"),
                side=record.get("side"),
            )
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def update(self, record: dict):
        get_logger().debug("update " + json.dumps(record))
        comming_id = record.get("idTrade")
        try:
            update_query = NftradeData\
                .update(record)\
                .where(NftradeData.idTrade == comming_id)
            update_query.execute()
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def exist(self, record: dict):
        record_exists = False
        comming_id = record.get("idTrade")
        if comming_id:
            record_exists = NftradeData\
                .select("idTrade")\
                .where(NftradeData.idTrade == comming_id)\
                .exists()
        else:
            raise ValueError("idTrade is empty!!")
        return record_exists
