from core.base_extractor import BaseExtractor
from core.core import get_logger
from .models import SantimentPriceChain
from .templates import PRICE

import peewee
import traceback
import requests
import pandas as pd
import json
import datetime

FORMAT_DATETIME = "%Y-%m-%dT%H:%M:%S.%fZ"


class SantimentExtractor(BaseExtractor):

    def __init__(self):
        self.network_slug = {
            "moonbeam": "moonbeam",
            "astar": "astar",
            "moonriver": "moonriver",
            "shiden": "shiden-network"
        }

    def fetch(self, 
              start_time: datetime.datetime=datetime.datetime.now()-datetime.timedelta(days=3),  
              end_time: datetime.datetime=datetime.datetime.now()):
            #   start_time: datetime.datetime = datetime.datetime(2022, 10, 1),
            #   end_time: datetime.datetime = datetime.datetime(2023, 10, 1)):
        result = []
        query = PRICE

        link = "https://api.santiment.net/graphql"
        for chain in self.network_slug.keys():
            ins_query = query.substitute(
                slug=self.network_slug[chain],
                start_time=start_time.strftime(FORMAT_DATETIME),
                end_time=end_time.strftime(FORMAT_DATETIME),
                interval="1h"
            )
            payload = {
                "query": ins_query
            }
            r = requests.post(link, json=payload)\
                .json()\
                .get('data', {})\
                .get('historyPrice', [])
            for record in r:
                record.update({"chain": chain})
            result.extend(r)

        return result

    def extract(self):
        fetched_data = self.fetch()
        get_logger().debug(f"fetched {len(fetched_data)} records")
        if fetched_data:
            
            data_price = pd.DataFrame(fetched_data)
            
            data_price = data_price.rename(columns={
                'datetime': 'time',
                'marketcapUsd': 'marketcap_usd',
                'priceUsd': 'price_usd',
                'volumeUsd': 'volume_usd'
            })
            result = json.loads(data_price.to_json(orient='records'))
            get_logger().debug(f"extracted {len(result)} records")
            return result
        else:
            raise ValueError("Empty Response")
    
    def init_table(self):
        ## Create table if not exists
        try:
            if not SantimentPriceChain.table_exists():
                SantimentPriceChain.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def bulk_upsert(self, records):
        get_logger().debug(f"upsert {len(records)} records")
        try:
            query = SantimentPriceChain.insert_many(records)
            query = query.on_conflict(conflict_target=[SantimentPriceChain.chain, SantimentPriceChain.time], update={
                "marketcap_usd": peewee.EXCLUDED.marketcap_usd,
                "price_usd": peewee.EXCLUDED.price_usd,
                "volume_usd": peewee.EXCLUDED.volume_usd,
            })
            query.execute()
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

