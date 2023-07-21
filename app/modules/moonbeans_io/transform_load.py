from schemas.nft_marketplace.nft_transform_load import NftTransformer
from schemas.nft_marketplace.dims.collections import DimCollections
from modules.subscan.daily.models import NetworkDailyData
from .models import MoonbeansCollectionData, MoonbeansTradeData

from core.core import get_logger
import datetime
import traceback
import pandas as pd
import json

def get_data_price(start_date: datetime.datetime=datetime.datetime.now()-datetime.timedelta(days=1), 
                    end_date: datetime.datetime=datetime.datetime.now()):
    return [inst.__dict__.get("__data__") for inst in NetworkDailyData\
            .select(
                NetworkDailyData.time_utc,
                NetworkDailyData.network, 
                NetworkDailyData.unit_usd,
                NetworkDailyData.unit
            ).where(
                NetworkDailyData.time_utc.between(start_date, end_date)
            )]


class MoonbeansTransformer(NftTransformer):

    def transform(self):
        data_trade = [inst.__dict__.get("__data__") for inst in MoonbeansTradeData.select()]
        data_collection = [inst.__dict__.get("__data__") for inst in MoonbeansCollectionData.select()]
        if data_trade != [] and data_collection != []:
            df_trade = pd.DataFrame(data_trade)
            df_trade = df_trade.rename(columns={'collectionId': 'contractAddress'})
            df_collection = pd.DataFrame(data_collection)
            df = df_trade.merge(df_collection, how="left", on='contractAddress')
            df = df.rename(columns={"chain": "network"})

            df['time_utc'] = df['timestamp'].dt.strftime("%Y-%m-%d")
            start_date = df['time_utc'].min()
            end_date = df['time_utc'].max()
            df_price = pd.DataFrame(get_data_price(start_date=start_date, end_date=end_date))
            df_price['time_utc'] = df_price['time_utc'].dt.strftime("%Y-%m-%d")
            merged_info = df.merge(df_price, on=['network', 'time_utc'], how='left')
            merged_info['usd_value'] = merged_info['unit_usd'] * merged_info['value']
            records = json.loads(merged_info.to_json(orient="records"))
            transformed_record = []
            for record in records:
                transformed_record.append(
                    {
                        "timestamp": record.get("timestamp", 0),
                        "contract_address": record.get("contractAddress"),
                        "contract_name": record.get("title"),
                        "owner_name": record.get("owner"),
                        "descriptions": record.get("fullDescription"),
                        "link": record.get("link"),
                        "max_supply": record.get("maxSupply"),
                        "total_supply": record.get("totalSupply"),
                        "buyer_address": record.get("buyer"),
                        "seller_address": record.get("seller"),
                        "chain_slug": record.get("network"),
                        "type_activities": "sale",
                        "token_id": record.get("tokenId"),
                        "value": record.get("value"),
                        "unit": record.get("unit"),
                        "unit_usd": record.get("unit_usd"),
                        "usd_value": record.get("usd_value"),
                        "source_record": "moonbeans.io",
                        "tx": record.get("tx")
                    }
                )
            
        return transformed_record
    

    def load_to_collection(self, record):
        contract_address = record.get("contract_address")
        contract_name = record.get("contract_name")
        descriptions = record.get("descriptions")
        owner_address = record.get("owner_address")
        owner_name = record.get("owner_name")
        link = record.get("link")
        max_supply = record.get("max_supply")
        total_supply = record.get("total_supply")

        if not DimCollections.select("contract_address").where(DimCollections.contract_address == contract_address).exists():
            get_logger().debug("load collection: " + contract_address)
            DimCollections.create(
                contract_address=contract_address,
                contract_name=contract_name,
                descriptions=descriptions,
                owner_address=owner_address,
                owner_name=owner_name,
                link=link,
                max_supply=max_supply,
                total_supply=total_supply
            )
        else: 
            get_logger().debug("update collection: " + contract_address)
            update_query = DimCollections.update(
                {
                    "owner_name":owner_name,
                    "link": link,
                    "max_supply":max_supply,
                    "total_supply":total_supply
                }
            ).where(
                DimCollections.contract_address == contract_address
            )
            update_query.execute()