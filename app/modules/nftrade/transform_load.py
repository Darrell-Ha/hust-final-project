from schemas.nft_marketplace.nft_transform_load import NftTransformer
from modules.subscan.daily.models import NetworkDailyData
from .models import NftradeData

import datetime
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


def translate_type(type_rec: str):
    dict_translate = {
        "BOUGHT": "bought",
        "LISTED": "listing",
        "LIST_CANCELLED": "list_cancelled",
        "OFFER": "offer",
        "OFFER_CANCELLED": "offer_cancelled",
        "SOLD": "sale",
        "TRADED": "transfer",
        "TRADE_CANCELLED": "transfer_cancelled",
        "TRADE_OFFERED": "transfer_offered",
    }
    return dict_translate[type_rec]

class NftradeTransformer(NftTransformer):


    def transform(self):
        data_raw = [inst.__dict__.get("__data__") for inst in NftradeData.select()]
        # df = pd.DataFrame
        if data_raw != []:
            df = pd.DataFrame(data_raw)
            df['network'] = "moonbeam"
            del df['chainId']
            df['typeRec'] = df['typeRec'].apply(translate_type)
            df['price'] = df['price'].astype(float)
            df['time_utc'] = df['createdAt'].dt.strftime("%Y-%m-%d")
            start_date = df['time_utc'].min()
            end_date = df['time_utc'].max()
            df_price = pd.DataFrame(get_data_price(start_date=start_date, end_date=end_date))
            df_price['time_utc'] = df_price['time_utc'].dt.strftime("%Y-%m-%d")
            merged_info = df.merge(df_price, on=['network', 'time_utc'], how='left')
            merged_info['usd_value'] = merged_info['unit_usd'] * merged_info['price']
            records = json.loads(merged_info.to_json(orient="records"))
            transformed_record = []
            for record in records:
                transformed_record.append(
                    {
                        "timestamp": record.get("createdAt", 0),
                        "contract_address": record.get("contractAddress"),
                        "contract_name": record.get("contractName"),
                        "buyer_address": record.get("toUser"),
                        "seller_address": record.get("fromUser"),
                        "chain_slug": record.get("network"),
                        "type_activities": record.get("typeRec"),
                        "token_id": record.get("tokenId"),
                        "value": record.get("price"),
                        "unit": record.get("unit"),
                        "unit_usd": record.get("unit_usd"),
                        "usd_value": record.get("usd_value"),
                        "source_record": "nftrade",
                        "link": record.get("tokenImage"),
                        "tx": record.get("tx")
                    }
                )
            
        return transformed_record