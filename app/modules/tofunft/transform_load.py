from schemas.nft_marketplace.nft_transform_load import NftTransformer
from modules.subscan.daily.models import NetworkDailyData
from .models import TradeData
import pandas as pd
import json
import datetime

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


class TofunftTransformer(NftTransformer):

    def transform(self):
        data_raw = [inst.__dict__.get("__data__") for inst in TradeData.select()]
        # df = pd.DataFrame
        if data_raw != []:
            df = pd.DataFrame(data_raw)
            df['time_utc'] = df['created_at'].dt.strftime("%Y-%m-%d")
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
                        "timestamp": record.get("created_at", 0),
                        "contract_address": record.get("nft_contract"),
                        "contract_name": record.get("contract_name"),
                        "descriptions": record.get("nft_description"),
                        "owner_address": record.get("owner"),
                        "buyer_address": record.get("to_address"),
                        "buyer_name": record.get("to_user_nickname"),
                        "seller_address": record.get("from_address"),
                        "seller_name": record.get("from_user_nickname"),
                        "chain_slug": record.get("network"),
                        "type_activities": record.get("category"),
                        "token_id": record.get("token_id"),
                        "value": record.get("price"),
                        "unit": record.get("unit"),
                        "unit_usd": record.get("unit_usd"),
                        "usd_value": record.get("usd_value"),
                        "source_record": "tofunft",
                        "tx": record.get("tx")
                    }
                )
            
        return transformed_record



