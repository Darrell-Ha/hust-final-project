from .dims.buyers import DimBuyers
from .dims.sellers import DimSellers
from .dims.date import DimDate
from .dims.collections import DimCollections
from .dims.chain import DimChain
from .facts.activities import FactActivities

# from .models import TradeData
from modules.subscan.daily.models import NetworkDailyData
from core.base_transform_load import BaseTransformLoad
from core.core import get_logger
from abc import abstractmethod
import pandas as pd
import json
import datetime
import traceback

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


class NftTransformer(BaseTransformLoad):

    def init_table(self):
        try:
            if not DimBuyers.table_exists():
                DimBuyers.create_table(safe=True)
            if not DimSellers.table_exists():
                DimSellers.create_table(safe=True)
            if not DimDate.table_exists():
                DimDate.create_table(safe=True)
            if not DimChain.table_exists():
                DimChain.create_table(safe=True)
            if not DimCollections.table_exists():
                DimCollections.create_table(safe=True)
            if not FactActivities.table_exists():
                FactActivities.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    @abstractmethod
    def transform(self):
        data_raw = [inst.__dict__.get("__data__") for inst in self.model.select()]
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

    def load(self, record: dict):
        self.load_to_date(record)
        self.load_to_seller(record)
        self.load_to_buyer(record)
        self.load_to_chain(record)
        self.load_to_collection(record)
        self.load_to_activities(record)
        pass

    def load_to_date(self, record: dict):
        record_time = record.get("timestamp")
        if not DimDate.select("date_id").where(DimDate.date_id == record_time).exists():
            get_logger().debug("load datetime " + str(record_time))
            date_processing = datetime.datetime.fromtimestamp(record_time / 1e3)
            DimDate.create(
                date_id=record_time,
                date_actual=date_processing.strftime("%Y-%m-%d %H:%M:%S.%f"),
                day_of_week=date_processing.weekday() + 2,
                day_of_month=date_processing.day,
                month=date_processing.month,
                quarter=(date_processing.month - 1) // 3 + 1,
                year=date_processing.year
            )
    
    def load_to_seller(self, record):
        seller_address = record.get("seller_address")
        name = record.get("seller_name")
        if seller_address:
            if not DimSellers.select("seller_address").where(DimSellers.seller_address == seller_address).exists():
                get_logger().debug("load seller: " + seller_address)
                DimSellers.create(
                    seller_address=seller_address,
                    name=name
                )
    
    def load_to_buyer(self, record):
        buyer_address = record.get("buyer_address")
        name = record.get("buyer_name")
        if buyer_address:
            if not DimBuyers.select("buyer_address").where(DimBuyers.buyer_address == buyer_address).exists():
                get_logger().debug("load buyer: " + buyer_address)
                DimBuyers.create(
                    buyer_address=buyer_address,
                    name=name
                )

    def load_to_chain(self, record):
        chain_slug = record.get("chain_slug")
        unit = record.get("unit")
        if chain_slug in {"moonbeam", "astar"}:
            relay_chain = "polkadot"
        elif chain_slug in {"shiden", "moonriver"}:
            relay_chain = "kusama"
        if DimChain.select("chain_slug").where(DimChain.chain_slug == chain_slug).exists():
            pass
        else:
            get_logger().debug("load chain: " + chain_slug)
            DimChain.create(
                chain_slug=chain_slug,
                relay_chain=relay_chain,
                unit=unit
            )
    
    @abstractmethod
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
    
    def load_to_activities(self, record):
        
        date_id = record.get("timestamp")
        contract_address = record.get("contract_address")
        buyer_address = record.get("buyer_address")
        seller_address = record.get("seller_address")
        chain_slug = record.get("chain_slug")
        
        if FactActivities\
            .select()\
            .where(
                FactActivities.date_id == date_id,
                FactActivities.contract_address == contract_address,
                FactActivities.buyer_address == buyer_address,
                FactActivities.seller_address == seller_address,
                FactActivities.chain_slug == chain_slug
            ).exists():
            pass
            # get_logger().debug("update activities: " + str(date_id))
            # update_query = FactActivities.update(
            #     {
            #         "date_id":date_id,
            #         "contract_address":contract_address,
            #         "buyer_address":buyer_address,
            #         "seller_address":seller_address,
            #         "chain_slug":chain_slug,
            #         "type_activities":record.get("type_activities"),
            #         "token_id":record.get("token_id"),
            #         "value":record.get("value"),
            #         "unit":record.get("unit"),
            #         "unit_usd":record.get("unit_usd"),
            #         "usd_value":record.get("usd_value"),
            #         "source_record":record.get("source_record"),
            #         "tx":record.get("tx")
            #     }
            # ).where(
            #     FactActivities.date_id == date_id,
            #     FactActivities.contract_address == contract_address,
            #     FactActivities.buyer_address == buyer_address,
            #     FactActivities.seller_address == seller_address,
            #     FactActivities.chain_slug == chain_slug
            # )
            # update_query.execute()
        
        else:
            get_logger().debug("load activities: " + str(date_id))
            FactActivities.create(
                date_id=date_id,
                contract_address=contract_address,
                buyer_address=buyer_address,
                seller_address=seller_address,
                chain_slug=chain_slug,
                type_activities=record.get("type_activities"),
                token_id=record.get("token_id"),
                value=record.get("value"),
                # unit=record.get("unit"),
                unit_usd=record.get("unit_usd"),
                usd_value=record.get("usd_value"),
                source_record=record.get("source_record"),
                tx=record.get("tx")
            )




