from schemas.nft_marketplace.nft_transform_load import NftTransformer
from .models import TofuTradeData
from core.core import get_logger
import pandas as pd


class TofunftTransformer(NftTransformer):
    
    def transform(self) -> pd.DataFrame:
        new_data = pd.DataFrame(
            TofuTradeData\
                .select()\
                .where(
                    TofuTradeData.created_at.between(self.START_TIME, self.END_TIME)
                )\
                .dicts()
        )
        data_price = self.get_data_price(chains=["shiden", "astar", "moonbeam"])
        data_price = data_price.rename(columns={"chain": "network"})
        new_data['time_price_mapping'] = new_data['created_at'].dt.strftime(self.FORMAT_TIME_MAPPING)
        process_data = new_data.merge(data_price, on=['network', 'time_price_mapping'], how='left')
        process_data['usd_value'] = process_data['unit_usd'] * process_data['price']
        process_data['source_record'] = 'tofunft'
        ## fill Null value
        for field in {"link", "max_supply", "total_supply", "owner_name", "owner_address", "descriptions"}:
            process_data[field] = None
        
        ## extract
        process_data = process_data.rename(
            columns={
                "created_at": "date_id",
                "nft_contract": "contract_address",
                # "nft_description": "descriptions",
                # "owner": "owner_address",
                "to_address": "buyer_address",
                "to_user_nickname": "buyer_name",
                "from_address": "seller_address",
                "from_user_nickname": "seller_name",
                "network": "chain_slug",
                "category": "type_activities",
                "price": "value"
            }
        )
        process_data = process_data[["date_id", "contract_address", "contract_name",\
                                     "link", "max_supply", "total_supply", "owner_name",\
                                     "descriptions", "owner_address", "buyer_address",\
                                     "buyer_name", "seller_address", "seller_name",\
                                     "chain_slug", "type_activities", "token_id",\
                                     "value", "unit_usd", "usd_value",\
                                     "source_record", "tx"]]
        
        get_logger().debug(f"transformed: {len(process_data)}")


        return process_data

