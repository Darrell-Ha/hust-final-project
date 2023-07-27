from schemas.nft_marketplace.nft_transform_load import NftTransformer
from .models import NftradeData
from core.core import get_logger
import pandas as pd


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

    def transform(self) -> pd.DataFrame:
        new_data = pd.DataFrame(
            NftradeData\
                .select()\
                .where(
                    NftradeData.createdAt.between(self.START_TIME, self.END_TIME)
                )\
                .dicts()
        )
        data_price = self.get_data_price(chains=["moonbeam"])
        new_data['time_price_mapping'] = new_data['createdAt'].dt.strftime(self.FORMAT_TIME_MAPPING)
        process_data = new_data.merge(data_price, on=['time_price_mapping'], how='left')
        process_data['usd_value'] = process_data['unit_usd'] * process_data['price']
        process_data['typeRec'] = process_data['typeRec'].apply(translate_type)
        process_data['source_record'] = 'nftrade'
        
        # Fill Null value
        for field in {"owner_name", "max_supply", "total_supply", "link",
                      "descriptions", "owner_address", "buyer_name", "seller_name", "tx"}:
            process_data[field] = None

        # Extract
        process_data = process_data.rename(
            columns={
                "createdAt": "date_id",
                "contractAddress": "contract_address",
                "contractName": "contract_name",
                "toUser": "buyer_address",
                "fromUser": "seller_address",
                "chain": "chain_slug",
                "typeRec": "type_activities",
                "tokenId": "token_id",
                "price": "value",
                "unit_usd": "unit_usd",
                "usd_value": "usd_value",
            }
        )
        process_data = process_data[["date_id", "contract_address", "contract_name",\
                                     "owner_name", "max_supply", "total_supply",\
                                     "descriptions", "owner_address", "buyer_name", "seller_name", "tx",\
                                     "buyer_address", "seller_address", "chain_slug",\
                                     "type_activities", "token_id",  "value", "link",\
                                     "unit_usd", "usd_value", "source_record"]]
        
        get_logger().debug(f"transformed: {len(process_data)}")

        return process_data
