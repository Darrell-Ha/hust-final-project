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
        

        # Extract
        process_data = process_data.rename(
            columns={
                "createdAt": "date_id",
                "fromUser": "seller_address",
                "toUser": "buyer_address",
                "chain": "chain_slug",
                "contractAddress": "contract_address",
                "contractName": "contract_name",
                "tokenId": "token_id",
                "tokenName": "token_unique_name",
                "tokenImage": "token_images",
                "typeRec": "type_activities",
                "source_record": "source_record",
                "price": "value",
                "unit_usd": "unit_usd",
                "usd_value": "usd_value",
            }
        )
        process_data = process_data[["date_id","seller_address","buyer_address","chain_slug",\
                                     "contract_address","contract_name","token_id","token_unique_name",\
                                     "token_images","type_activities","source_record","value",\
                                     "unit_usd","usd_value"]]
        
        get_logger().debug(f"transformed: {len(process_data)}")

        return process_data
