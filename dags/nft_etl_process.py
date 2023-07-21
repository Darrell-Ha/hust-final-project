from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#nft
from modules.moonbeans_io.extract_trade_data import MoonbeansTradeDataExtractor
from modules.moonbeans_io.transform_load import MoonbeansTransformer

from modules.moonbeans_io.extract_collection_data import NftCollectionDataExtractor

from modules.nftrade.extract_trade_data import NftradeDataExtractor
from modules.nftrade.transform_load import NftradeTransformer

from modules.tofunft.extract_trade_data import TofuNftDataExtractor
from modules.tofunft.transform_load import TofunftTransformer



def extract_moonbeans_io():
    extractor = MoonbeansTradeDataExtractor()
    return extractor.process()

def transform_load_moonbeans_io():
    transformer = MoonbeansTransformer()
    return transformer.process()


def extract_moonbeans_io_collection():
    extractor = NftCollectionDataExtractor()
    return extractor.process()


def extract_nftrade():
    extractor = NftradeDataExtractor()
    return extractor.process()

def transform_load_nftrade():
    transformer = NftradeTransformer()
    return transformer.process()


def extract_tofunft():
    extractor = TofuNftDataExtractor()
    return extractor.process()

def transform_load_tofunft():
    transformer = TofunftTransformer()
    return transformer.process()




args = {
    'owner': 'hadat'
}
with DAG(
        dag_id = 'NFT_pipeline',
        default_args = args,
        schedule_interval = "0 2 * * *",
        # schedule_interval = None,
        tags = ['final_thesis'],
        start_date = (datetime.today() - timedelta(days = 1)),
        concurrency = 2,
        max_active_runs = 4,
        params = {},
        catchup=False
) as dag:
    
    ## Extract
    extract_moonbeans_io = PythonOperator(
        task_id="extract_moonbeans_io",
        python_callable=extract_moonbeans_io
    )
    extract_moonbeans_io_collection = PythonOperator(
        task_id="extract_moonbeans_io_collection",
        python_callable=extract_moonbeans_io_collection
    )
    transform_load_moonbeans_io = PythonOperator(
        task_id="transform_load_moonbeans_io",
        python_callable=transform_load_moonbeans_io
    )


    extract_nftrade = PythonOperator(
        task_id="extract_nftrade",
        python_callable=extract_nftrade
    )
    transform_load_nftrade = PythonOperator(
        task_id="transform_load_nftrade",
        python_callable=transform_load_nftrade
    )
    

    extract_tofunft = PythonOperator(
        task_id="extract_tofunft",
        python_callable=extract_tofunft
    )
    transform_load_tofunft = PythonOperator(
        task_id="transform_load_tofunft",
        python_callable=transform_load_tofunft
    )


    # flow

    extract_moonbeans_io_collection >> extract_moonbeans_io >> transform_load_moonbeans_io
    extract_nftrade >> transform_load_nftrade
    extract_tofunft >> transform_load_tofunft
