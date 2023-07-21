from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#nft

from modules.moonbeans_io.extract_collection_data import NftCollectionDataExtractor

def extract_moonbeans_io_collection():
    extractor = NftCollectionDataExtractor()
    return extractor.process()


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
    
    extract_moonbeans_io_collection = PythonOperator(
        task_id="extract_moonbeans_io_collection",
        python_callable=extract_moonbeans_io_collection
    )

    # flow

    extract_moonbeans_io_collection
