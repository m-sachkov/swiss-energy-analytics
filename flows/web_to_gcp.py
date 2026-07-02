
from prefect import flow, task
from io import BytesIO
from pathlib import Path
import pandas as pd
from google.cloud import storage
import requests
from prefect.blocks.system import Secret

BASE_DIR = Path(__file__).resolve().parent.parent
GCS_BUCKET_NAME = "swiss_energy"
GCS_CREDENTIALS_PATH = f"{BASE_DIR}/gcs_key.json"
    
    
@task(retries=3)
def fetch_dataset(url: str) -> pd.DataFrame:
    """Fetches a dataset from a given URL and returns it as a pandas DataFrame."""
    response = requests.get(url)
    response.raise_for_status()  

    data = response.json()
    df = pd.DataFrame(data['data'])
    
    return df

@task()
def upload_dtaset_to_gcs(dataset: bytes, dataset_name: str):
   """Upload dataset to Google Cloud Storage bucket"""
   
   secret_block = Secret.load("gcs-key")
   credentials_info = secret_block.get()
   
   client = storage.Client.from_service_account_info(credentials_info)
   bucket = client.bucket(GCS_BUCKET_NAME)
   blob = bucket.blob(f"data/{dataset_name}")

   blob.upload_from_string(
       dataset,
       content_type="application/x-parquet"
   )
    
def transfort_dataframe_to_parquet(df: pd.DataFrame) -> bytes:
    """Converts a pandas DataFrame to a Parquet string."""
    
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    return parquet_buffer.getvalue()

@flow(name="main_pipeline")
def main_pipeline():
    """Main pipeline to fetch datasets and upload to GCS"""
    
    dataset_metadata = {
        "electricity_generation": "https://energiedashboard.ch/api/v1/datasets/stromproduktion-swissgrid/data?offset=0",
        "electricity_consumption": "https://energiedashboard.ch/api/v1/datasets/stromverbrauch-swissgrid-lv-und-endv/data?offset=0"
    }
    
    for name, url in dataset_metadata.items():
        df = fetch_dataset(url)
        filename = name + ".parquet"
        upload_dtaset_to_gcs(transfort_dataframe_to_parquet(df), filename)

if __name__ == "__main__":    
    main_pipeline()