
from prefect import task, flow
from utils import get_bq_client, get_gcs_client, BASE_DIR, get_project_id
import pandas as pd

ELECTRICITY_GENERATION_PATH = BASE_DIR / "data/electricity_generation.parquet"
ELECTRICITY_CONSUMPTION_PATH = BASE_DIR /"data/electricity_consumption.parquet"

@task()
def download_datasets_from_gcs():
    """Load data from Google Cloud Storage bucket"""
    
    gcs_data_paths = [
        ELECTRICITY_GENERATION_PATH,
        ELECTRICITY_CONSUMPTION_PATH
    ]
    
    bucket = get_gcs_client().bucket("swiss_energy")
    for blob in bucket.list_blobs():
        if "parquet" in blob.name:
            print("HERE "+ str(blob.name))
    
    for path in gcs_data_paths:
        blob = bucket.get_blob(path)
        
        local_path = BASE_DIR / path
        blob.download_to_filename(local_path)
        yield local_path

@task()
def transform() -> pd.DataFrame:
    """Clean datasets by joining data to one file, translating to english"""

    electricity_generation_df = pd.read_parquet(ELECTRICITY_GENERATION_PATH)
    electricity_consumption_df = pd.read_parquet(ELECTRICITY_CONSUMPTION_PATH)
    
    # Columns translation
    electricity_generation_df = electricity_generation_df.rename(columns={
        "datum": "date",
        "energietraeger": "energy_source", # Type of energy source (e.g., hydro, nuclear, solar)
        "produktion_gwh": "production_gwh", # Total electricity produced in Switzerland in GWh
    })
    
    electricity_consumption_df = electricity_consumption_df.rename(columns={
        "datum": "date",
        "landesverbrauch_gwh": "national_consumption_gwh", # Total electricity consumed in Switzerland
        "endverbrauch_gwh": "final_consumption_gwh" # Electricity actually delivered to end users
    })
    
    # Values translation
    electricity_generation_df["energy_source"] = electricity_generation_df["energy_source"].map({
        "Wasser": "Hydro",
        "Kernkraft": "Nuclear",
        "Sonne": "Solar",
        "Wind": "Wind",
        "Thermische": "Thermal",
        "Sonstige": "Other"
    })
    
    joined_df = electricity_generation_df.merge(
        electricity_consumption_df,
        on="date",
        how="outer"
    )
    
    return joined_df

@task()
def upload_to_bq(df: pd.DataFrame):
    """Upload datasets to BigQuery"""
  
    bq_client = get_bq_client()
    
    print("HAHA " + str(get_project_id()))
    job = bq_client.load_table_from_dataframe(df, f"{get_project_id()}.swiss_energy.electricity_data")
    job.result()

def remove_local_cash():
    """Remove local cache of datasets"""
    
    gcs_data_paths = [
        ELECTRICITY_GENERATION_PATH,
        ELECTRICITY_CONSUMPTION_PATH
    ]
    
    for path in gcs_data_paths:
        if path.exists():
            path.unlink()

@flow(name="gcs_to_bq_pipeline")
def gcs_to_bq_pipeline():
    """Main pipeline to stream datasets from GCS"""
    
    download_datasets_from_gcs()
    transformed_df = transform()
    upload_to_bq(transformed_df)
        

if __name__ == "__main__":    
    gcs_to_bq_pipeline()
