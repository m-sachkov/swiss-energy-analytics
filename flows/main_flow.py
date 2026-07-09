

from prefect import flow
from flows.gcs_to_bq_flow import gcs_to_bq_pipeline
from flows.web_to_gcp_flow import web_to_gcs_pipeline


@flow(name="main_pipeline")
def main_pipeline():
    """Main flow to orchestrate the data pipeline"""
    
    # Step 1: Fetch datasets from the web and upload to GCS
    web_to_gcs_pipeline()
    
    # Step 2: Download datasets from GCS and upload to BigQuery
    gcs_to_bq_pipeline()