

from pathlib import Path
from prefect.blocks.system import Secret
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from prefect_gcp import credentials


BASE_DIR = Path(__file__).parent


def get_project_id() -> str:
    """Get the Google Cloud project ID from Prefect Secret block."""
    
    secret_block = Secret.load("gcs-key")
    credentials_info = secret_block.get()
    return credentials_info.get("project_id")

def get_google_credentials() -> service_account.Credentials:
    """Get Google Cloud credentials from Prefect Secret block."""
    
    secret_block = Secret.load("gcs-key")
    credentials_info = secret_block.get()
    return service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],

    )

def get_gcs_client() -> storage.Client:
    """Get a Google Cloud Storage client using service account credentials."""
    
    credentials_info = get_google_credentials()
   
    return storage.Client(
        project=credentials_info.project_id,
        credentials=credentials_info,
    )
    
def get_bq_client() -> bigquery.Client:
    """Get a Google BigQuery client using service account credentials."""
    
    credentials_info = get_google_credentials()
    print("!!!Project:", credentials_info.project_id)

    print("!!!!ervice account:", credentials_info.service_account_email)
    return bigquery.Client(
        project=credentials_info.project_id,
        credentials=credentials_info,
    )