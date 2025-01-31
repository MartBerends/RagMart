import requests
import pandas as pd
import logging
from google.cloud import bigquery

# Google Cloud Configuration
PROJECT_ID = "corded-forge-417909"  # Your Google Cloud Project ID
BQ_DATASET_ID = "ProjectRAGMart"  # Your BigQuery Dataset
BQ_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET_ID}.documents"  # BigQuery Table

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize BigQuery client
bq_client = bigquery.Client()

BASE_URL = "https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0/"

def fetch_data(entity, top=100, skip=0, expand=None):
    """Fetch data from the overheid API."""
    url = f"{BASE_URL}{entity}?$top={top}&$skip={skip}"
    if expand:
        url += f"&$expand={expand}"
    
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Error fetching {entity}: {response.status_code}")
        return None

def get_total_count(entity):
    """Retrieve total document count from the overheid API."""
    url = f"{BASE_URL}{entity}/$count"
    response = requests.get(url)
    if response.status_code == 200:
        return int(response.text)
    else:
        logging.error(f"Error fetching count for {entity}: {response.status_code}")
        return None

def get_existing_document_ids():
    """Retrieve already processed document IDs from BigQuery."""
    query = f"SELECT Id FROM `{BQ_TABLE_ID}`"
    query_job = bq_client.query(query)
    return {row["Id"] for row in query_job.result()}  # Return a set of IDs

def gather_data(entity, expand=None, save_every=500):
    """Fetch new documents and store them in BigQuery."""
    existing_docs = get_existing_document_ids()
    total_count = get_total_count(entity)
    
    if total_count is None:
        return "Error retrieving total count."

    logging.info(f"Total records to fetch for {entity}: {total_count}")

    data = []
    skip = len(existing_docs)  # Start from where we left off

    while skip < total_count:
        logging.info(f"Fetching {entity} records starting at offset {skip}... / {total_count}")
        result = fetch_data(entity, expand=expand, skip=skip)

        if not result or "value" not in result or not result["value"]:
            break

        new_records = [
            row for row in result["value"] if row["Id"] not in existing_docs
        ]
        data.extend(new_records)
        skip += 100

        # Save in batches
        if len(data) >= save_every:
            upload_to_bigquery(data)
            data = []

    # Final batch upload
    if data:
        upload_to_bigquery(data)

    return f"Fetched and stored {total_count - len(existing_docs)} new records."

def upload_to_bigquery(data):
    """Upload document metadata to BigQuery."""
    df = pd.DataFrame(data)
    job = bq_client.load_table_from_dataframe(df, BQ_TABLE_ID)
    job.result()  # Wait for job completion
    logging.info(f"Uploaded {len(df)} documents to BigQuery.")

def fetch_and_store_documents(request):
    """Cloud Function entry point."""
    return gather_data("Document", save_every=5000)
