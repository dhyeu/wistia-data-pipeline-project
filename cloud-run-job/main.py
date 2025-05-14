import os
import requests
from google.cloud import bigquery
from google.oauth2 import service_account # Might not be needed if using ADC
import google.auth # Recommended for ADC in Cloud Run

# --- Configuration ---
# Get Wistia API Key from environment variable
# Make sure you configure this variable when deploying the Cloud Run Job
WISTIA_API_KEY = os.environ.get('0323ade64e13f79821bdc0f2a9410d9ec3873aa9df01f8a4a54d4e0f3dd2e6b4')

# BigQuery Configuration
# Get Project ID from environment (set by Cloud Run) or gcloud config
PROJECT_ID = os.environ.get('GCP_PROJECT', os.environ.get('CLOUD_PROJECT'))
BIGQUERY_DATASET = 'wistia_analytics' # <<< Your BigQuery Dataset Name
BIGQUERY_TABLE = 'media_stats'       # <<< Your BigQuery Table Name (e.g., for media stats)

# Wistia API Base URL
WISTIA_API_BASE_URL = 'https://api.wistia.com/v1'

# --- BigQuery Client Initialization ---
# Use default credentials (Application Default Credentials) when running on GCP
# This should work automatically in Cloud Run with the assigned Service Account
try:
    credentials, PROJECT_ID = google.auth.default()
    BIGQUERY_CLIENT = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    print(f"BigQuery client initialized for project: {PROJECT_ID}")
except google.auth.exceptions.DefaultCredentialsError as e:
    print(f"Could not automatically determine BigQuery credentials: {e}")
    print("Ensure you are running in a GCP environment or have GOOGLE_APPLICATION_CREDENTIALS set.")
    # You might need to exit or handle this case depending on your testing setup
    BIGQUERY_CLIENT = None # Or initialize with explicit credentials if testing locally

# --- Function to Fetch Data from Wistia API ---
def fetch_wistia_stats(api_key: str, endpoint: str):
    """Fetches data from a Wistia API endpoint."""
    if not api_key:
        print("Error: Wistia API Key is not set.")
        return []

    headers = {
        'Authorization': f'Bearer {api_key}',
        'User-Agent': 'Wistia Data Pipeline (CI/CD Test)'
    }

    all_data = []
    url = f'{WISTIA_API_BASE_URL}/{endpoint}.json' # Example endpoint, adjust as needed

    print(f"Fetching data from: {url}")

    # --- Implement Pagination Logic Here ---
    # Wistia API uses pagination. You need to handle 'page' and 'per_page' parameters
    # and loop through all pages until no more data is returned.
    # Example: initial_params = {'page': 1, 'per_page': 100}
    # Loop while the response contains data...

    try:
        response = requests.get(url, headers=headers) # Add params=initial_params
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        data = response.json()
        all_data.extend(data) # Extend with data from the current page

        print(f"Fetched {len(data)} records from this page.")

        # --- Pagination Loop ---
        # Add logic to check for next page and continue fetching
        # while next_page_exists:
        #     response = requests.get(next_page_url, headers=headers)
        #     ... fetch next page ...
        # ----------------------

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Wistia API: {e}")
        return None # Return None or handle error appropriately

    print(f"Finished fetching. Total records fetched: {len(all_data)}")
    return all_data # Return the list of dictionaries (JSON objects)

# --- Function to Load Data into BigQuery ---
def load_data_to_bigquery(client: bigquery.Client, dataset_id: str, table_id: str, data: list):
    """Loads a list of dictionaries into a BigQuery table."""
    if not client:
        print("BigQuery client is not initialized. Skipping load.")
        return

    if not data:
        print("No data to load into BigQuery.")
        return

    table_ref = client.dataset(dataset_id).table(table_id)

    # --- BigQuery Load Job Configuration ---
    job_config = bigquery.LoadJobConfig(
        # Specify the data format
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        # Configure write disposition (e.g., WRITE_TRUNCATE, WRITE_APPEND)
        # WRITE_APPEND is suitable for incremental loads.
        # WRITE_TRUNCATE is for full loads (overwriting the table). Be careful with this!
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # <<< Adjust as needed (APPEND vs TRUNCATE)
        # Optionally specify schema if you don't want auto-detection
        # schema=[
        #     bigquery.SchemaField("id", "STRING"),
        #     bigquery.SchemaField("name", "STRING"),
        #     # ... add other fields
        # ],
    )

    print(f"Loading {len(data)} rows into {dataset_id}.{table_id}")

    # Convert data (list of dicts) to newline-delimited JSON strings
    json_data = [os.linesep.join([json.dumps(row) for row in data])] # Need to import json

    try:
        # Make sure your BigQuery table exists before running the job
        # The client.create_table(table) command can be used if needed (handle exceptions if it exists)

        # Start the load job
        # client.load_table_from_json requires list of dicts directly, not newline-delimited strings
        # Let's use the standard load_table_from_json for simplicity with list of dicts
        # This method handles converting the list of dictionaries correctly internally
        job = client.load_table_from_json(data, table_ref, job_config=job_config)

        print(f"Starting load job: {job.job_id}")
        job.result() # Wait for the job to complete

        print(f"Load job completed: {job.job_id}")
        print(f"Rows loaded: {job.output_rows}")

    except Exception as e: # Catch broader exceptions during load
        print(f"Error during BigQuery load job: {e}")
        # You might want to log job errors more specifically or handle retries

# --- Main Execution Block ---
if __name__ == "__main__":
    print("Wistia Pipeline Job started.")

    # --- Fetch data ---
    # <<< ADJUST the endpoint based on the Wistia Stats API data you need (e.g., 'stats/media', 'stats/visits')
    wistia_data = fetch_wistia_stats(WISTIA_API_KEY, 'stats/media') # Example: fetching media stats

    if wistia_data is not None: # Check if fetching was successful
        # --- Load data into BigQuery ---
        # Ensure your BigQuery Dataset and Table exist beforehand
        # You can create them manually via the GCP Console or with a gcloud command
        # Example: gcloud bigquery datasets create wistia_analytics --project=YOUR_PROJECT_ID
        # Example: bq mk --table YOUR_PROJECT_ID:wistia_analytics.media_stats ./path/to/your_schema.json
        # Auto-schema detection often works for initial loads.

        if BIGQUERY_CLIENT: # Only attempt load if BigQuery client initialized
             load_data_to_bigquery(BIGQUERY_CLIENT, BIGQUERY_DATASET, BIGQUERY_TABLE, wistia_data)

    print("Wistia Pipeline Job finished.")