# Complete ingest_wistia.py code

import requests
import os
import json
from typing import Optional, Dict, List, Any
import datetime
from google.cloud import bigquery # Keep imports, even if BQ creation moves later
from google.api_core.exceptions import Conflict # Keep imports

# --- Configuration ---
# Ensure your WISTIA_API_TOKEN is configured securely
WISTIA_API_TOKEN = os.environ.get("WISTIA_API_TOKEN", "YOUR_WISTIA_API_TOKEN_PLACEHOLDER")

# Google Cloud Project ID (might be used by BQ client if creation is here)
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "wistia-data-pipeline-project")

# BigQuery Dataset and Table IDs (might be used by BQ client if creation is here)
BIGQUERY_DATASET_ID = "wistia_analytics_dwh"
BIGQUERY_TABLE_IDS = {
    "dim_media": "dim_media",
    "dim_visitor": "dim_visitor",
    "fact_media_engagement": "fact_media_engagement"
}

# The specific Media IDs you want to include in the DWH (used for fetching events)
MEDIA_HASHED_IDS = ["gskhw4w4lm", "v08dlrgr7v"] # <<< Specify your target media IDs here

# Wistia API Base URLs
STATS_BASE_URL = "https://api.wistia.com/v1/stats/medias/" # Still needed for get_media_stats if used
MEDIA_BASE_URL = "https://api.wistia.com/v1/medias/" # Still needed for fetch_media_metadata if used for single item
EVENTS_LIST_URL = "https://api.wistia.com/v1/stats/events.json"
MEDIA_LIST_URL = "https://api.wistia.com/v1/medias.json" # New: Media List Endpoint

# Raw data directory
RAW_DATA_DIR = "raw_data"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# --- BigQuery Schema Definitions ---
# These schemas are needed if this script is responsible for BigQuery setup
# Although we moved BQ setup to process_wistia_data.py, keeping them here
# provides a single source of truth for the schema if this script were to
# programmatically create tables before ingestion (an alternative structure).
# However, since process_wistia_data.py now handles BQ creation, these definitions
# are technically redundant *in this script* for the current pipeline structure.
# Let's include them for completeness or if the user wants to revert to this script
# doing BQ setup later.

# Define the schema for dim_media
dim_media_schema = [
    bigquery.SchemaField("media_id", "STRING", mode="REQUIRED"), # Hashed ID
    bigquery.SchemaField("wistia_id", "INTEGER", mode="NULLABLE"), # Internal ID
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("duration", "BIGNUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("project_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("project_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("channel", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"),
]
# Define the schema for dim_visitor
dim_visitor_schema = [
    bigquery.SchemaField("visitor_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ip_address", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("user_agent_browser", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("user_agent_platform", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("user_agent_mobile", "BOOL", mode="NULLABLE"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"),
]
# Define the schema for fact_media_engagement
fact_media_engagement_schema = [
    bigquery.SchemaField("media_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("visitor_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("percent_viewed", "BIGNUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("ip_address", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("play_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("play_rate", "BIGNUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("total_watch_time", "BIGNUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"),
]


# --- BigQuery Interaction Function (Table Creation - Optional in this script's main flow) ---
# This function is kept here in case you want this script to handle BQ setup,
# but currently process_wistia_data.py is responsible for it.
def create_bigquery_tables(project_id: str, dataset_id: str, table_ids: Dict[str, str]):
    """
    Creates BigQuery dataset and tables if they do not exist.
    """
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)

    try:
        client.create_dataset(dataset_ref)
        print(f"✅ Dataset '{dataset_id}' created in project '{project_id}'.")
    except Conflict:
        print(f"ℹ️ Dataset '{dataset_id}' already exists in project '{project_id}'.")
    except Exception as e:
        print(f"❌ Error creating dataset '{dataset_id}': {e}")
        return

    table_configs = {
        table_ids["dim_media"]: bigquery.Table(dataset_ref.table(table_ids["dim_media"]), schema=dim_media_schema),
        table_ids["dim_visitor"]: bigquery.Table(dataset_ref.table(table_ids["dim_visitor"]), schema=dim_visitor_schema),
        table_ids["fact_media_engagement"]: bigquery.Table(dataset_ref.table(table_ids["fact_media_engagement"]), schema=fact_media_engagement_schema),
    }

    for table_name, table in table_configs.items():
        try:
            client.create_table(table)
            print(f"✅ Table '{table_name}' created in dataset '{dataset_id}'.")
        except Conflict:
            print(f"ℹ️ Table '{table_name}' already exists in dataset '{dataset_id}'.")
        except Exception as e:
            print(f"❌ Error creating table '{table_name}': {e}")


# --- API Interaction Functions ---

# Function to fetch stats for a single media item (not used in main ingestion flow currently)
def get_media_stats(media_id: str, api_token: str) -> Optional[Dict]:
    """
    Fetches basic stats for a single media item.
    """
    url = f"{STATS_BASE_URL}{media_id}.json"
    headers = {"Authorization": f"Bearer {api_token}"}
    print(f"Attempting to fetch stats for media ID: {media_id}")
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        if response.status_code == 200:
            print(f"✅ Successfully retrieved stats for {media_id}")
            return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed for media ID {media_id}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"❌ Failed to decode JSON response for media ID {media_id}")
        return None
    except Exception as e:
        print(f"❌ An unexpected error occurred during stats fetch for {media_id}: {e}")
        return None

# Function to fetch metadata for a single media item (not used in main ingestion flow currently)
def fetch_media_metadata(media_hashed_id: str, api_token: str) -> Optional[Dict]:
    """
    Fetches metadata for a single media item using the Show endpoint.
    """
    url = f"{MEDIA_BASE_URL}{media_hashed_id}.json"
    headers = {"Authorization": f"Bearer {api_token}"}
    print(f"Attempting to fetch metadata for media hashed ID: {media_hashed_id}")
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        if response.status_code == 200:
            print(f"✅ Successfully retrieved metadata for {media_hashed_id}")
            metadata = response.json()
            # Returning the full metadata response here
            return metadata
        else:
             print(f"⚠️ Error: Received status code {response.status_code} for metadata {media_hashed_id} - {response.text}")
             return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Metadata request failed for media hashed ID {media_hashed_id}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"❌ Failed to decode JSON response for metadata")
        return None
    except Exception as e:
        print(f"❌ An unexpected error occurred during metadata fetch for {media_hashed_id}: {e}")
        return None


# Function to fetch ALL media metadata using the List endpoint with pagination
def fetch_all_media_metadata(api_token: str) -> List[Dict]:
    """
    Fetches metadata for ALL media using the list endpoint, handling pagination.
    """
    all_media_metadata = []
    page = 1
    per_page = 100 # Wistia default is 100

    print("--- Fetching All Media Metadata ---")

    while True:
        params = {
            "per_page": per_page,
            "page": page,
        }
        headers = {"Authorization": f"Bearer {api_token}"}

        print(f"  Fetching page {page} from Media List endpoint")

        try:
            response = requests.get(MEDIA_LIST_URL, headers=headers, params=params, timeout=60)
            response.raise_for_status()

            if response.status_code == 200:
                media_on_page = response.json()

                if not media_on_page:
                    print(f"  No media found on page {page} or reached end of pagination.")
                    break

                print(f"  ✅ Successfully retrieved {len(media_on_page)} media items from page {page}")
                all_media_metadata.extend(media_on_page)

                # Wistia List API pagination check: If the number of items on the page
                # is less than per_page, it's the last page.
                if len(media_on_page) < per_page:
                    print("  Reached the last page.")
                    break

                page += 1

            else:
                 print(f"⚠️ Error: Received status code {response.status_code} for Media List page {page} - {response.text}")
                 break # Stop pagination on error

        except requests.exceptions.Timeout:
             print(f"❌ Media List request timed out for page {page}.")
             break
        except requests.exceptions.RequestException as e:
            print(f"❌ Media List request failed for page {page}: {e}")
            break
        except json.JSONDecodeError:
            print(f"❌ Failed to decode JSON response for Media List page {page}")
            break
        except Exception as e:
            print(f"❌ An unexpected error occurred during Media List fetch for page {page}: {e}")
            break

    print(f"Finished fetching all media metadata. Total items fetched: {len(all_media_metadata)}")
    return all_media_metadata


# Function to fetch events for a given media hashed ID, handling pagination and date filtering
def fetch_media_events(media_hashed_id: str, api_token: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetches events for a given media hashed ID, handling pagination and date filtering.
    Accepts start_date and end_date parameters.
    """
    all_events = []
    page = 1
    per_page = 100

    print(f"\nAttempting to fetch events for media hashed ID: {media_hashed_id}")

    while True:
        params = {
            "media_id": media_hashed_id,
            "per_page": per_page,
            "page": page,
            "start_date": start_date,
            "end_date": end_date,
        }

        headers = {"Authorization": f"Bearer {api_token}"}

        print(f"  Fetching page {page} for {media_hashed_id} (Date range: {start_date or 'all'} to {end_date or 'all'})")

        try:
            response = requests.get(EVENTS_LIST_URL, headers=headers, params=params, timeout=60)
            response.raise_for_status()

            if response.status_code == 200:
                events_on_page = response.json()

                if not events_on_page:
                    print(f"  No events found on page {page} or reached end of pagination for {media_hashed_id}.")
                    break

                print(f"  ✅ Successfully retrieved {len(events_on_page)} events from page {page}")
                all_events.extend(events_on_page)
                page += 1 # Move to the next page

            else:
                 print(f"⚠️ Error: Received status code {response.status_code} for events {media_hashed_id} page {page} - {response.text}")
                 break # Stop pagination on error

        except requests.exceptions.Timeout:
             print(f"❌ Events request timed out for media hashed ID {media_hashed_id} page {page}.")
             break # Stop pagination on timeout
        except requests.exceptions.RequestException as e:
            print(f"❌ Events request failed for media hashed ID {media_hashed_id} page {page}: {e}")
            break # Stop pagination on other request errors
        except json.JSONDecodeError:
            print(f"❌ Failed to decode JSON response for events page {page}")
            break # Stop pagination on JSON error
        except Exception as e:
            print(f"❌ An unexpected error occurred during events fetch for {media_hashed_id} page {page}: {e}")
            break # Stop pagination on unexpected errors

    print(f"Finished fetching events for {media_hashed_id}. Total events fetched: {len(all_events)}")
    return all_events


# --- Main Ingestion Logic Function ---
# This function orchestrates the fetching and saving of raw data for one run
def run_wistia_ingestion(api_token: str, media_hashed_ids_for_events: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, str]:
    """
    Runs the Wistia data ingestion process, fetches metadata and events,
    and saves raw data to local files.
    Fetches ALL media metadata. Fetches events only for specified media_hashed_ids_for_events.
    Accepts date range for event fetching.
    Returns dictionary containing the run timestamp and paths to generated raw data files.
    """
    # Generate a run timestamp for organizing raw data files
    run_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\n--- Starting Wistia Data Ingestion Run: {run_timestamp} ---")
    print(f"Fetching data for date range: {start_date or 'all'} to {end_date or 'all'}")


    # --- Fetch Media Metadata (using the new List endpoint function) ---
    # We fetch all media metadata here to ensure we get duration if available via this endpoint
    all_media_metadata = fetch_all_media_metadata(api_token)

    # Save ALL Media Metadata fetched from the list endpoint
    metadata_filename = None
    if all_media_metadata:
        metadata_filename = os.path.join(RAW_DATA_DIR, f"all_media_metadata_{run_timestamp}.json") # New filename format reflects fetching ALL
        try:
            with open(metadata_filename, 'w') as f:
                json.dump(all_media_metadata, f, indent=2)
            print(f"✅ Saved all media metadata to {metadata_filename}")
        except Exception as e:
             print(f"❌ Error saving all media metadata to file {metadata_filename}: {e}")
             metadata_filename = None # Indicate failure to save
    else:
        print("No media metadata fetched to save.")


    # --- Fetch Media Events ---
    print("\n--- Fetching Media Events ---")
    event_filenames = {} # Dictionary to store paths to saved event files

    # Iterate through the specific media IDs you care about for event data
    for media_hashed_id in media_hashed_ids_for_events:
         # Call the fetch_media_events function for each specified media ID
         events_data = fetch_media_events(media_hashed_id, api_token, start_date=start_date, end_date=end_date)
         if events_data:
             # Save Events for this media to a local JSON file
             events_filename = os.path.join(RAW_DATA_DIR, f"events_{media_hashed_id}_{run_timestamp}.json")
             try:
                 with open(events_filename, 'w') as f:
                     json.dump(events_data, f, indent=2)
                 print(f"✅ Saved {len(events_data)} events for {media_hashed_id} to {events_filename}")
                 event_filenames[f'events_{media_hashed_id}'] = events_filename
             except Exception as e:
                 print(f"❌ Error saving events for {media_hashed_id} to file {events_filename}: {e}")
                 # Continue with other media IDs even if one fails to save
         else:
             print(f"No events fetched or could not fetch events for {media_hashed_id} within date range {start_date or 'all'} to {end_date or 'all'}")

    print(f"\n--- Wistia Data Ingestion Run Complete: {run_timestamp} ---")

    # Return the timestamp and the generated file paths for use by transformation script
    generated_files = {
        'run_timestamp': run_timestamp,
        'metadata': metadata_filename, # This will be the path to the all_media_metadata file
        **event_filenames # Include the dictionary of event filenames
    }
    return generated_files


# --- Main Execution (for direct running/testing this ingestion script) ---
# This block runs ONLY when you execute ingest_wistia.py directly.
# It's useful for testing the ingestion part in isolation.
# When orchestrated by process_wistia_data.py, that script will call run_wistia_ingestion().

if __name__ == "__main__":
    print("--- Starting Ingestion Script Test Run ---")

    if WISTIA_API_TOKEN == "YOUR_WISTIA_API_TOKEN_PLACEHOLDER" or not WISTIA_API_TOKEN:
        print("\n" + "="*50)
        print("!!! API TOKEN NOT CONFIGURED !!!")
        print("Please set the WISTIA_API_TOKEN environment variable or replace the placeholder.")
        print("DO NOT COMMIT YOUR ACTUAL TOKEN TO GIT.")
        print("="*50 + "\n")
        # import sys
        # sys.exit("API Token not configured.")

    else:
        print("Using configured Wistia API Token.")

        # Example of running the ingestion function for a specific date range (like a batch)
        # You might want to adjust these dates for testing
        # For testing incremental fetch capability of the API call:
        # Use a start_date that you know has some events after it.
        test_start_date = "2024-01-01" # Example historical start for testing batch fetch
        test_end_date = datetime.date.today().strftime("%Y-%m-%d") # Today's date for end

        # Run the ingestion logic
        # Pass the list of media IDs you want events for
        run_details = run_wistia_ingestion(WISTIA_API_TOKEN, MEDIA_HASHED_IDS, start_date=test_start_date, end_date=test_end_date)

        print("\n--- Ingestion Script Finished ---")
        print(f"Run Timestamp: {run_details.get('run_timestamp')}")
        print(f"Metadata file: {run_details.get('metadata')}")
        # Print event files dynamically
        print(f"Event files: {', '.join([f'{k}: {v}' for k,v in run_details.items() if k.startswith('events_')])}")
        print(f"Raw data saved to the '{RAW_DATA_DIR}' directory.")
        print("\nNext: Run the transformation and loading script (process_wistia_data.py) to process this raw data.")