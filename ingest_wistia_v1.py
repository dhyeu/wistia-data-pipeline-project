import requests
import os
import json
from typing import Optional, Dict, List, Any
import datetime
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# --- Configuration ---
# Ensure your WISTIA_API_TOKEN is configured securely (environment variable is best)
# REPLACE 'YOUR_WISTIA_API_TOKEN_PLACEHOLDER' with your actual token ONLY FOR LOCAL TESTING
# if you are not using environment variables yet.
# ***REMEMBER TO NEVER COMMIT YOUR ACTUAL TOKEN TO GITHUB***
WISTIA_API_TOKEN = os.environ.get("WISTIA_API_TOKEN", "YOUR_WISTIA_API_TOKEN_PLACEHOLDER")

# Google Cloud Project ID (should be set by gcloud init and ADC)
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "wistia-data-pipeline-project")

# BigQuery Dataset and Table IDs
BIGQUERY_DATASET_ID = "wistia_analytics_dwh"
BIGQUERY_TABLE_IDS = {
    "dim_media": "dim_media",
    "dim_visitor": "dim_visitor",
    "fact_media_engagement": "fact_media_engagement"
}

# The specific Media IDs provided in the requirements document (these are hashed IDs)
MEDIA_HASHED_IDS = ["gskhw4w4lm", "v08dlrgr7v"]

# Wistia API Base URLs
STATS_BASE_URL = "https://api.wistia.com/v1/stats/medias/"
MEDIA_BASE_URL = "https://api.wistia.com/v1/medias/"
EVENTS_LIST_URL = "https://api.wistia.com/v1/stats/events.json" # Corrected base URL name

# Define a directory for raw data (create if it doesn't exist)
RAW_DATA_DIR = "raw_data"
os.makedirs(RAW_DATA_DIR, exist_ok=True)


# --- BigQuery Schema Definitions ---

# Define the schema for dim_media
# Mapped from Media Show JSON and Events List JSON
dim_media_schema = [
    bigquery.SchemaField("media_id", "STRING", mode="REQUIRED"), # Hashed ID
    bigquery.SchemaField("wistia_id", "INTEGER", mode="NULLABLE"), # Internal ID
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("url", "STRING", mode="NULLABLE"), # Getting from event data's media_url
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("duration", "BIGNUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("project_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("project_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("channel", "STRING", mode="NULLABLE"), # Derived/Inferred
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"), # Timestamp when loaded into DWH
]

# Define the schema for dim_visitor
# Mapped primarily from Events List JSON
dim_visitor_schema = [
    bigquery.SchemaField("visitor_id", "STRING", mode="REQUIRED"), # visitor_key
    bigquery.SchemaField("ip_address", "STRING", mode="NULLABLE"), # ip
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"), # country
    bigquery.SchemaField("region", "STRING", mode="NULLABLE"), # region
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"), # city
    bigquery.SchemaField("user_agent_browser", "STRING", mode="NULLABLE"), # user_agent_details.browser
    bigquery.SchemaField("user_agent_platform", "STRING", mode="NULLABLE"), # user_agent_details.platform
    bigquery.SchemaField("user_agent_mobile", "BOOL", mode="NULLABLE"), # user_agent_details.mobile
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"), # Timestamp when loaded into DWH
]

# Define the schema for fact_media_engagement
# Mapped from Events List JSON + derived metrics
fact_media_engagement_schema = [
    bigquery.SchemaField("media_id", "STRING", mode="REQUIRED"), # Hashed media ID (from event)
    bigquery.SchemaField("visitor_id", "STRING", mode="REQUIRED"), # visitor_key (from event)
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"), # Date of event (from received_at) - Daily grain?
    bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="NULLABLE"), # Exact event timestamp (from received_at)
    bigquery.SchemaField("percent_viewed", "BIGNUMERIC", mode="NULLABLE"), # percent_viewed (from event)
    bigquery.SchemaField("ip_address", "STRING", mode="NULLABLE"), # ip (from event - denormalized)
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"), # country (from event - denormalized)
    bigquery.SchemaField("play_count", "INTEGER", mode="NULLABLE"), # Derived from events
    bigquery.SchemaField("play_rate", "BIGNUMERIC", mode="NULLABLE"), # Derived from events
    bigquery.SchemaField("total_watch_time", "BIGNUMERIC", mode="NULLABLE"), # Derived from events (in seconds?)
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"), # Timestamp when loaded into DWH
    # Consider Partitioning (e.g., by 'date') and Clustering ('media_id', 'visitor_id') here for performance
    # bigquery.TimePartitioning(field="date")
    # clustering_fields=["media_id", "visitor_id"]
]


# --- BigQuery Interaction Function ---

def create_bigquery_tables(project_id: str, dataset_id: str, table_ids: Dict[str, str]):
    """
    Creates BigQuery dataset and tables if they do not exist.
    """
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)

    # Create Dataset
    try:
        client.create_dataset(dataset_ref)
        print(f"✅ Dataset '{dataset_id}' created in project '{project_id}'.")
    except Conflict:
        print(f"ℹ️ Dataset '{dataset_id}' already exists in project '{project_id}'.")
    except Exception as e:
        print(f"❌ Error creating dataset '{dataset_id}': {e}")
        return

    # Define table configurations with their schemas
    table_configs = {
        table_ids["dim_media"]: bigquery.Table(dataset_ref.table(table_ids["dim_media"]), schema=dim_media_schema),
        table_ids["dim_visitor"]: bigquery.Table(dataset_ref.table(table_ids["dim_visitor"]), schema=dim_visitor_schema),
        table_ids["fact_media_engagement"]: bigquery.Table(dataset_ref.table(table_ids["fact_media_engagement"]), schema=fact_media_engagement_schema),
    }

    # Set partitioning and clustering for the fact table (Recommended for large tables)
    # fact_table_ref = dataset_ref.table(table_ids["fact_media_engagement"])
    # fact_table_configs = bigquery.Table(fact_table_ref, schema=fact_media_engagement_schema)
    # fact_table_configs.time_partitioning = bigquery.TimePartitioning(field="date")
    # fact_table_configs.clustering_fields = ["media_id", "visitor_id"]
    # table_configs[table_ids["fact_media_engagement"]] = fact_table_configs

    # Create Tables
    for table_name, table in table_configs.items():
        try:
            client.create_table(table)
            print(f"✅ Table '{table_name}' created in dataset '{dataset_id}'.")
        except Conflict:
            print(f"ℹ️ Table '{table_name}' already exists in dataset '{dataset_id}'.")
        except Exception as e:
            print(f"❌ Error creating table '{table_name}': {e}")


# --- API Interaction Functions ---

def get_media_stats(media_id: str, api_token: str) -> Optional[Dict]:
    """
    Fetches aggregated statistics for a given Wistia media ID.
    """
    url = f"{STATS_BASE_URL}{media_id}.json"
    headers = {"Authorization": f"Bearer {api_token}"}
    print(f"Attempting to fetch stats for media ID: {media_id}")
    try:
        response = requests.get(url, headers=headers, timeout=60) # Added timeout
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


def fetch_media_metadata(media_hashed_id: str, api_token: str) -> Optional[Dict]:
    """
    Fetches metadata for a given Wistia media hashed ID using the Media Show endpoint.
    """
    url = f"{MEDIA_BASE_URL}{media_hashed_id}.json"
    headers = {"Authorization": f"Bearer {api_token}"}

    print(f"Attempting to fetch metadata for media hashed ID: {media_hashed_id}")

    try:
        response = requests.get(url, headers=headers, timeout=60) # Added timeout
        response.raise_for_status()

        if response.status_code == 200:
            print(f"✅ Successfully retrieved metadata for {media_hashed_id}")
            metadata = response.json()

            # --- TEMPORARY: Print the full JSON response to inspect keys ---
            # Commented out now that we've confirmed the keys
            # print(f"\nFull Metadata JSON for {media_hashed_id}:")
            # print(json.dumps(metadata, indent=2))
            # print("--- End Full Metadata JSON ---\n")
            # --- END TEMPORARY ---

            extracted_metadata = {
                "id": metadata.get("id"),
                "hashed_id": metadata.get("hashed_id"),
                "title": metadata.get("name"),
                "created_at": metadata.get("created"), # Use the correct key 'created'
                # Get 'url' from event data later (media_url)
            }
            return extracted_metadata
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


def fetch_media_events(media_hashed_id: str, api_token: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetches all events for a given media hashed ID, handling pagination and date filtering.
    """
    all_events = []
    page = 1
    per_page = 100

    print(f"Attempting to fetch events for media hashed ID: {media_hashed_id}")

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
            # Add a timeout to the request (e.g., 60 seconds)
            response = requests.get(EVENTS_LIST_URL, headers=headers, params=params, timeout=60)
            response.raise_for_status()

            if response.status_code == 200:
                events_on_page = response.json()

                if not events_on_page:
                    print(f"  No events found on page {page} or reached end of pagination for {media_hashed_id}.")
                    break

                print(f"  ✅ Successfully retrieved {len(events_on_page)} events from page {page}")
                all_events.extend(events_on_page)
                page += 1

            else:
                 print(f"⚠️ Error: Received status code {response.status_code} for events {media_hashed_id} page {page} - {response.text}")
                 break

        except requests.exceptions.Timeout:
             print(f"❌ Events request timed out for media hashed ID {media_hashed_id} page {page}.")
             break # Break on timeout, or implement retry logic here
        except requests.exceptions.RequestException as e:
            print(f"❌ Events request failed for media hashed ID {media_hashed_id} page {page}: {e}")
            break
        except json.JSONDecodeError:
            print(f"❌ Failed to decode JSON response for events page {page}")
            break
        except Exception as e:
            print(f"❌ An unexpected error occurred during events fetch for {media_hashed_id} page {page}: {e}")
            break

    print(f"Finished fetching events for {media_hashed_id}. Total events fetched: {len(all_events)}")
    return all_events


# --- Main Execution ---

if __name__ == "__main__":
    # --- BigQuery Setup (Programmatic Schema Definition) ---
    print("\n--- Setting up BigQuery Tables ---")
    # Ensure your gcloud ADC and quota project are set up:
    # gcloud auth application-default login
    # gcloud auth application-default set-quota-project your-project-id
    create_bigquery_tables(PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_IDS)
    print("--- BigQuery Setup Complete ---")
    # -----------------------------------------------------


    if WISTIA_API_TOKEN == "YOUR_WISTIA_API_TOKEN_PLACEHOLDER":
        print("\n" + "="*50)
        print("!!! SECURITY WARNING !!!")
        print("Please replace 'YOUR_WISTIA_API_TOKEN_PLACEHOLDER' with your actual Wistia API token.")
        print("Alternatively, set the WISTIA_API_TOKEN environment variable.")
        print("DO NOT COMMIT YOUR ACTUAL TOKEN TO GIT.")
        print("="*50 + "\n")
        # import sys
        # sys.exit("API Token placeholder not replaced.")

    elif not WISTIA_API_TOKEN:
         print("❌ Error: Wistia API token not found. Please set the WISTIA_API_TOKEN environment variable or replace the placeholder.")
         # import sys
         # sys.exit("API Token not configured.")

    else:
        print("Using configured Wistia API Token.")

        # Generate a run timestamp for organizing raw data files
        run_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"Starting data fetch at: {run_timestamp}")

        # --- Fetch Media Metadata ---
        print("\n--- Fetching Media Metadata ---")
        all_media_metadata = []
        for media_hashed_id in MEDIA_HASHED_IDS:
            metadata = fetch_media_metadata(media_hashed_id, WISTIA_API_TOKEN)
            if metadata:
                all_media_metadata.append(metadata)
                # print(f"Extracted metadata for {media_hashed_id}: {metadata}")

        # Save Media Metadata to a local JSON file
        if all_media_metadata:
            metadata_filename = os.path.join(RAW_DATA_DIR, f"media_metadata_{run_timestamp}.json")
            with open(metadata_filename, 'w') as f:
                json.dump(all_media_metadata, f, indent=2)
            print(f"✅ Saved media metadata to {metadata_filename}")
        else:
            print("No media metadata fetched to save.")


        # --- Fetch Media Events ---
        print("\n--- Fetching Media Events ---")
        all_fetched_events = {} # Dictionary to store events per media ID

        # Define a date range for fetching events (YYYY-MM-DD)
        # IMPORTANT: Adjust dates based on your data requirements and retention policy.
        test_start_date = "2024-01-01" # Example start date
        test_end_date = datetime.date.today().strftime("%Y-%m-%d") # Today's date as end date


        for media_hashed_id in MEDIA_HASHED_IDS:
             events_data = fetch_media_events(media_hashed_id, WISTIA_API_TOKEN, start_date=test_start_date, end_date=test_end_date)
             if events_data:
                 all_fetched_events[media_hashed_id] = events_data
                 print(f"Total {len(events_data)} events fetched for {media_hashed_id} within date range {test_start_date} to {test_end_date}")

                 # Save Events for this media to a local JSON file
                 events_filename = os.path.join(RAW_DATA_DIR, f"events_{media_hashed_id}_{run_timestamp}.json")
                 with open(events_filename, 'w') as f:
                     json.dump(events_data, f, indent=2)
                 print(f"✅ Saved {len(events_data)} events for {media_hashed_id} to {events_filename}")

             else:
                 print(f"No events or could not fetch events for {media_hashed_id} within date range {test_start_date} to {test_end_date}")

        print("\n--- Data Fetching and Local Staging Complete ---")
        print(f"Raw data saved in the '{RAW_DATA_DIR}' directory.")
        print("Note: Data was manually uploaded to GCS in a previous step. In a full pipeline, this would be automated.")


        print("\n--- Next Steps ---")
        print("1. Implement the data transformation logic (Python).")
        print(f"2. Load transformed data into BigQuery tables: {BIGQUERY_DATASET_ID}.{{dim_media, dim_visitor, fact_media_engagement}}.")
        print("   Remember to load the data from the GCS files you uploaded.")