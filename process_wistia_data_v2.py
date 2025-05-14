# Complete process_wistia_data.py code

import json
import os
from typing import List, Dict, Any, Tuple, Optional
import re
import datetime
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
from google.cloud import storage

# Import the ingestion script as a module (assuming it's in the same directory)
# Make sure ingest_wistia.py is updated with the run_wistia_ingestion function
# that fetches ALL media metadata and saves it as all_media_metadata_...json
import ingest_wistia

# --- Configuration ---
RAW_DATA_DIR = "raw_data" # Directory where raw data is saved
# Regex to find timestamps in filenames like *_YYYYMMDD_HHMMSS.json
TIMESTAMP_REGEX = re.compile(r"_(\d{8}_\d{6})\.json$")


# Google Cloud Project ID (should be set by gcloud init and ADC)
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "wistia-data-pipeline-project")

# BigQuery Dataset and Table IDs
BIGQUERY_DATASET_ID = "wistia_analytics_dwh"
BIGQUERY_TABLE_IDS = {
    "dim_media": "dim_media",
    "dim_visitor": "dim_visitor",
    "fact_media_engagement": "fact_media_engagement"
}

# Your GCS bucket name for staging transformed data
# Make sure this bucket exists and you have permissions to write to it
GCS_BUCKET_NAME = "wistia-raw-bucket-dea" # <<< REPLACE WITH YOUR ACTUAL GCS BUCKET NAME

# --- BigQuery Schema Definitions ---
# Define the schema for dim_media
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


# --- BigQuery Interaction Function (Table Creation) ---
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

    # Set partitioning and clustering (Optional but recommended)
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


# --- BigQuery Interaction Function (High-Water Mark) ---
def get_bigquery_high_water_mark(project_id: str, dataset_id: str, table_id: str) -> Optional[datetime.datetime]:
    """
    Queries BigQuery to get the maximum event_timestamp from the fact table.
    Returns the max timestamp as a datetime object, or None if the table is empty or doesn't exist.
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    query = f"""
        SELECT MAX(event_timestamp)
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE event_timestamp IS NOT NULL
    """

    print(f"\n--- Getting BigQuery High-Water Mark from {dataset_id}.{table_id} ---")
    print(f"Executing query: {query}")

    try:
        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            max_timestamp = row[0]

            if max_timestamp:
                print(f"✅ Found high-water mark: {max_timestamp}")
                return max_timestamp
            else:
                print("ℹ️ BigQuery table is empty or contains no valid timestamps. No high-water mark found.")
                return None

    except Exception as e:
        print(f"❌ Error getting high-water mark from BigQuery: {e}")
        print("Assuming no high-water mark (first run or error).")
        return None


# --- Data Reading Function ---
def read_raw_json_file(filepath: str) -> Any:
    """
    Reads and parses a local JSON file.
    """
    if not os.path.exists(filepath):
        print(f"❌ Error: File not found at {filepath}")
        return None
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError:
        print(f"❌ Error: Could not decode JSON from {filepath}")
        return None
    except Exception as e:
        print(f"❌ Error reading file {filepath}: {e}")
        return None


# --- File Discovery and Selection (Less relevant for incremental pipeline main flow) ---
# This function is less relevant for the main incremental pipeline flow which
# processes files generated by the current ingestion run.
def get_files_for_latest_run(raw_data_dir: str) -> Dict[str, Optional[str]]:
    """
    Identifies the filenames for the latest ingestion run based on timestamps.
    Looks for files matching the timestamp pattern.
    """
    all_files = os.listdir(raw_data_dir)
    run_timestamps = set()

    # Regex to find timestamps in filenames like *_YYYYMMDD_HHMMSS.json
    timestamp_pattern = re.compile(r"_(\d{8}_\d{6})\.json$")

    for filename in all_files:
        match = timestamp_pattern.search(filename)
        if match:
            run_timestamps.add(match.group(1))

    if not run_timestamps:
        print(f"❌ No data files found in '{raw_data_dir}'.")
        return {}

    latest_timestamp = sorted(list(run_timestamps))[-1]
    print(f"✅ Identified latest run timestamp based on filenames: {latest_timestamp}")

    files_to_process = {
        'metadata': None,
        # Define keys for event files based on your media IDs from ingest_wistia
        f'events_{ingest_wistia.MEDIA_HASHED_IDS[0]}': None,
        f'events_{ingest_wistia.MEDIA_HASHED_IDS[1]}': None,
        # Add other media IDs if needed
    }

    for filename in all_files:
        if latest_timestamp in filename:
             # Check for the 'all_media_metadata' file
            if filename.startswith("all_media_metadata_"):
                files_to_process['metadata'] = filename
            # Check for event files based on media IDs
            for media_id in ingest_wistia.MEDIA_HASHED_IDS:
                if filename.startswith(f"events_{media_id}_"):
                    files_to_process[f'events_{media_id}'] = filename

    missing_files = [key for key, value in files_to_process.items() if value is None]
    if missing_files:
        print(f"⚠️ Warning: Missing expected files for latest run ({latest_timestamp}): {', '.join(missing_files)}")

    return {key: os.path.join(raw_data_dir, filename) for key, filename in files_to_process.items() if filename is not None}



# --- Data Transformation Logic ---

# Modify transform_media_data to filter the list of ALL media metadata
def transform_media_data(all_raw_metadata: List[Dict[str, Any]], target_media_hashed_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Transforms raw media metadata (from the List endpoint) into the dim_media schema,
    filtering to include only specified target media IDs.
    Args:
        all_raw_metadata: List of all raw media metadata dictionaries.
        target_media_hashed_ids: List of hashed IDs for media to include.
    Returns:
        List of transformed media records for the target IDs.
    """
    transformed_data = []
    processing_timestamp = datetime.datetime.now().isoformat()

    if all_raw_metadata is None:
        return transformed_data

    print(f"Filtering {len(all_raw_metadata)} raw media metadata records for target IDs: {target_media_hashed_ids}")

    # Filter the raw metadata to include only the target media IDs
    filtered_raw_metadata = [
        media_record for media_record in all_raw_metadata
        if media_record.get("hashed_id") in target_media_hashed_ids
    ]

    print(f"Found {len(filtered_raw_metadata)} target media records matching target IDs.")

    # Now transform the filtered records
    for media_record in filtered_raw_metadata:
        project_data = media_record.get("project", {})
        title = media_record.get("name", "")
        channel = None
        if title and ("Facebook" in title or "FB" in title):
            channel = "Facebook"
        elif title and ("Youtube" in title or "YT" in title):
            channel = "YouTube"

        created_at_str = media_record.get("created")
        created_at_ts = None
        if created_at_str:
             try:
                 # Wistia timestamps are ISO 8601, handle potential 'Z' or offsets
                 created_at_ts = datetime.datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
             except ValueError:
                  print(f"⚠️ Warning: Could not parse created_at timestamp '{created_at_str}' for media {media_record.get('hashed_id')}")

        updated_at_str = media_record.get("updated")
        updated_at_ts = None
        if updated_at_str:
             try:
                 updated_at_ts = datetime.datetime.fromisoformat(updated_at_str.replace('Z', '+00:00'))
             except ValueError:
                  print(f"⚠️ Warning: Could not parse updated_at timestamp '{updated_at_str}' for media {media_record.get('hashed_id')}")

        transformed_data.append({
            "media_id": media_record.get("hashed_id"),
            "wistia_id": media_record.get("id"),
            "title": title,
            "url": None, # Will populate from event data later if needed
            "created_at": created_at_ts.isoformat() if created_at_ts else None, # ISO format string for BigQuery TIMESTAMP
            "duration": media_record.get("duration"), # This should now be available from the List endpoint
            "type": media_record.get("type"),
            "project_id": project_data.get("id"),
            "project_name": project_data.get("name"),
            "channel": channel,
            "updated_at": updated_at_ts.isoformat() if updated_at_ts else None, # ISO format string for BigQuery TIMESTAMP
            "ingestion_timestamp": processing_timestamp, # ISO format string for BigQuery TIMESTAMP
        })
    return transformed_data


def transform_visitor_data(all_raw_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extracts and transforms unique visitor data from raw event records.
    """
    transformed_data = []
    seen_visitor_keys = set()
    processing_timestamp = datetime.datetime.now().isoformat()

    if all_raw_events is None:
        return transformed_data

    for event_record in all_raw_events:
        visitor_key = event_record.get("visitor_key")

        if visitor_key and visitor_key not in seen_visitor_keys:
            ua_details = event_record.get("user_agent_details", {})

            transformed_data.append({
                "visitor_id": visitor_key,
                "ip_address": event_record.get("ip"),
                "country": event_record.get("country"),
                "region": event_record.get("region"),
                "city": event_record.get("city"),
                "user_agent_browser": ua_details.get("browser"),
                "user_agent_platform": ua_details.get("platform"),
                "user_agent_mobile": ua_details.get("mobile"),
                "ingestion_timestamp": processing_timestamp, # ISO format string for BigQuery TIMESTAMP
            })
            seen_visitor_keys.add(visitor_key)

    return transformed_data


# Function to aggregate fact data (including complex metrics)
# Updated aggregate_fact_data function with refined logic

def aggregate_fact_data(all_raw_events: List[Dict[str, Any]], media_lookup: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aggregates raw event data to derive metrics for fact_media_engagement.
    Groups events by (media_id, visitor_key, date) and calculates metrics.
    Args:
        all_raw_events: List of raw event dictionaries.
        media_lookup: Dictionary for quick lookup of media details by media_id (hashed_id).
                      e.g., {media_id: {duration: ..., title: ...}}
    Returns:
        List of aggregated fact records.
    """
    grouped_events = {}
    processing_timestamp = datetime.datetime.now().isoformat()

    if all_raw_events is None:
        return []

    print("--- Aggregating Fact Data ---")

    for event_record in all_raw_events:
        media_id = event_record.get("media_id")
        visitor_key = event_record.get("visitor_key")
        received_at_str = event_record.get("received_at")

        if media_id and visitor_key and received_at_str:
            try:
                event_timestamp_dt = datetime.datetime.fromisoformat(received_at_str.replace('Z', '+00:00'))
                event_date_str = event_timestamp_dt.date().isoformat()

                group_key = (media_id, visitor_key, event_date_str)

                if group_key not in grouped_events:
                    grouped_events[group_key] = []
                grouped_events[group_key].append(event_record)

            except ValueError:
                print(f"⚠️ Warning: Could not parse timestamp '{received_at_str}'. Skipping event for grouping.")
                continue

    print(f"Grouped events into {len(grouped_events)} unique media/visitor/date combinations.")

    aggregated_fact_data = []

    for (media_id, visitor_id, event_date), events_in_group in grouped_events.items():
        # Sort events by timestamp for sequential processing
        sorted_events = sorted(events_in_group, key=lambda x: datetime.datetime.fromisoformat(x.get("received_at", "1970-01-01T00:00:00Z").replace('Z', '+00:00')))

        # Play Count: Count 'play' events. If no 'play' but some progress, assume 1 play.
        play_count = sum(1 for event in sorted_events if event.get("name") == "play")
        if play_count == 0 and any(event.get("percent_viewed", 0.0) > 0 for event in sorted_events):
             play_count = 1 # Assume at least one play if any progress was made

        # --- Watch Time Calculation ---
        total_seconds_watched = 0.0
        last_relevant_event_time = None
        last_percent = 0.0

        media_duration = media_lookup.get(media_id, {}).get("duration")

        if media_duration is not None and media_duration > 0:
            for event in sorted_events:
                event_time_str = event.get("received_at")
                current_percent = event.get("percent_viewed")
                event_name = event.get("name") # Use event name if available

                if event_time_str and current_percent is not None:
                    try:
                        current_time = datetime.datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))

                        # Initialize or reset for watch time calculation
                        # Start tracking from the first event with percentage progress or a 'play' event
                        if last_relevant_event_time is None and (current_percent > 0 or event_name == 'play'):
                             last_relevant_event_time = current_time
                             last_percent = current_percent # Start from current percent


                        # If tracking has started and we have a previous point
                        elif last_relevant_event_time is not None:
                            time_elapsed = (current_time - last_relevant_event_time).total_seconds()

                            # Only add watch time if there was percentage progress and time elapsed
                            # Also consider event names if available for better pause/play detection
                            if time_elapsed > 0 and current_percent > last_percent:
                                 # If the event name is 'pause' or 'end', don't count watch time in this interval, just update point
                                 if event_name not in ['pause', 'end']:
                                     percentage_change = current_percent - last_percent
                                     # Calculate expected time for this percentage change at normal speed
                                     expected_time_for_change = (percentage_change / 100) * media_duration

                                     # Add the time watched in this interval. Cap at time_elapsed to handle faster-than-real-time playback
                                     # or potential clock drift between events.
                                     time_watched_in_interval = min(time_elapsed, expected_time_for_change)
                                     total_seconds_watched += time_watched_in_interval

                                 last_percent = current_percent
                                 last_relevant_event_time = current_time # Update last time for watch time calculation


                            # If percentage jumped significantly forward (seek), reset the starting point for the next calculation
                            # and update last_percent and last_relevant_event_time
                            # Using a threshold like 1% jump to detect seeks
                            elif current_percent > last_percent + 0.01:
                                last_percent = current_percent
                                last_relevant_event_time = current_time

                            # If percentage decreased or stayed same but time elapsed (could be pause, seek back, or just sparse events)
                            # Reset relevant time point to the current time to avoid counting time during pauses/seeks
                            # and update last_percent.
                            elif time_elapsed > 0 and current_percent <= last_percent:
                                last_percent = current_percent
                                last_relevant_event_time = current_time
                                # Note: This simplified logic doesn't perfectly handle pauses vs seeks back.
                                # A robust solution would use the 'name' field more extensively ('pause', 'seek').

                    except ValueError:
                         pass # Ignore events with invalid timestamps

        # Ensure total watch time doesn't exceed media duration (can happen with inexact float math or unusual events)
        if media_duration is not None:
             total_seconds_watched = min(total_seconds_watched, media_duration)


        play_rate_calculated = 0.0 # Default to 0.0
        if media_duration is not None and media_duration > 0:
            if total_seconds_watched > 0: # Only calculate play rate if there was watch time
                 play_rate_calculated = round((total_seconds_watched / media_duration), 2)


        # If play count is 0 after all logic, force total watch time and play rate to 0
        if play_count == 0:
             total_seconds_watched = 0.0
             play_rate_calculated = 0.0


        # Find the timestamp of the first event in the group for the 'event_timestamp' column
        # This is still a simplification, ideally event_timestamp is the timestamp of a key event like 'play' or first % > 0
        first_event_timestamp_iso = None
        if sorted_events:
            first_event_timestamp_str = sorted_events[0].get("received_at")
            if first_event_timestamp_str:
                try:
                    first_event_timestamp_dt = datetime.datetime.fromisoformat(first_event_timestamp_str.replace('Z', '+00:00'))
                    first_event_timestamp_iso = first_event_timestamp_dt.isoformat()
                except ValueError:
                    pass

        # Find the maximum percent viewed in the group
        max_percent_viewed = max((event.get("percent_viewed", 0.0) for event in sorted_events if event.get("percent_viewed") is not None), default=0.0)

        # Get IP and Country from the first event that has these fields (simplification)
        ip_address = None
        country = None
        for event in sorted_events:
            if event.get("ip"):
                ip_address = event.get("ip")
            if event.get("country"):
                country = event.get("country")
            if ip_address and country: # Stop if both found
                 break
        # Fallback if not found in sorted events
        if ip_address is None and country is None and events_in_group:
             first_event_fallback = events_in_group[0] # Just take the first event in the unsorted list
             ip_address = first_event_fallback.get("ip")
             country = first_event_fallback.get("country")


        # Final Aggregated Record for the group
        aggregated_fact_data.append({
            "media_id": media_id,
            "visitor_id": visitor_id,
            "date": event_date, # Date of the group
            "event_timestamp": first_event_timestamp_iso, # Timestamp of the first event in the group (simplification)
            "percent_viewed": max_percent_viewed, # Maximum percent viewed in the group
            "ip_address": ip_address, # Taken from the first event found with IP (simplification)
            "country": country, # Taken from the first event found with Country (simplification)
            "play_count": play_count, # Calculated play count (refined)
            "play_rate": play_rate_calculated, # Calculated play rate (refined)
            "total_watch_time": round(total_seconds_watched, 2), # Calculated total watch time (rounded, refined)
            "ingestion_timestamp": processing_timestamp, # Timestamp of this transformation run
        })

    return aggregated_fact_data


# --- BigQuery Loading Functions (Using GCS Load Jobs) ---

def save_data_to_gcs_jsonl(bucket_name: str, destination_blob_name: str, data: List[Dict[str, Any]]):
    """
    Saves a list of dictionaries to a GCS blob as newline-delimited JSON (JSON Lines).
    """
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    print(f"Attempting to save {len(data)} rows to gs://{bucket_name}/{destination_blob_name} as JSON Lines...")

    # Convert list of dicts to JSON Lines format
    # Ensure timestamps and dates are in ISO format strings for JSON Lines
    jsonl_data = "\n".join([json.dumps(record, default=str) for record in data]) # Use default=str to handle datetime objects


    try:
        blob.upload_from_string(jsonl_data, content_type='application/json')
        print(f"✅ Successfully saved {len(data)} rows to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"❌ Error saving data to GCS blob {destination_blob_name}: {e}")
        raise # Re-raise the exception as saving to GCS is critical


def load_data_from_gcs_to_bigquery(project_id: str, dataset_id: str, table_id: str, gcs_uri: str):
    """
    Initiates a BigQuery Load Job from a GCS URI.
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            schema=table.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
    except Exception as e:
        print(f"❌ Error getting schema for table {dataset_id}.{table_id}: {e}")
        print("Please ensure the BigQuery table exists and your credentials have access.")
        raise

    print(f"Attempting to load data from {gcs_uri} into {dataset_id}.{table_id} using a Load Job...")

    try:
        job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

        print(f"✅ Load job {job.job_id} initiated for {dataset_id}.{table_id}. Waiting for job to complete...")

        job.result()

        print(f"🎉 Load job {job.job_id} completed for {dataset_id}.{table_id}.")
        print(f"Loaded {job.output_rows} rows.")

    except Exception as e:
        print(f"❌ Error during BigQuery load job for {dataset_id}.{table_id}: {e}")
        raise


# --- Main Execution for Incremental Pipeline ---

if __name__ == "__main__":
    print("--- Starting Wistia Incremental Pipeline Run ---")

    # --- BigQuery Setup (Ensure Tables Exist) ---
    print("\n--- Setting up BigQuery Tables (if they don't exist) ---")
    create_bigquery_tables(PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_IDS)
    print("--- BigQuery Setup Complete ---")
    # -----------------------------------------------------

    # --- Determine High-Water Mark for Incremental Fetch ---
    high_water_mark = get_bigquery_high_water_mark(
        PROJECT_ID,
        BIGQUERY_DATASET_ID,
        BIGQUERY_TABLE_IDS["fact_media_engagement"]
    )

    ingestion_start_date = None
    if high_water_mark:
        ingestion_start_date = high_water_mark.isoformat().replace('+00:00', 'Z')
    else:
        # First run - define a historical start date
        ingestion_start_date = "2024-01-01" # <<< SET YOUR HISTORICAL START DATE HERE


    # Omit end_date to get data up to the current time
    ingestion_end_date = None


    print(f"Ingestion Date Range: {ingestion_start_date or 'beginning'} to {ingestion_end_date or 'current time'}")


    # --- Run Wistia Data Ingestion ---
    wistia_api_token = os.environ.get("WISTIA_API_TOKEN", "YOUR_WISTIA_API_TOKEN_PLACEHOLDER")
    if wistia_api_token == "YOUR_WISTIA_API_TOKEN_PLACEHOLDER" or not wistia_api_token:
         print("\n" + "="*50)
         print("!!! WISTIA API TOKEN NOT CONFIGURED !!!")
         print("Please set the WISTIA_API_TOKEN environment variable.")
         print("==================================================" + "\n")
         import sys
         sys.exit("Wistia API Token not configured.")


    # Call the ingestion function from ingest_wistia.py
    # Pass the list of media IDs you want events for
    run_details = ingest_wistia.run_wistia_ingestion(
        wistia_api_token,
        ingest_wistia.MEDIA_HASHED_IDS, # Use the MEDIA_HASHED_IDS from ingest_wistia.py for event fetching
        start_date=ingestion_start_date,
        end_date=ingestion_end_date
    )

    # --- Get paths to the raw data files generated by the current ingestion run ---
    # The metadata file is now named all_media_metadata_...json
    raw_files_paths = {
        'metadata': run_details.get('metadata'),
        # Define keys for event files based on your media IDs from ingest_wistia dynamically
    }
    # Add event file paths dynamically
    for media_id in ingest_wistia.MEDIA_HASHED_IDS:
         raw_files_paths[f'events_{media_id}'] = run_details.get(f'events_{media_id}')


    run_timestamp_of_ingestion = run_details.get('run_timestamp')

    # Check if ingestion was successful and generated necessary files
    # Check for the all_media_metadata file and the required event files
    required_event_file_keys = [f'events_{mid}' for mid in ingest_wistia.MEDIA_HASHED_IDS]
    if not raw_files_paths.get('metadata') or not all(raw_files_paths.get(key) for key in required_event_file_keys):
         print("\n❌ Ingestion failed to generate necessary raw data files. Stopping pipeline.")
         import sys
         sys.exit("Ingestion failed.")


    # --- Read Raw Data from Current Ingestion Run ---
    print("\n--- Reading Raw Data from Current Ingestion Run ---")

    # Read the ALL media metadata file
    all_raw_media_metadata = read_raw_json_file(raw_files_paths['metadata']) if raw_files_paths.get('metadata') else None

    # Read the event files for the target media IDs dynamically
    raw_event_data_lists = {}
    for media_id in ingest_wistia.MEDIA_HASHED_IDS:
        event_data = read_raw_json_file(raw_files_paths[f'events_{media_id}']) if raw_files_paths.get(f'events_{media_id}') else None
        if event_data is not None and isinstance(event_data, list):
             raw_event_data_lists[f'events_{media_id}'] = event_data
        elif event_data is not None:
             print(f"⚠️ Warning: Raw event data for {media_id} was not a list. Skipping.")
             raw_event_data_lists[f'events_{media_id}'] = []
        else:
             raw_event_data_lists[f'events_{media_id}'] = [] # Ensure it's an empty list if file not found


    # --- Transformation Logic ---
    transformed_media_data = []
    transformed_visitor_data = []
    transformed_fact_data = []


    # Check if necessary raw data was read before applying transformations
    # Check that all_raw_media_metadata is a list (expected format from List endpoint)
    # Check that raw event data lists for all required media IDs are available and are lists
    if all_raw_media_metadata is not None and isinstance(all_raw_media_metadata, list) and \
       all(isinstance(raw_event_data_lists.get(f'events_{mid}'), list) for mid in ingest_wistia.MEDIA_HASHED_IDS):
        print("\n--- Raw Data Read Successfully ---")
        print("Applying transformation logic...")

        # Transform Media Data (passing the full list and target IDs for filtering)
        transformed_media_data = transform_media_data(all_raw_media_metadata, ingest_wistia.MEDIA_HASHED_IDS)
        print(f"Transformed {len(transformed_media_data)} records for dim_media (filtered).")

        # Combine all raw event data lists into one list
        all_raw_events = []
        for media_id in ingest_wistia.MEDIA_HASHED_IDS:
             all_raw_events.extend(raw_event_data_lists.get(f'events_{media_id}', []))

        print(f"Total raw event records for transformation: {len(all_raw_events)}")

        # Transform Visitor Data
        transformed_visitor_data = transform_visitor_data(all_raw_events)
        print(f"Transformed {len(transformed_visitor_data)} records for dim_visitor.")

        # --- Create a lookup for media duration by media_id ---
        # This lookup should now contain duration if available from the List endpoint
        media_duration_lookup = {
            media.get("media_id"): {
                "duration": media.get("duration"),
                "title": media.get("title")
                }
            for media in transformed_media_data if media.get("media_id") and media.get("duration") is not None
        }
        print(f"Created media duration lookup for {len(media_duration_lookup)} media IDs with duration.")


        # Transform Fact Data (Aggregate Metrics)
        transformed_fact_data = aggregate_fact_data(all_raw_events, media_duration_lookup)
        print(f"Transformed {len(transformed_fact_data)} records for fact_media_engagement (aggregated).")


        # --- Loading Data into BigQuery (via GCS Load Job) ---
        print("\n--- Loading Transformed Data into BigQuery (via GCS Load Job) ---")

        # Use the ingestion run timestamp for the GCS path
        load_blob_prefix = f"transformed_wistia_data/{run_timestamp_of_ingestion}"

        # Load dim_media
        if transformed_media_data:
            media_blob_name = f"{load_blob_prefix}/dim_media.jsonl"
            try:
                save_data_to_gcs_jsonl(GCS_BUCKET_NAME, media_blob_name, transformed_media_data)
                media_gcs_uri = f"gs://{GCS_BUCKET_NAME}/{media_blob_name}"
                load_data_from_gcs_to_bigquery(PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_IDS["dim_media"], media_gcs_uri)
            except Exception as e:
                print(f"❌ Failed to save or load dim_media data: {e}")


        # Load dim_visitor
        if transformed_visitor_data:
            visitor_blob_name = f"{load_blob_prefix}/dim_visitor.jsonl"
            try:
                save_data_to_gcs_jsonl(GCS_BUCKET_NAME, visitor_blob_name, transformed_visitor_data)
                visitor_gcs_uri = f"gs://{GCS_BUCKET_NAME}/{visitor_blob_name}"
                load_data_from_gcs_to_bigquery(PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_IDS["dim_visitor"], visitor_gcs_uri)
            except Exception as e:
                print(f"❌ Failed to save or load dim_visitor data: {e}")


        # Load fact_media_engagement (Append is generally suitable for fact tables)
        if transformed_fact_data:
            fact_blob_name = f"{load_blob_prefix}/fact_media_engagement.jsonl"
            try:
                save_data_to_gcs_jsonl(GCS_BUCKET_NAME, fact_blob_name, transformed_fact_data)
                fact_gcs_uri = f"gs://{GCS_BUCKET_NAME}/{fact_blob_name}"
                load_data_from_gcs_to_bigquery(PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_IDS["fact_media_engagement"], fact_gcs_uri)
            except Exception as e:
                print(f"❌ Failed to save or load fact_media_engagement data: {e}")

        print("\n--- Pipeline Run Complete ---")
        print("Check Google Cloud Console > BigQuery > Job History for Load Job status.")
        print("Check Google Cloud Console > Cloud Storage > Buckets > YOUR_GCS_BUCKET_NAME > transformed_wistia_data for temporary files.")


    else:
        print("\n--- Could not read all necessary raw data files from the current ingestion run ---")
        print("Please check the ingestion process and the expected files based on its output.")