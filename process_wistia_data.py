import os
import requests
import json
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery, storage
import tempfile
import uuid
import logging
import sys

# Configure basic logging to stdout (automatically captured by Cloud Logging in Cloud Run)
logging.basicConfig(
    level=logging.INFO, # Set the minimum logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Configuration (read from environment variables)
# In Cloud Run, these are set via --set-env-vars and --set-secrets
WISTIA_API_TOKEN = os.environ.get('WISTIA_API_TOKEN')
# Ensure leading/trailing whitespace is removed from the token
if WISTIA_API_TOKEN:
    WISTIA_API_TOKEN = WISTIA_API_TOKEN.strip()

GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
PROJECT_ID = os.environ.get('PROJECT_ID') # Often implicitly available, but explicit is fine

# BigQuery Configuration
BIGQUERY_DATASET = 'wistia_analytics_dwh'
DIM_MEDIA_TABLE = f'{PROJECT_ID}.{BIGQUERY_DATASET}.dim_media'
DIM_VISITOR_TABLE = f'{PROJECT_ID}.{BIGQUERY_DATASET}.dim_visitor' # Note: Visitor data is not currently collected by this pipeline, but table defined
FACT_ENGAGEMENT_TABLE = f'{PROJECT_ID}.{BIGQUERY_DATASET}.fact_media_engagement'

# Wistia API Endpoints
# Use the List endpoint for media metadata to get duration
WISTIA_API_MEDIA_LIST_URL = 'https://api.wistia.com/v1/medias.json'
WISTIA_API_MEDIA_EVENTS_URL = 'https://api.wistia.com/v1/stats/events.json' # Base URL for events

# Specify Wistia Media Hashed IDs to include
# ###########################################################################
# IMPORTANT: YOU MUST REPLACE THE COMMENTS BELOW WITH YOUR ACTUAL WISTIA MEDIA HASHED IDs
# These are the specific IDs you found in the debug log output (execution wistia-pipeline-job-mt99l).
# Make sure each ID is a string enclosed in double quotes, separated by commas.
# ###########################################################################
MEDIA_HASHED_IDS = [
    "aox0k6q0mx",
    "46xq1tb537",
    "18kcofhz1p",
    "3pfoxdkkxn",
    "417twacp0c",
    # ... ADD ALL THE OTHER IDs YOU CHOSE FROM THE DEBUG LOGS HERE ...
]
# ###########################################################################
# ENSURE YOU HAVE ADDED YOUR ACTUAL CHOSEN HASHED IDs ABOVE THIS LINE
# ###########################################################################


def get_last_event_timestamp(bigquery_client):
    """
    Queries BigQuery to find the timestamp of the most recent event.
    Used as the high-water mark for incremental loading.
    """
    logger.info(f"Checking last event timestamp in {FACT_ENGAGEMENT_TABLE}...")
    query = f"""
        SELECT
            MAX(event_timestamp)
        FROM
            `{FACT_ENGAGEMENT_TABLE}`
    """
    try:
        query_job = bigquery_client.query(query)
        results = query_job.result()
        for row in results:
            last_timestamp = row[0]
            if last_timestamp:
                # BigQuery timestamp is timezone-aware, convert to UTC and format for Wistia API
                last_timestamp_utc = last_timestamp.astimezone(timezone.utc)
                # Wistia API expects ISO 8601 format including time for 'since' param
                formatted_timestamp = last_timestamp_utc.isoformat(timespec='seconds').replace('+00:00', 'Z') # Use Z for UTC
                logger.info(f"Found last event timestamp: {formatted_timestamp}")
                return formatted_timestamp
            else:
                logger.info("No events found in BigQuery. Performing initial load.")
                return None # Indicates no previous data, do a full load (Wistia API defaults)
    except Exception as e:
        logger.error(f"Error querying BigQuery for last event timestamp: {e}", exc_info=True)
        # If query fails, log the error and proceed as if no data exists to attempt a load
        logger.warning("Proceeding as if no previous data exists due to BigQuery query error.")
        return None


def fetch_wistia_data(api_url, headers=None, params=None, page=1, per_page=100):
    """
    Fetches data from a Wistia API endpoint with pagination.
    Uses Authorization: Bearer header for authentication.
    """
    all_data = []
    current_page = page
    logger.info(f"Starting data fetch from {api_url}...")

    while True:
        current_params = params.copy() if params else {}
        current_params['page'] = current_page
        current_params['per_page'] = per_page

        # Use provided headers, which should contain the Authorization: Bearer header
        request_headers = headers.copy() if headers else {}

        try:
            # Pass headers and params to requests.get
            response = requests.get(api_url, headers=request_headers, params=current_params)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            data = response.json()

            if not data:
                logger.info(f"No data returned on page {current_page}. Ending pagination.")
                break # No more data

            all_data.extend(data)
            logger.info(f"Fetched {len(data)} items from page {current_page}. Total items fetched so far: {len(all_data)}")

            # Wistia API pagination: Check if the number of items returned is less than per_page
            if len(data) < per_page:
                 logger.info("Last page reached.")
                 break # Last page

            current_page += 1
            logger.debug(f"Proceeding to page {current_page}")


        except requests.exceptions.RequestException as e:
            # Log the request details *without* the full token value for security
            logger.error(f"Wistia API request failed for {api_url} (Page {current_page}). Status Code: {e.response.status_code if e.response else 'N/A'}. Details: {e}", exc_info=True)
            raise # Re-raise the exception to be caught by the main handler
        except json.JSONDecodeError as e:
             logger.error(f"Failed to decode JSON response from {api_url} (Page {current_page}): {e}", exc_info=True)
             raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during fetch from {api_url} (Page {current_page}): {e}", exc_info=True)
            raise


    logger.info(f"Finished fetching data from {api_url}. Total items fetched: {len(all_data)}")
    return all_data


def filter_media_metadata(media_list):
    """
    Filters media metadata to include only the specified hashed IDs.
    """
    if not MEDIA_HASHED_IDS:
        logger.warning("MEDIA_HASHED_IDS list is empty. No media metadata will be filtered.")
        return []

    logger.info(f"Filtering media metadata for {len(MEDIA_HASHED_IDS)} specified IDs...")
    # Ensure hashed IDs from API response are treated as strings for comparison
    filtered_media = [media for media in media_list if str(media.get('hashed_id')) in MEDIA_HASHED_IDS]
    logger.info(f"Found {len(filtered_media)} matching media assets.")
    return filtered_media


def save_data_to_gcs_jsonl(bucket_name, folder_path, file_name, data):
    """
    Saves a list of dictionaries to a JSON Lines file in Google Cloud Storage.
    """
    gcs_uri = f'gs://{bucket_name}/{folder_path}/{file_name}'
    logger.info(f"Saving data to GCS: {gcs_uri}...")
    storage_client = storage.Client() # Client picks up project from env

    # Ensure the folder path exists implicitly with the blob name
    blob = storage_client.bucket(bucket_name).blob(f'{folder_path}/{file_name}')

    # Use a temporary file to write the JSON Lines data
    # Cloud Run provides a writable /tmp directory
    temp_dir = tempfile.gettempdir()
    temp_file_path = os.path.join(temp_dir, f"{uuid.uuid4()}.jsonl") # Use UUID for uniqueness

    try:
        with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
            for item in data:
                temp_file.write(json.dumps(item) + '\n')

        blob.upload_from_filename(temp_file_path, content_type='application/jsonl')
        logger.info(f"Successfully saved data to GCS: {gcs_uri}")
    except Exception as e:
        logger.error(f"Failed to save data to GCS: {gcs_uri}: {e}", exc_info=True)
        raise
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logger.debug(f"Cleaned up temporary file: {temp_file_path}")


def load_data_to_bigquery(gcs_uri, table_id, bigquery_client, schema, write_disposition):
    """
    Loads data from a JSON Lines file in GCS into a BigQuery table.
    """
    logger.info(f"Loading data from {gcs_uri} to BigQuery table {table_id}...")

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition, # APPEND, WRITE_TRUNCATE, WRITE_EMPTY
        # For incremental load (append), rely on the data having only new records
        # For dim tables (upsert/merge logic needed separately or handled by WRITE_TRUNCATE if full refresh is acceptable)
    )

    load_job = None # Initialize job variable

    try:
        load_job = bigquery_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )
        logger.info(f"BigQuery load job {load_job.job_id} initiated for table {table_id}. Waiting for job to complete...")
        load_job.result() # Wait for the job to complete

        if load_job.state == 'DONE':
            logger.info(f"BigQuery load job {load_job.job_id} for table {table_id} completed successfully.")
            logger.info(f"Loaded {load_job.output_rows} rows into {table_id}.")
        else:
            logger.error(f"BigQuery load job {load_job.job_id} for table {table_id} failed with state: {load_job.state}")
            if load_job.error_result:
                 logger.error(f"BigQuery Job Error for {table_id}: {load_job.error_result}")
            raise Exception(f"BigQuery load job failed for table {table_id}: {load_job.job_id}")

    except Exception as e:
        job_id_log = load_job.job_id if load_job and 'job_id' in dir(load_job) else 'N/A'
        logger.error(f"An error occurred during BigQuery load job {job_id_log} for table {table_id}: {e}", exc_info=True)
        raise


# --- BigQuery Schemas ---
# Define BigQuery schemas based on your target tables

# Schema for dim_media (based on relevant fields from Wistia media list response)
SCHEMA_DIM_MEDIA = [
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("hashed_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("duration", "FLOAT", mode="NULLABLE"), # Duration in seconds
    bigquery.SchemaField("section", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    # Add other relevant fields from media metadata if needed
]

# Schema for dim_visitor (placeholder, not currently populated)
SCHEMA_DIM_VISITOR = [
    bigquery.SchemaField("visitor_id", "STRING", mode="REQUIRED"), # Wistia visitor key
    # Add other potential visitor attributes if captured
]

# Schema for fact_media_engagement (aggregated event data)
SCHEMA_FACT_ENGAGEMENT = [
    bigquery.SchemaField("media_hashed_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"), # Timestamp of the event or aggregation period start
    bigquery.SchemaField("visitor_id", "STRING", mode="NULLABLE"), # Wistia visitor key (if event is per visitor)
    bigquery.SchemaField("play_count", "INTEGER", mode="NULLABLE"), # Sum of play events
    bigquery.SchemaField("play_rate", "FLOAT", mode="NULLABLE"), # Calculated play rate (play_count / views or impressions) - Need views/impressions data if available
    bigquery.SchemaField("total_watch_time", "FLOAT", mode="NULLABLE"), # Aggregated total watch time in seconds
    # Add other relevant aggregated metrics
]


def transform_media_data(media_list):
    """
    Transforms raw media metadata into the dim_media schema.
    """
    logger.info(f"Transforming {len(media_list)} media metadata records...")
    transformed = []
    for media in media_list:
        transformed.append({
            "id": media.get("id"),
            "hashed_id": media.get("hashed_id"),
            "name": media.get("name"),
            "type": media.get("type"),
            "created_at": media.get("created_at"), # Assuming ISO 8601 format from API
            "updated_at": media.get("updated_at"), # Assuming ISO 8601 format from API
            "duration": media.get("duration"),    # Duration in seconds
            "section": media.get("section"),
            "description": media.get("description"),
            # Map other fields as needed
        })
    logger.info(f"Finished transforming {len(transformed)} media metadata records.")
    return transformed


def aggregate_fact_data(event_data, media_duration_map):
    """
    Aggregates raw event data into the fact_media_engagement schema.
    Calculates metrics like total_watch_time using media duration.
    This is a basic aggregation by media_hashed_id and timestamp.
    More granular aggregation (e.g., hourly, daily, or by visitor) would require
    grouping logic here. For simplicity, we'll create a record per event for now,
    but structure it for potential aggregation.
    """
    logger.info(f"Aggregating {len(event_data)} event records...")
    aggregated = []
    # Define base event parameters outside the loop if they don't change per media
    # (e.g., 'since' parameter from get_last_event_timestamp)
    # Assuming base_event_params was intended to be used here from main
    # For this function, we only need the raw event_data and duration map
    # Re-initializing here just for function scope clarity if not passed in
    base_event_params = {} # This should ideally be passed into this function if used for filtering within aggregation

    for event in event_data:
        media_hashed_id = event.get('media', {}).get('hashed_id')
        event_timestamp_str = event.get('occurred_at') # Assuming ISO 8601 format from API
        visitor_id = event.get('visitor', {}).get('key')
        event_type = event.get('type') # e.g., 'play', 'percent:0', 'percent:50', 'percent:100'
        # engagement = event.get('engagement') # Engagement percentage for percent events

        # Basic aggregation logic: Treat each 'play' event as 1 play
        play_count = 1 if event_type == 'play' else None
        # Calculate watch time based on percentage completion events if duration is known
        watch_time = None
        if event_type == 'percent:100' and media_hashed_id and media_hashed_id in media_duration_map and media_duration_map[media_hashed_id] is not None:
             try:
                 # Ensure duration is a valid number before using it
                 duration = float(media_duration_map[media_hashed_id])
                 watch_time = duration # Assume completing 100% means watching the full duration
             except (ValueError, TypeError):
                 logger.warning(f"Invalid duration '{media_duration_map[media_hashed_id]}' for media '{media_hashed_id}'. Cannot calculate watch time.")


        # Assuming event_timestamp is the relevant timestamp for the fact table record
        # Need to parse the ISO 8601 string to a BigQuery compatible timestamp (datetime object)
        event_timestamp = None
        if event_timestamp_str:
            try:
                # Parse ISO 8601 string with timezone
                event_timestamp = datetime.fromisoformat(event_timestamp_str.replace('Z', '+00:00'))
                # Ensure it's timezone-aware (fromisoformat should handle this with +00:00)
            except ValueError as e:
                 logger.warning(f"Could not parse event timestamp '{event_timestamp_str}': {e}")
                 event_timestamp = None # Set to None if parsing fails


        # Only include records with a valid timestamp and media ID
        if event_timestamp and media_hashed_id:
            aggregated.append({
                "media_hashed_id": media_hashed_id,
                "event_timestamp": event_timestamp.isoformat(), # Convert back to string for JSONL loading
                "visitor_id": visitor_id,
                "play_count": play_count,
                "play_rate": None, # Cannot accurately calculate without views/impressions
                "total_watch_time": watch_time,
            })
        else:
            logger.warning(f"Skipping event record due to missing timestamp or media ID: {event}")


    logger.info(f"Finished aggregating {len(aggregated)} fact records.")
    return aggregated


def main():
    """
    Main function to orchestrate the Wistia data pipeline execution.
    Handles incremental loading, data fetching, transformation, and loading to BigQuery.
    """
    # WISTIA_API_TOKEN is read at the top and strip() is applied
    if not WISTIA_API_TOKEN or not GCS_BUCKET_NAME or not PROJECT_ID:
        logger.error("Missing required environment variables (WISTIA_API_TOKEN, GCS_BUCKET_NAME, PROJECT_ID). Exiting.")
        # Exit with a non-zero status code to signal failure to Cloud Run Jobs
        sys.exit(1)

    logger.info("Starting Wistia Data Pipeline Execution...")
    logger.info(f"Project ID: {PROJECT_ID}")
    logger.info(f"GCS Bucket: {GCS_BUCKET_NAME}")

    wistia_token_for_api = WISTIA_API_TOKEN
    headers = {'Authorization': f'Bearer {wistia_token_for_api}'}


    bigquery_client = None
    storage_client = None

    try:
        # Initialize BigQuery client with project ID
        try:
            bigquery_client = bigquery.Client(project=PROJECT_ID)
            logger.info("BigQuery client initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}", exc_info=True)
            raise # Re-raise to be caught by main handler


        # Initialize Storage client (project ID should be picked up from environment)
        try:
            storage_client = storage.Client()
            logger.info("Google Cloud Storage client initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize Google Cloud Storage client: {e}", exc_info=True)
            raise # Re-raise to be caught by main handler


        # --- Step 1: Determine High-Water Mark ---
        last_event_timestamp_bq = get_last_event_timestamp(bigquery_client)
        event_fetch_params = {}
        if last_event_timestamp_bq:
            # Wistia API Events List Doc: 'since': datetime (ISO 8601)
            # Add a small buffer (e.g., 1 second) to the timestamp from BQ to ensure we don't miss edge cases.
            try:
                last_ts_dt = datetime.fromisoformat(last_event_timestamp_bq.replace('Z', '+00:00'))
                # Add a small buffer, e.g., 1 second
                buffered_timestamp_dt = last_ts_dt + timedelta(seconds=1)
                # Convert back to ISO format string for the API
                buffered_timestamp_str = buffered_timestamp_dt.isoformat(timespec='seconds').replace('+00:00', 'Z') # Use Z for UTC
                event_fetch_params['since'] = buffered_timestamp_str
                logger.info(f"Configured API fetch to get events since buffered timestamp: {buffered_timestamp_str}")
            except ValueError as e:
                 logger.error(f"Could not parse last event timestamp from BQ '{last_event_timestamp_bq}' for buffering: {e}", exc_info=True)
                 # If parsing fails, proceed without 'since' parameter (full historical fetch, could be large!)
                 logger.warning("Proceeding without 'since' parameter due to timestamp parsing error. This may fetch large amounts of historical data.")
                 event_fetch_params = {} # Reset params to fetch everything


        # --- Step 2: Extract Data from Wistia API ---

        # Fetch Media Metadata (full list for filtering and duration lookup)
        # This is not incremental based on HWM, always fetches current metadata
        logger.info("Fetching all media metadata from Wistia API List endpoint...")
        # Pass the prepared headers and empty params dictionary
        all_media_metadata = fetch_wistia_data(WISTIA_API_MEDIA_LIST_URL, headers=headers, params={}) # Pass headers, empty params
        logger.info(f"Finished fetching all media metadata. Count: {len(all_media_metadata)}")


        # Filter media metadata to get details only for the specified hashed IDs
        filtered_media_metadata = filter_media_metadata(all_media_metadata)


        # Create a map of hashed_id to duration for aggregation
        media_duration_map = {media.get('hashed_id'): media.get('duration') for media in filtered_media_metadata if media.get('hashed_id')}
        logger.info(f"Created duration map for {len(media_duration_map)} media assets.")
        logger.debug(f"Media Duration Map: {media_duration_map}") # Log map for debugging if needed


        # Fetch Events Data (incrementally using 'since' parameter and filtering by media_id)
        all_filtered_events = []
        # Check if MEDIA_HASHED_IDS is empty or if the filter returned no media
        # Corrected logic to only fetch events if filtered_media_metadata is not empty
        if not filtered_media_metadata:
             if not MEDIA_HASHED_IDS: # This case was already covered, but good to be explicit
                  logger.warning("MEDIA_HASHED_IDS list is empty. No events will be fetched.")
             else: # MEDIA_HASHED_IDS is not empty, but filter found no matches
                  logger.warning(f"No filtered media metadata found matching specified MEDIA_HASHED_IDS ({len(MEDIA_HASHED_IDS)} IDs). No events will be fetched for these IDs.")
        else:
            # --- Use the hashed IDs from the filtered_media_metadata list, not the original MEDIA_HASHED_IDS ---
            # This ensures we only try to fetch events for media that were actually found via the media list API
            media_ids_to_fetch_events = [media.get('hashed_id') for media in filtered_media_metadata if media.get('hashed_id')]
            logger.info(f"Fetching events for each of the {len(media_ids_to_fetch_events)} found specified media assets incrementally...")

            # Define base event parameters with 'since' only once here
            base_event_params = event_fetch_params.copy() # Use the 'since' parameter determined earlier

            for media_hashed_id in media_ids_to_fetch_events: # Iterate over found media IDs
                media_event_params = base_event_params.copy() # Start with the base params (like 'since')
                media_event_params['media_id'] = media_hashed_id # Add media_id filter

                logger.info(f"Fetching events for media '{media_hashed_id}' with params: {media_event_params}")
                try:
                    # Pass the same Authorization headers, and the prepared params
                    media_events = fetch_wistia_data(WISTIA_API_MEDIA_EVENTS_URL, headers=headers, params=media_event_params) # Pass headers and params
                    all_filtered_events.extend(media_events)
                    logger.info(f"Fetched {len(media_events)} events for media '{media_hashed_id}'. Total collected so far: {len(all_filtered_events)}")
                except Exception as e:
                    # Log the specific media ID fetch failure but continue with others
                    logger.error(f"Failed to fetch events for media ID '{media_hashed_id}': {e}", exc_info=True)
                    # Continue loop to try fetching for other media IDs


        logger.info(f"Successfully completed fetching attempts for all specified media assets.")
        logger.info(f"Total events collected for specified media assets: {len(all_filtered_events)}")


        # --- Step 3: Transform and Aggregate Data ---

        # Transform Media Metadata for dim_media (full refresh strategy for now)
        if filtered_media_metadata:
            transformed_media = transform_media_data(filtered_media_metadata)
            logger.info(f"Prepared {len(transformed_media)} records for dim_media.")
        else:
            transformed_media = []
            logger.warning("No filtered media metadata to transform for dim_media.")


        # Aggregate Event Data for fact_media_engagement
        if all_filtered_events:
            aggregated_facts = aggregate_fact_data(all_filtered_events, media_duration_map)
            logger.info(f"Prepared {len(aggregated_facts)} records for fact_media_engagement.")
        else:
             aggregated_facts = []
             logger.info("Skipping load to fact_media_engagement as there was no new aggregated fact data.")


        # --- Step 4: Load Data into BigQuery (via GCS staging) ---

        timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S') # Use UTC time for file naming

        # Load dim_media (using WRITE_TRUNCATE for a full refresh each run - adjust for merge if needed)
        if transformed_media:
            # --- Corrected GCS folder path ---
            dim_media_gcs_folder = 'transformed_data/dim_media'
            dim_media_file_name = f'dim_media_{timestamp_str}.jsonl'
            dim_media_gcs_uri = f'gs://{GCS_BUCKET_NAME}/{dim_media_gcs_folder}/{dim_media_file_name}'
            save_data_to_gcs_jsonl(GCS_BUCKET_NAME, dim_media_gcs_folder, dim_media_file_name, transformed_media)
            load_data_to_bigquery(dim_media_gcs_uri, DIM_MEDIA_TABLE, bigquery_client, SCHEMA_DIM_MEDIA, bigquery.WriteDisposition.WRITE_TRUNCATE)
            logger.info("Loading to dim_media table complete.")
        else:
            logger.warning("Skipping load to dim_media as there was no transformed media metadata.")


        # Load fact_media_engagement (using WRITE_APPEND for incremental data)
        if aggregated_facts:
            # --- Corrected GCS folder path ---
            fact_engagement_gcs_folder = 'transformed_data/fact_media_engagement'
            fact_engagement_file_name = f'fact_media_engagement_{timestamp_str}.jsonl'
            fact_engagement_gcs_uri = f'gs://{GCS_BUCKET_NAME}/{fact_engagement_gcs_folder}/{fact_engagement_file_name}'
            save_data_to_gcs_jsonl(GCS_BUCKET_NAME, fact_engagement_gcs_folder, fact_engagement_file_name, aggregated_facts)
            load_data_to_bigquery(fact_engagement_gcs_uri, FACT_ENGAGEMENT_TABLE, bigquery_client, SCHEMA_FACT_ENGAGEMENT, bigquery.WriteDisposition.WRITE_APPEND)
            logger.info("Loading to fact_media_engagement table complete.")
        else:
            logger.info("Skipping load to fact_media_engagement as there was no new aggregated fact data.")


        logger.info("Wistia Data Pipeline Execution Finished Successfully.")
        sys.exit(0) # Exit with status code 0 to indicate success


    except Exception as e:
        # Catch any unhandled exceptions at the top level
        logger.error("An unhandled critical error occurred during pipeline execution.", exc_info=True) # Log the error and traceback
        # In Cloud Run Jobs, a non-zero exit code signals failure
        sys.exit(1) # Exit with status code 1 to signal failure

# Entry point
if __name__ == "__main__":
    main()