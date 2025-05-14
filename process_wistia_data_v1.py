import json
import os
from typing import List, Dict, Any, Tuple, Optional
import re # Import regex for parsing filenames
import datetime

# --- Configuration ---
RAW_DATA_DIR = "raw_data" # Directory where raw data is saved

# Regex to extract the timestamp from filenames like 'media_metadata_YYYYMMDD_HHMMSS.json'
TIMESTAMP_REGEX = re.compile(r"_(\d{8}_\d{6})\.json$")

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
        # print(f"✅ Successfully read and parsed JSON from {filepath}") # Keep print less verbose for multiple files
        return data
    except json.JSONDecodeError:
        print(f"❌ Error: Could not decode JSON from {filepath}")
        return None
    except Exception as e:
        print(f"❌ Error reading file {filepath}: {e}")
        return None

# --- File Discovery and Selection ---

def get_files_for_latest_run(raw_data_dir: str) -> Dict[str, Optional[str]]:
    """
    Identifies the filenames for the latest ingestion run based on timestamps.
    Returns a dictionary like {'metadata': 'filename', 'events_gskhw4w4lm': 'filename', ...}
    """
    all_files = os.listdir(raw_data_dir)
    run_timestamps = set()

    # Extract all unique timestamps from the filenames
    for filename in all_files:
        match = TIMESTAMP_REGEX.search(filename)
        if match:
            run_timestamps.add(match.group(1))

    if not run_timestamps:
        print(f"❌ No data files found in '{raw_data_dir}'.")
        return {}

    # Find the latest timestamp
    latest_timestamp = sorted(list(run_timestamps))[-1]
    print(f"✅ Identified latest run timestamp: {latest_timestamp}")

    # Find the specific files matching the latest timestamp
    files_to_process = {
        'metadata': None,
        'events_gskhw4w4lm': None,
        'events_v08dlrgr7v': None # Assuming these are the two media IDs
        # Add other media IDs if needed later
    }

    for filename in all_files:
        if latest_timestamp in filename:
            if filename.startswith("media_metadata_"):
                files_to_process['metadata'] = filename
            elif filename.startswith("events_gskhw4w4lm_"):
                files_to_process['events_gskhw4w4lm'] = filename
            elif filename.startswith("events_v08dlrgr7v_"):
                 files_to_process['events_v08dlrgr7v'] = filename
            # Add logic for other event files if you process more media IDs later


    # Check if expected files for the latest run are found
    missing_files = [key for key, value in files_to_process.items() if value is None]
    if missing_files:
        print(f"⚠️ Warning: Missing expected files for latest run ({latest_timestamp}): {', '.join(missing_files)}")
        # Depending on strictness, you might return {} or None here

    return {key: os.path.join(raw_data_dir, filename) for key, filename in files_to_process.items() if filename is not None}


# --- Main Execution for Transformation Script ---

if __name__ == "__main__":
    print("--- Starting Data Transformation ---")

    # --- Identify and Read Raw Data for the Latest Run ---
    print("--- Reading Raw Data for Latest Run ---")

    raw_files_paths = get_files_for_latest_run(RAW_DATA_DIR)

    raw_media_metadata = None
    raw_events_gskhw4w4lm = None
    raw_events_v08dlrgr7v = None

    if 'metadata' in raw_files_paths:
        raw_media_metadata = read_raw_json_file(raw_files_paths['metadata'])
        if raw_media_metadata is not None:
             print(f"Read {len(raw_media_metadata) if isinstance(raw_media_metadata, list) else 1} media metadata records from latest run.")

    if 'events_gskhw4w4lm' in raw_files_paths:
        raw_events_gskhw4w4lm = read_raw_json_file(raw_files_paths['events_gskhw4w4lm'])
        if raw_events_gskhw4w4lm is not None:
             print(f"Read {len(raw_events_gskhw4w4lm) if isinstance(raw_events_gskhw4w4lm, list) else 1} events for gskhw4w4lm from latest run.")

    if 'events_v08dlrgr7v' in raw_files_paths:
         raw_events_v08dlrgr7v = read_raw_json_file(raw_files_paths['events_v08dlrgr7v'])
         if raw_events_v08dlrgr7v is not None:
              print(f"Read {len(raw_events_v08dlrgr7v) if isinstance(raw_events_v08dlrgr7v, list) else 1} events for v08dlrgr7v from latest run.")

# ... (previous imports and functions) ...

# --- Data Transformation Logic ---

def transform_media_data(raw_metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # ... (function code remains the same) ...
    transformed_data = []
    processing_timestamp = datetime.datetime.now().isoformat()
    # ... (rest of the function) ...
    if raw_metadata is None:
        return transformed_data
    for media_record in raw_metadata:
         project_data = media_record.get("project", {})
         title = media_record.get("name", "")
         channel = None
         if "Facebook" in title or "FB" in title:
             channel = "Facebook"
         elif "Youtube" in title or "YT" in title:
             channel = "YouTube"
         # Add other channel inference logic as needed
         transformed_data.append({
             "media_id": media_record.get("hashed_id"),
             "wistia_id": media_record.get("id"),
             "title": title,
             "url": None, # Will populate from event data later if needed
             "created_at": media_record.get("created"),
             "duration": media_record.get("duration"),
             "type": media_record.get("type"),
             "project_id": project_data.get("id"),
             "project_name": project_data.get("name"),
             "channel": channel,
             "updated_at": media_record.get("updated"),
             "ingestion_timestamp": processing_timestamp,
         })
    return transformed_data


def transform_visitor_data(all_raw_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extracts and transforms unique visitor data from raw event records.
    """
    transformed_data = []
    seen_visitor_keys = set() # Use a set to track unique visitor keys
    processing_timestamp = datetime.datetime.now().isoformat()

    if all_raw_events is None:
        return transformed_data

    for event_record in all_raw_events:
        visitor_key = event_record.get("visitor_key")

        # Only process if visitor_key is present and hasn't been seen yet
        if visitor_key and visitor_key not in seen_visitor_keys:
            # Access nested user_agent_details safely
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
                "ingestion_timestamp": processing_timestamp,
            })
            seen_visitor_keys.add(visitor_key) # Mark this visitor_key as seen

    return transformed_data

# Placeholder for fact table transformation (next step)
# def transform_fact_data(all_raw_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#     pass


# --- Main Execution for Transformation Script ---

if __name__ == "__main__":
    print("--- Starting Data Transformation ---")

    # --- Identify and Read Raw Data for the Latest Run ---
    # ... (code to get file paths and read raw data remains the same) ...
    print("--- Reading Raw Data for Latest Run ---")
    raw_files_paths = get_files_for_latest_run(RAW_DATA_DIR)
    raw_media_metadata = None
    raw_events_gskhw4w4lm = None
    raw_events_v08dlrgr7v = None

    # ... (code to read files remains the same) ...
    if 'metadata' in raw_files_paths:
        raw_media_metadata = read_raw_json_file(raw_files_paths['metadata'])
        if raw_media_metadata is not None:
             print(f"Read {len(raw_media_metadata) if isinstance(raw_media_metadata, list) else 1} media metadata records from latest run.")

    if 'events_gskhw4w4lm' in raw_files_paths:
        raw_events_gskhw4w4lm = read_raw_json_file(raw_files_paths['events_gskhw4w4lm'])
        if raw_events_gskhw4w4lm is not None:
             print(f"Read {len(raw_events_gskhw4w4lm) if isinstance(raw_events_gskhw4w4lm, list) else 1} events for gskhw4w4lm from latest run.")

    if 'events_v08dlrgr7v' in raw_files_paths:
         raw_events_v08dlrgr7v = read_raw_json_file(raw_files_paths['events_v08dlrgr7v'])
         if raw_events_v08dlrgr7v is not None:
              print(f"Read {len(raw_events_v08dlrgr7v) if isinstance(raw_events_v08dlrgr7v, list) else 1} events for v08dlrgr7v from latest run.")


    # --- Transformation Logic ---
    if raw_media_metadata is not None and raw_events_gskhw4w4lm is not None and raw_events_v08dlrgr7v is not None:
        print("\n--- Raw Data Read Successfully ---")
        print("Applying transformation logic...")

        # Transform Media Data
        transformed_media_data = transform_media_data(raw_media_metadata)
        print(f"Transformed {len(transformed_media_data)} records for dim_media.")
        # print("\nSample transformed dim_media data:")
        # print(json.dumps(transformed_media_data[:1], indent=2))


        # Combine events from both media IDs for unified processing
        all_raw_events = raw_events_gskhw4w4lm + raw_events_v08dlrgr7v
        print(f"Total raw event records for transformation: {len(all_raw_events)}")

        # Transform Visitor Data
        transformed_visitor_data = transform_visitor_data(all_raw_events)
        print(f"Transformed {len(transformed_visitor_data)} records for dim_visitor.")
        # print("\nSample transformed dim_visitor data:")
        # print(json.dumps(transformed_visitor_data[:2], indent=2))


        # --- Next: Transform Fact Data ---
        # Transformation for fact_media_engagement will go here.
        # This will involve iterating through all_raw_events and processing/aggregating them.

        print("\n--- Transformation Logic Placeholder ---")
        print("Implement the steps to process raw event data, derive metrics, and prepare data for fact_media_engagement.")


    else:
        print("\n--- Could not read all necessary raw data files for the latest run ---")
        print("Please ensure your ingestion script ran successfully and the expected files are in the 'raw_data' directory.")

    print("\n--- Data Transformation Phase Complete ---")
    print("Next steps: Load transformed data into BigQuery tables.")