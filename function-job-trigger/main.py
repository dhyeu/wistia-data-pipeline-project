# main.py for Cloud Function

import os
import logging
import functions_framework
from google.cloud.run_v2 import JobsClient
from google.api_core.exceptions import GoogleAPICallError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment variables (set during deployment)
# These should match your Cloud Run Job details
PROJECT_ID = os.environ.get('PROJECT_ID')
JOB_REGION = os.environ.get('JOB_REGION')
JOB_NAME = os.environ.get('JOB_NAME')

# Validate configuration at startup
config_error = None
if not PROJECT_ID:
    config_error = "PROJECT_ID environment variable is not set."
elif not JOB_REGION:
    config_error = "JOB_REGION environment variable is not set."
elif not JOB_NAME:
    config_error = "JOB_NAME environment variable is not set."

if config_error:
     logger.error(f"Configuration error at startup: {config_error}")
     # Note: We don't exit here, the function will just return an error when triggered

# Initialize the Cloud Run Jobs client
# The client will automatically pick up credentials from the Cloud Function's service account
run_client = None # Initialize to None
client_init_error = None # Initialize client error state

if not config_error: # Only try to initialize client if basic config is present
    try:
        run_client = JobsClient()
        logger.info("Cloud Run Jobs client initialized successfully.")
    except Exception as e:
        client_init_error = f"Failed to initialize Cloud Run Jobs client: {e}"
        logger.error(client_init_error, exc_info=True)


@functions_framework.http
def trigger_cloud_run_job(request):
    """
    HTTP Cloud Function to trigger a Cloud Run Job execution.
    This function is designed to be triggered via HTTP (e.g., by Cloud Scheduler).
    It reads the target Cloud Run Job details from environment variables and
    uses the google-cloud-run client library to execute the job.

    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        A Flask response object indicating success or failure.
    """
    logger.info("Received HTTP trigger for Cloud Run Job execution.")

    # Check for configuration errors identified at startup
    if config_error:
        logger.error(f"Execution blocked due to startup configuration error: {config_error}")
        return (f"Configuration error: {config_error}", 500) # Return HTTP 500 error

    # Check for client initialization errors identified at startup
    if client_init_error:
         logger.error(f"Execution blocked due to client initialization error: {client_init_error}")
         return (f"Internal Server Error: Client initialization failed.", 500) # Return HTTP 500 error

    # Ensure client is available
    if run_client is None:
         # This case should ideally be covered by client_init_error, but as a safeguard
         logger.error("Cloud Run Jobs client is not available (was not initialized).")
         return ("Internal Server Error: Cloud Run Jobs client not available.", 500)


    # Construct the full resource name for the job
    # Format: projects/{project}/locations/{location}/jobs/{job}
    job_resource_name = f"projects/{PROJECT_ID}/locations/{JOB_REGION}/jobs/{JOB_NAME}"
    logger.info(f"Attempting to trigger Cloud Run Job: {job_resource_name}")

    try:
        # Call the run_job method to trigger an execution
        # This is an asynchronous operation; it starts the job and returns
        operation = run_client.run_job(name=job_resource_name)
        logger.info(f"Cloud Run Job execution initiation successful. Operation Name: {operation.operation.name}")

        # You can optionally add logic here to wait for the job execution to complete,
        # but for a simple trigger, just initiating it is usually sufficient.
        # If you needed to know the outcome of the job run, you would wait on the operation.

        return (f"Successfully initiated Cloud Run Job '{JOB_NAME}'. Operation: {operation.operation.name}", 200)

    except GoogleAPICallError as e:
        logger.error(f"Google API error while triggering job: {e}", exc_info=True)
        # Check for specific errors, e.g., permission denied (HTTP Status 403)
        error_code = e.code if hasattr(e, 'code') else 500
        error_message = f"API error triggering job: {e.message}"

        if error_code == 403: # Permission Denied
             error_message = f"Permission denied: The Cloud Function's service account ({os.environ.get('K_SERVICE_ACCOUNT', 'default function SA')}) likely does not have the 'roles/run.invoker' permission on Cloud Run Job '{JOB_NAME}' in region '{JOB_REGION}'. Details: {e.message}"
             logger.error(error_message)
        else:
             logger.error(error_message)

        return (f"Failed to trigger Cloud Run Job: {error_message}", error_code)

    except Exception as e:
        logger.error(f"An unexpected error occurred during job triggering: {e}", exc_info=True)