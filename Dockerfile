# Use a lightweight Python base image
FROM python:3.9-slim

# Set the working directory inside the container
# All subsequent commands will run relative to this directory
WORKDIR /app

# Copy the requirements file and install dependencies
# Copying requirements.txt first allows Docker to cache this layer,
# making subsequent builds faster if only your Python code changes.
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python application code into the container
# Ensure these filenames match your project structure
COPY ingest_wistia.py .
COPY process_wistia_data.py .

# Define the command that should be run when the container starts
# Using ENTRYPOINT with the exec form is generally recommended for init systems
ENTRYPOINT ["python", "process_wistia_data.py"]

# Note: No need to expose ports as this is a background job triggered by Pub/Sub