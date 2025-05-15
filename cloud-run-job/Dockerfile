# Use an official Python runtime as a parent image
# Using a specific version (3.10-slim) is recommended for stability
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file from your project to the working directory
COPY requirements.txt .

# Install any needed dependencies specified in requirements.txt
# Use --no-cache-dir to prevent storing cache data
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code from your project to the working directory
COPY main.py .
# COPY your_other_modules/ /app/your_other_modules/ # Copy any other Python files/folders needed

# Run the application when the container starts
# This specifies the command to execute
CMD ["python", "main.py"]
