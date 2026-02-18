FROM python:3.13-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code
COPY src/ ./src/

# Note: CMD/ENTRYPOINT not specified - command passed via DockerOperator
