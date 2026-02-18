"""
Pipeline logic for Poisson grid image generation.

This module contains all the task logic that runs inside Docker containers.
"""

import os
import json
import logging
import numpy as np
import boto3
from PIL import Image
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get environment variables
task_name = os.environ.get('TASK_NAME')
run_id = os.environ.get('RUN_ID')
s3_data_bucket = os.environ.get('S3_DATA_BUCKET')

# Initialize S3 client
s3_client = boto3.client('s3')

# Configuration
GRID_SIZE = 2048  # 2048x2048 grid for a large image
POISSON_LAMBDA = 3.0  # Lambda parameter for Poisson distribution


def generate_poisson_grid():
    """
    Generate a large 2D grid of numbers from a Poisson distribution.
    Values are normalized to the range [0, 1].
    """
    logger.info(f"Generating {GRID_SIZE}x{GRID_SIZE} Poisson-distributed grid with lambda={POISSON_LAMBDA}")
    
    # Generate Poisson-distributed random numbers
    grid = np.random.poisson(lam=POISSON_LAMBDA, size=(GRID_SIZE, GRID_SIZE))
    
    # Normalize to [0, 1] range
    grid_min = grid.min()
    grid_max = grid.max()
    normalized_grid = (grid - grid_min) / (grid_max - grid_min)
    
    logger.info(f"Grid generated. Min: {grid_min}, Max: {grid_max}")
    logger.info(f"Normalized grid range: [{normalized_grid.min()}, {normalized_grid.max()}]")
    
    # Save grid to S3 as numpy array
    grid_bytes = io.BytesIO()
    np.save(grid_bytes, normalized_grid)
    grid_bytes.seek(0)
    
    s3_key = f"runs/{run_id}/generate_poisson_grid/grid.npy"
    s3_client.put_object(
        Bucket=s3_data_bucket,
        Key=s3_key,
        Body=grid_bytes.getvalue()
    )
    
    logger.info(f"Grid saved to s3://{s3_data_bucket}/{s3_key}")
    
    # Return metadata
    metadata = {
        'grid_size': GRID_SIZE,
        'lambda': POISSON_LAMBDA,
        'original_min': float(grid_min),
        'original_max': float(grid_max),
        's3_key': s3_key
    }
    
    print(json.dumps(metadata))


def create_jpeg_image():
    """
    Load the grid from S3 and convert it to a black and white JPEG image.
    """
    logger.info("Creating JPEG image from Poisson grid")
    
    # Load grid from S3
    s3_key = f"runs/{run_id}/generate_poisson_grid/grid.npy"
    logger.info(f"Loading grid from s3://{s3_data_bucket}/{s3_key}")
    
    response = s3_client.get_object(Bucket=s3_data_bucket, Key=s3_key)
    grid_bytes = io.BytesIO(response['Body'].read())
    grid_bytes.seek(0)
    normalized_grid = np.load(grid_bytes)
    
    logger.info(f"Grid loaded. Shape: {normalized_grid.shape}")
    
    # Convert normalized values [0, 1] to grayscale pixel values [0, 255]
    pixel_values = (normalized_grid * 255).astype(np.uint8)
    
    # Create PIL Image (L mode = 8-bit grayscale)
    image = Image.fromarray(pixel_values, mode='L')
    
    # Save as JPEG to bytes buffer
    jpeg_buffer = io.BytesIO()
    image.save(jpeg_buffer, format='JPEG', quality=95)
    jpeg_buffer.seek(0)
    
    # Save JPEG to S3
    jpeg_s3_key = f"runs/{run_id}/create_jpeg_image/poisson_grid.jpg"
    s3_client.put_object(
        Bucket=s3_data_bucket,
        Key=jpeg_s3_key,
        Body=jpeg_buffer.getvalue(),
        ContentType='image/jpeg'
    )
    
    logger.info(f"JPEG image saved to s3://{s3_data_bucket}/{jpeg_s3_key}")
    
    # Return metadata
    metadata = {
        'image_size': f"{normalized_grid.shape[1]}x{normalized_grid.shape[0]}",
        'format': 'JPEG',
        'temporary_s3_key': jpeg_s3_key
    }
    
    print(json.dumps(metadata))


def upload_image_to_s3():
    """
    Upload the final JPEG image to the target S3 bucket.
    """
    logger.info("Uploading JPEG image to final S3 location")
    
    # Get the temporary image location
    temp_s3_key = f"runs/{run_id}/create_jpeg_image/poisson_grid.jpg"
    logger.info(f"Loading image from s3://{s3_data_bucket}/{temp_s3_key}")
    
    # Download from temporary location
    response = s3_client.get_object(Bucket=s3_data_bucket, Key=temp_s3_key)
    image_data = response['Body'].read()
    
    # Upload to final location
    output_bucket = os.environ.get('OUTPUT_S3_BUCKET', s3_data_bucket)
    final_s3_key = f"poisson-grid-images/{run_id}/poisson_grid_{GRID_SIZE}x{GRID_SIZE}.jpg"
    
    s3_client.put_object(
        Bucket=output_bucket,
        Key=final_s3_key,
        Body=image_data,
        ContentType='image/jpeg',
        Metadata={
            'grid-size': str(GRID_SIZE),
            'lambda': str(POISSON_LAMBDA),
            'run-id': run_id
        }
    )
    
    final_s3_uri = f"s3://{output_bucket}/{final_s3_key}"
    logger.info(f"Final image uploaded to {final_s3_uri}")
    
    # Return final location
    result = {
        'final_s3_uri': final_s3_uri,
        'bucket': output_bucket,
        'key': final_s3_key,
        'size': f"{GRID_SIZE}x{GRID_SIZE}"
    }
    
    print(json.dumps(result))


# Main execution logic based on TASK_NAME
if __name__ == "__main__":
    logger.info(f"Starting task: {task_name}")
    
    if task_name == 'generate_poisson_grid':
        generate_poisson_grid()
    elif task_name == 'create_jpeg_image':
        create_jpeg_image()
    elif task_name == 'upload_image_to_s3':
        upload_image_to_s3()
    else:
        raise ValueError(f"Unknown task name: {task_name}")
    
    logger.info(f"Task {task_name} completed successfully")
