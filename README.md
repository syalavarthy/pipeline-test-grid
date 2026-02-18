# Poisson Grid Image Generator Pipeline

This Airflow pipeline generates a large 2D grid of numbers based on the Poisson distribution and converts it into a black and white JPEG image that is uploaded to S3.

## Overview

The pipeline performs three main tasks:

1. **Generate Poisson Grid**: Creates a 2048x2048 grid of numbers drawn from a Poisson distribution (lambda=3.0), normalized to the range [0, 1]
2. **Create JPEG Image**: Converts the normalized grid into a grayscale JPEG image where pixel brightness corresponds to the grid values
3. **Upload to S3**: Uploads the final JPEG image to a specified S3 bucket

## Pipeline Configuration

- **Grid Size**: 2048x2048 pixels (configurable in `pipeline_logic.py`)
- **Poisson Lambda**: 3.0 (configurable in `pipeline_logic.py`)
- **Image Format**: JPEG with 95% quality
- **Color Mode**: Grayscale (black and white)

## Architecture

This pipeline uses Docker-based task execution:

- **Infrastructure Tasks**:
  - `get_image`: Fetches Docker image URI from AWS Secrets Manager
  - `ecr_login`: Authenticates with ECR and pulls the Docker image

- **Workflow Tasks**:
  - `generate_poisson_grid`: Generates the random grid using NumPy
  - `create_jpeg_image`: Converts grid to JPEG using Pillow
  - `upload_image_to_s3`: Uploads final image to target S3 bucket

## S3 Configuration

The pipeline uses two S3 buckets:

1. **Data Flow Bucket** (`pipelines-data-flow`): Temporary storage for intermediate data (grid array and temporary image)
2. **Output Bucket**: Final destination for the generated JPEG images

### Configuring Output Bucket

Edit the `OUTPUT_S3_BUCKET` environment variable in `src/main.py`:

```python
'OUTPUT_S3_BUCKET': 'your-output-bucket-name'  # Change this!
```

The final image will be stored at:
```
s3://your-output-bucket-name/poisson-grid-images/{run_id}/poisson_grid_2048x2048.jpg
```

## Dependencies

- **boto3**: AWS SDK for S3 operations
- **numpy**: Numerical computing for grid generation
- **Pillow**: Image processing library for JPEG creation

## Running the Pipeline

1. Ensure AWS credentials are configured in Airflow (connection: `aws_default`)
2. Update the `OUTPUT_S3_BUCKET` in `src/main.py`
3. Trigger the DAG manually (schedule is set to `None`)
4. Monitor task execution in the Airflow UI
5. Find your generated image in S3

## Image Properties

- **Format**: JPEG
- **Dimensions**: 2048x2048 pixels
- **Color Space**: Grayscale (8-bit)
- **Quality**: 95%
- **File Size**: ~300-500 KB (typical for Poisson-distributed data)

## Customization

To modify the grid or image properties, edit `src/pipeline_logic.py`:

```python
GRID_SIZE = 2048  # Change to 1024, 4096, etc.
POISSON_LAMBDA = 3.0  # Change distribution parameter
```

For JPEG quality adjustment in `create_jpeg_image()`:

```python
image.save(jpeg_buffer, format='JPEG', quality=95)  # Change quality (1-100)
```

## Mathematical Background

The Poisson distribution generates discrete values representing the number of events occurring in a fixed interval. With lambda=3.0:
- Mean and variance both equal 3.0
- Values are normalized to [0, 1] by dividing by the range
- Darker pixels represent lower values, brighter pixels represent higher values

## Troubleshooting

- **Image not generated**: Check CloudWatch logs for NumPy or Pillow errors
- **S3 upload fails**: Verify bucket permissions and AWS credentials
- **Out of memory**: Reduce `GRID_SIZE` for systems with limited RAM
