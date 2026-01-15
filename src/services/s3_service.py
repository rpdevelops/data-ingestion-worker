"""
S3 service for reading CSV files.
"""
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from io import StringIO
import csv

from src.settings import settings
from src.app.logging_config import get_logger

logger = get_logger(__name__)


class S3Service:
    """Service for S3 operations."""
    
    def __init__(self):
        """Initialize S3 client."""
        self.bucket_name = settings.CSV_BUCKET_NAME
        self.region = settings.AWS_REGION
        
        try:
            self.s3_client = boto3.client('s3', region_name=self.region)
            logger.debug(
                "S3 client initialized",
                extra={"bucket_name": self.bucket_name, "region": self.region}
            )
        except Exception as e:
            logger.error(
                "Failed to initialize S3 client",
                extra={"region": self.region, "error": str(e)},
                exc_info=True
            )
            raise
    
    def read_csv_file(self, s3_key: str) -> list[dict]:
        """
        Read CSV file from S3 and return as list of dictionaries.
        
        Args:
            s3_key: S3 object key
            
        Returns:
            List of dictionaries, each representing a row
            
        Raises:
            Exception: If file cannot be read
        """
        try:
            logger.info(
                "Reading CSV file from S3",
                extra={"bucket_name": self.bucket_name, "s3_key": s3_key}
            )
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            # Read file content
            content = response['Body'].read().decode('utf-8')
            
            # Parse CSV
            csv_reader = csv.DictReader(StringIO(content))
            rows = list(csv_reader)
            
            logger.info(
                "CSV file read successfully",
                extra={
                    "s3_key": s3_key,
                    "row_count": len(rows)
                }
            )
            
            return rows
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(
                "Failed to read CSV file from S3",
                extra={
                    "bucket_name": self.bucket_name,
                    "s3_key": s3_key,
                    "error_code": error_code,
                    "error": str(e)
                },
                exc_info=True
            )
            raise Exception(f"Failed to read CSV from S3: {error_code}")
        
        except Exception as e:
            logger.error(
                "Unexpected error reading CSV file",
                extra={
                    "bucket_name": self.bucket_name,
                    "s3_key": s3_key,
                    "error": str(e)
                },
                exc_info=True
            )
            raise


# Singleton instance
s3_service = S3Service()
