"""
Application settings and configuration.
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database
    DATABASE_URL: str
    
    # AWS S3
    CSV_BUCKET_NAME: str
    AWS_REGION: str = "us-east-1"
    
    # AWS SQS
    SQS_QUEUE_URL: str
    SQS_MAX_NUMBER_OF_MESSAGES: int = 1
    SQS_WAIT_TIME_SECONDS: int = 20
    SQS_VISIBILITY_TIMEOUT: int = 300
    
    # Processing
    MAX_RETRIES: int = 3
    RETRY_DELAY_SECONDS: int = 5
    PROGRESS_UPDATE_INTERVAL: int = 10  # Update job_processed_rows every N rows
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
