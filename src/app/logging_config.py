"""
Structured logging configuration for CloudWatch compatibility.
"""
import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict
import os

from src.settings import settings


class CloudWatchJSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for CloudWatch Logs.
    Formats logs as JSON for better parsing and querying in CloudWatch.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON string representation of the log record
        """
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields from record
        if hasattr(record, "job_id"):
            log_data["job_id"] = record.job_id
        if hasattr(record, "s3_key"):
            log_data["s3_key"] = record.s3_key
        if hasattr(record, "flow_type"):
            log_data["flow_type"] = record.flow_type
        if hasattr(record, "staging_id"):
            log_data["staging_id"] = record.staging_id
        if hasattr(record, "row_number"):
            log_data["row_number"] = record.row_number
        
        # Add any other extra fields
        for key, value in record.__dict__.items():
            if key not in [
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName", "relativeCreated",
                "thread", "threadName", "exc_info", "exc_text", "stack_info"
            ]:
                if not key.startswith("_"):
                    log_data[key] = value
        
        return json.dumps(log_data)


def setup_logging():
    """
    Configure logging for the application.
    Uses JSON formatting for CloudWatch compatibility in production.
    """
    # Determine log level from environment
    log_level = os.getenv("LOG_LEVEL", settings.LOG_LEVEL).upper()
    
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Remove existing handlers
    root_logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Use JSON formatter for production (CloudWatch), simple formatter for local dev
    use_json = os.getenv("LOG_FORMAT", settings.LOG_FORMAT).lower() == "json"
    
    if use_json:
        formatter = CloudWatchJSONFormatter()
    else:
        # Simple format for local development
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Set levels for third-party libraries
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)
