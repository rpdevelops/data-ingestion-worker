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
        Tries multiple encodings to handle different character sets.
        
        Args:
            s3_key: S3 object key
            
        Returns:
            List of dictionaries, each representing a row
            
        Raises:
            Exception: If file cannot be read with any encoding
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
            
            # Read raw bytes from S3
            raw_content = response['Body'].read()
            
            # Try multiple encodings in order of preference
            encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1", "windows-1252"]
            content = None
            used_encoding = None
            
            for encoding in encodings:
                try:
                    content = raw_content.decode(encoding)
                    used_encoding = encoding
                    logger.debug(
                        "CSV decoded successfully with encoding",
                        extra={"s3_key": s3_key, "encoding": encoding}
                    )
                    break
                except UnicodeDecodeError as e:
                    logger.debug(
                        "Failed to decode CSV with encoding, trying next",
                        extra={
                            "s3_key": s3_key,
                            "encoding": encoding,
                            "error": str(e)
                        }
                    )
                    continue
            
            if content is None:
                raise Exception(
                    f"Failed to decode CSV file with any encoding. "
                    f"Tried: {', '.join(encodings)}"
                )
            
            # Log first few lines for debugging
            lines_preview = content.split('\n')[:3]
            logger.debug(
                "CSV content preview (first 3 lines)",
                extra={
                    "s3_key": s3_key,
                    "encoding": used_encoding,
                    "preview": lines_preview
                }
            )
            
            # Try different delimiters (semicolon first for European format, then comma, then tab)
            # Many CSV files in Portuguese/European format use semicolon
            # Order matters: try semicolon first since it's common in European CSVs
            delimiters = [';', ',', '\t']
            rows = None
            used_delimiter = None
            
            for delimiter in delimiters:
                try:
                    # Reset StringIO for each delimiter attempt
                    content_io = StringIO(content)
                    csv_reader = csv.DictReader(content_io, delimiter=delimiter)
                    
                    # Log the fieldnames detected by DictReader
                    if hasattr(csv_reader, 'fieldnames'):
                        logger.debug(
                            "CSV fieldnames detected",
                            extra={
                                "s3_key": s3_key,
                                "delimiter": repr(delimiter),
                                "fieldnames": csv_reader.fieldnames
                            }
                        )
                    
                    # Process rows and clean up empty fields/values
                    test_rows = []
                    for row_index, row in enumerate(csv_reader):
                        # Log raw row before cleaning (only first row for debugging)
                        if row_index == 0:
                            logger.debug(
                                "Raw CSV row (before cleaning)",
                                extra={
                                    "s3_key": s3_key,
                                    "delimiter": repr(delimiter),
                                    "row_index": row_index,
                                    "raw_row": dict(row),
                                    "raw_row_keys": list(row.keys()),
                                    "raw_row_values": list(row.values())
                                }
                            )
                        
                        # Clean up row - remove fields with empty keys and strip values
                        cleaned_row = {}
                        for key, value in row.items():
                            # Skip fields with empty or None keys (from trailing ;;)
                            if key and key.strip():
                                cleaned_key = key.strip()
                                cleaned_value = value.strip() if value else value
                                # Only add if value is not empty or if it's a valid field
                                cleaned_row[cleaned_key] = cleaned_value
                        
                        # Log cleaned row (only first row for debugging)
                        if row_index == 0:
                            logger.debug(
                                "Cleaned CSV row",
                                extra={
                                    "s3_key": s3_key,
                                    "delimiter": repr(delimiter),
                                    "row_index": row_index,
                                    "cleaned_row": cleaned_row,
                                    "cleaned_row_keys": list(cleaned_row.keys()),
                                    "cleaned_row_values": list(cleaned_row.values())
                                }
                            )
                        
                        # Only add row if it has at least one non-empty value
                        if cleaned_row and any(v and str(v).strip() for v in cleaned_row.values()):
                            test_rows.append(cleaned_row)
                    
                    # Check if we got meaningful data
                    if test_rows:
                        # Count non-empty fields in first row
                        first_row = test_rows[0]
                        field_names = list(first_row.keys())
                        non_empty_count = sum(1 for v in first_row.values() if v and v.strip())
                        
                        # Check if delimiter produced multiple fields (more than 1 field indicates proper parsing)
                        has_multiple_fields = len(field_names) > 1
                        
                        # Verify that field names look reasonable:
                        # - Field names should not contain the other delimiter characters (indicating wrong delimiter was used)
                        # - For ';' delimiter: field names should not contain ',' or multiple ';'
                        # - For ',' delimiter: field names should not contain ';' or multiple ','
                        # - For '\t' delimiter: field names should not contain ',' or ';'
                        if delimiter == ';':
                            # If using ';', field names should not contain ',' (would indicate it's actually comma-separated)
                            field_names_look_valid = not any(',' in str(fn) for fn in field_names if fn)
                        elif delimiter == ',':
                            # If using ',', field names should not contain ';' (would indicate it's actually semicolon-separated)
                            field_names_look_valid = not any(';' in str(fn) for fn in field_names if fn)
                        else:
                            # For tab, check for both comma and semicolon
                            field_names_look_valid = not any(',' in str(fn) or ';' in str(fn) for fn in field_names if fn)
                        
                        # If we have multiple fields AND at least one non-empty value AND field names look valid, this delimiter works
                        if has_multiple_fields and non_empty_count > 0 and field_names_look_valid:
                            rows = test_rows
                            used_delimiter = delimiter
                            logger.debug(
                                "CSV parsed successfully with delimiter",
                                extra={
                                    "s3_key": s3_key,
                                    "delimiter": repr(delimiter),
                                    "row_count": len(rows),
                                    "non_empty_fields_in_first_row": non_empty_count,
                                    "field_names": field_names,
                                    "has_multiple_fields": has_multiple_fields
                                }
                            )
                            break
                        else:
                            logger.debug(
                                "Delimiter did not produce valid results",
                                extra={
                                    "s3_key": s3_key,
                                    "delimiter": repr(delimiter),
                                    "field_count": len(field_names),
                                    "non_empty_count": non_empty_count,
                                    "field_names": field_names,
                                    "has_multiple_fields": has_multiple_fields,
                                    "field_names_look_valid": field_names_look_valid
                                }
                            )
                except Exception as e:
                    logger.debug(
                        "Failed to parse CSV with delimiter, trying next",
                        extra={
                            "s3_key": s3_key,
                            "delimiter": repr(delimiter),
                            "error": str(e)
                        }
                    )
                    continue
            
            # If no delimiter worked, try default (comma) and log the issue
            if rows is None:
                logger.warning(
                    "Could not parse CSV with common delimiters, using default comma",
                    extra={"s3_key": s3_key, "delimiters_tried": [repr(d) for d in delimiters]}
                )
                csv_reader = csv.DictReader(StringIO(content))
                # Clean rows even with default delimiter
                rows = []
                for row in csv_reader:
                    cleaned_row = {}
                    for key, value in row.items():
                        if key and key.strip():
                            cleaned_row[key.strip()] = value.strip() if value else value
                    if cleaned_row and any(v and str(v).strip() for v in cleaned_row.values()):
                        rows.append(cleaned_row)
                used_delimiter = ','
            
            # Validate that we actually read data
            if not rows:
                logger.warning(
                    "CSV file appears to be empty or could not be parsed",
                    extra={
                        "s3_key": s3_key,
                        "encoding": used_encoding,
                        "delimiter": used_delimiter
                    }
                )
            else:
                # Log detailed info about first row
                first_row = rows[0]
                field_info = {
                    key: {
                        "value": value[:50] if value else None,  # Truncate long values
                        "is_empty": not (value and value.strip()) if value else True
                    }
                    for key, value in first_row.items()
                }
                
                non_empty_fields = sum(1 for info in field_info.values() if not info["is_empty"])
                empty_fields = sum(1 for info in field_info.values() if info["is_empty"])
                
                logger.info(
                    "CSV first row analysis",
                    extra={
                        "s3_key": s3_key,
                        "encoding": used_encoding,
                        "delimiter": used_delimiter,
                        "total_fields": len(first_row),
                        "non_empty_fields": non_empty_fields,
                        "empty_fields": empty_fields,
                        "field_names": list(first_row.keys()),
                        "field_info": field_info
                    }
                )
                
                if non_empty_fields == 0:
                    logger.warning(
                        "CSV parsed but all fields appear empty - check delimiter and encoding",
                        extra={
                            "s3_key": s3_key,
                            "encoding": used_encoding,
                            "delimiter": used_delimiter,
                            "first_row_keys": list(first_row.keys()),
                            "raw_first_line_preview": content.split('\n')[0][:100] if content else None
                        }
                    )
            
            logger.info(
                "CSV file read successfully",
                extra={
                    "s3_key": s3_key,
                    "row_count": len(rows),
                    "encoding": used_encoding,
                    "delimiter": used_delimiter
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
