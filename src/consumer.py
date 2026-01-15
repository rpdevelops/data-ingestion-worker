"""
SQS consumer for processing job messages.
"""
import json
import boto3
import time
from botocore.exceptions import ClientError, BotoCoreError
from sqlalchemy.orm import Session

from src.settings import settings
from src.app.db.database import SessionLocal
from src.app.logging_config import get_logger, setup_logging
from src.processor import Processor

logger = get_logger(__name__)


class SQSConsumer:
    """Consumer for SQS messages."""
    
    def __init__(self):
        """Initialize SQS consumer."""
        self.queue_url = settings.SQS_QUEUE_URL
        self.region = settings.AWS_REGION
        self.max_messages = settings.SQS_MAX_NUMBER_OF_MESSAGES
        self.wait_time = settings.SQS_WAIT_TIME_SECONDS
        self.visibility_timeout = settings.SQS_VISIBILITY_TIMEOUT
        
        try:
            self.sqs_client = boto3.client('sqs', region_name=self.region)
            logger.info(
                "SQS consumer initialized",
                extra={
                    "queue_url": self.queue_url,
                    "region": self.region,
                    "max_messages": self.max_messages,
                    "wait_time": self.wait_time
                }
            )
        except Exception as e:
            logger.error(
                "Failed to initialize SQS client",
                extra={"region": self.region, "error": str(e)},
                exc_info=True
            )
            raise
    
    def start(self) -> None:
        """
        Start consuming messages from SQS queue.
        Runs indefinitely until interrupted.
        """
        logger.info("Starting SQS consumer", extra={"queue_url": self.queue_url})
        
        while True:
            try:
                # Receive messages from queue
                response = self.sqs_client.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=self.max_messages,
                    WaitTimeSeconds=self.wait_time,
                    VisibilityTimeout=self.visibility_timeout,
                    MessageAttributeNames=['All']
                )
                
                messages = response.get('Messages', [])
                
                if not messages:
                    # No messages - continue polling
                    continue
                
                # Process each message
                for message in messages:
                    try:
                        self._process_message(message)
                    except Exception as e:
                        logger.error(
                            "Error processing message",
                            extra={
                                "message_id": message.get('MessageId'),
                                "error": str(e)
                            },
                            exc_info=True
                        )
                        # Message will become visible again after visibility timeout
                        # and can be retried or sent to DLQ
                        continue
                
            except KeyboardInterrupt:
                logger.info("Consumer interrupted by user")
                break
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                logger.error(
                    "SQS client error",
                    extra={
                        "error_code": error_code,
                        "error": str(e)
                    },
                    exc_info=True
                )
                # Wait before retrying
                time.sleep(5)
            except Exception as e:
                logger.error(
                    "Unexpected error in consumer loop",
                    extra={"error": str(e)},
                    exc_info=True
                )
                # Wait before retrying
                time.sleep(5)
    
    def _process_message(self, message: dict) -> None:
        """
        Process a single SQS message.
        
        Args:
            message: SQS message dictionary
        """
        message_id = message.get('MessageId')
        receipt_handle = message.get('ReceiptHandle')
        body = message.get('Body', '{}')
        
        logger.info(
            "Processing message",
            extra={"message_id": message_id}
        )
        
        try:
            # Parse message body
            message_data = json.loads(body)
            job_id = message_data.get('job_id')
            s3_key = message_data.get('s3_key')
            
            if not job_id or not s3_key:
                raise ValueError(f"Invalid message format: missing job_id or s3_key")
            
            logger.info(
                "Message parsed",
                extra={
                    "message_id": message_id,
                    "job_id": job_id,
                    "s3_key": s3_key
                }
            )
            
            # Process job
            db: Session = SessionLocal()
            try:
                processor = Processor(db)
                processor.process_job(job_id, s3_key)
                
                # Delete message from queue on success
                self._delete_message(receipt_handle)
                
                logger.info(
                    "Job processed successfully",
                    extra={
                        "message_id": message_id,
                        "job_id": job_id
                    }
                )
                
            except Exception as e:
                logger.error(
                    "Error processing job",
                    extra={
                        "message_id": message_id,
                        "job_id": job_id,
                        "error": str(e)
                    },
                    exc_info=True
                )
                # Don't delete message - it will become visible again
                # and can be retried or sent to DLQ after max retries
                raise
            finally:
                db.close()
                
        except json.JSONDecodeError as e:
            logger.error(
                "Invalid JSON in message body",
                extra={
                    "message_id": message_id,
                    "body": body,
                    "error": str(e)
                }
            )
            # Delete invalid message to prevent infinite retries
            self._delete_message(receipt_handle)
        except Exception as e:
            logger.error(
                "Error processing message",
                extra={
                    "message_id": message_id,
                    "error": str(e)
                },
                exc_info=True
            )
            # Don't delete message - allow retry
            raise
    
    def _delete_message(self, receipt_handle: str) -> None:
        """
        Delete message from queue.
        
        Args:
            receipt_handle: Message receipt handle
        """
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.debug("Message deleted from queue")
        except Exception as e:
            logger.error(
                "Failed to delete message from queue",
                extra={"error": str(e)},
                exc_info=True
            )


def main():
    """Main entry point for consumer."""
    # Setup logging
    setup_logging()
    
    logger.info("Starting data ingestion worker")
    
    # Create and start consumer
    consumer = SQSConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
