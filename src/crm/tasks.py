"""
Module for CRM data synchronization tasks.
"""

from loguru import logger

from src.crm.flow import process_data
from src.tasks.worker import celery_app


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def sync_crm_data(self):
    """
    Task to synchronize CRM data from source APIs to target system.
    Processes both online and offline data.

    Returns:
        dict: Result of the operation
    """
    try:
        logger.info("Starting CRM data synchronization task")
        result = process_data()
        logger.info(
            f"CRM data sync completed with status: {result.get('status', 'unknown')}"
        )
        return result
    except Exception as e:
        logger.error(f"Error in CRM data sync task: {str(e)}", exc_info=True)
        # Retry task if an error occurs
        self.retry(exc=e)
        return {
            "success": False,
            "message": f"Error in CRM data sync: {str(e)}",
        }


# Export the task
__all__ = ["sync_crm_data"]
