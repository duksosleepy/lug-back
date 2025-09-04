"""
CRM module for data synchronization between systems.
"""

from .flow import main, process_data
from .tasks import sync_crm_data

__all__ = ["process_data", "main", "sync_crm_data"]
