from .offline_processor import DaskExcelProcessor as OfflineProcessor
from .online_processor import DaskExcelProcessor as OnlineProcessor

__all__ = ['OfflineProcessor', 'OnlineProcessor']
