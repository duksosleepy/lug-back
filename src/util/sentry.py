"""
Utility for Sentry error tracking integration.
"""

import logging

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from settings import sentry_settings

logger = logging.getLogger(__name__)


def init():
    """
    Initialize Sentry for error tracking.
    Uses configuration from sentry_settings.
    """
    if not sentry_settings.is_configured():
        logger.info("Sentry integration is disabled or not configured.")
        return

    try:
        logger.info(
            f"Initializing Sentry with environment: {sentry_settings.environment}, "
            f"release: {sentry_settings.release}"
        )

        sentry_sdk.init(
            dsn=sentry_settings.dsn,
            environment=sentry_settings.environment,
            release=sentry_settings.release,
            before_send=sentry_settings.get_before_send(),  # Get function from settings
            send_default_pii=True,
            integrations=[
                FastApiIntegration(),
                SqlalchemyIntegration(),
            ],
            traces_sample_rate=sentry_settings.traces_sample_rate,
        )
        logger.info("Sentry initialization complete")
    except Exception as e:
        logger.error(f"Failed to initialize Sentry: {str(e)}")
