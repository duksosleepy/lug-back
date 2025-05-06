"""
Examples of how to use Loguru in your application code.
"""

from loguru import logger


def log_basic_example():
    """
    Basic logging examples using different log levels.
    """
    # Basic logging with different levels
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")


def log_with_context():
    """
    Logging with additional context and structured data.
    """
    # Add structured data
    logger.info("Processing user request", user_id=12345, action="login")

    # Log with additional context - use bind() to add fields to all subsequent logs
    logger_with_context = logger.bind(request_id="abc-123", user_agent="Chrome")
    logger_with_context.info("Request received")
    logger_with_context.warning("Slow database query detected", query_time=2.3)


def log_with_formatting():
    """
    Logging with custom formatting.
    """
    # Format output for better readability
    logger.info(
        "Processing item {item_id} for user {user}", item_id=42, user="John"
    )

    # Format a list of items
    items = ["apple", "banana", "orange"]
    logger.debug(f"Processing items: {', '.join(items)}")


def log_exceptions():
    """
    Logging exceptions with traceback.
    """
    try:
        # Some code that might raise an exception
        result = 1 / 0
    except Exception as e:
        # Log with exception information
        logger.exception(f"Error occurred: {str(e)}")

        # Alternative way using the error level
        logger.error(f"Division error: {str(e)}", exc_info=True)

        # Log with additional diagnostic data
        logger.opt(exception=True).error(
            "Exception with custom message and data",
            operation="division",
            values=(1, 0),
        )


if __name__ == "__main__":
    # Example usage
    log_basic_example()
    log_with_context()
    log_with_formatting()
    log_exceptions()
