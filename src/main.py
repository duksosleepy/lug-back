import uvicorn


def main():
    # Import modules inside function to avoid import issues
    from settings import app_settings
    from util.logging import setup_logging

    # Set up Loguru for application-wide logging
    logger = setup_logging(
        log_level="DEBUG" if app_settings.debug else "INFO",
        json_logs=False,
        log_to_file=False,
    )

    logger.info(
        f"Starting {app_settings.app_name} in {'debug' if app_settings.debug else 'production'} mode"
    )
    uvicorn.run(
        "api.server:app", host="127.0.0.1", port=8000, reload=app_settings.debug
    )


if __name__ == "__main__":
    main()
