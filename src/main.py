import uvicorn

from settings import app_settings

if __name__ == "__main__":
    uvicorn.run(
        "api.server:app", host="127.0.0.1", port=8000, reload=app_settings.debug
    )
