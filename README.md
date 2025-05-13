Run server
```bash
uv run src/main.py
```


Run job queue
```bash
uv run celery -A src.tasks.worker:celery_app worker --loglevel=info
```
