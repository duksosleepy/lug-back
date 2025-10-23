### Run server
```bash
uv run src/main.py
```


### Run job queue
```bash
uv run celery -A src.tasks.worker:celery_app worker --loglevel=info --concurrency=4

uv run celery -A src.tasks.worker:celery_app beat --loglevel=info
```


### Check if tasks are scheduled
```bash
uv run celery -A src.tasks.worker:celery_app inspect scheduled
```

uv run celery -A src.tasks.worker:celery_app inspect scheduled
uv run celery -A src.tasks.worker:celery_app inspect active
