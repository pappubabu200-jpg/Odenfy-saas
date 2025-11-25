
```python
import json
import logging
from backend.app.config import settings
from backend.app.db import SessionLocal
from backend.app.models.usage_log import UsageLog
from backend.app.celery_app import celery_app

try:
    import redis
    REDIS = redis.from_url(settings.REDIS_URL)
except Exception:
    REDIS = None

logger = logging.getLogger(__name__)
USAGE_QUEUE_KEY = "usage:logs:queue"
BATCH_SIZE = 500

@celery_app.task(name="process_usage_logs")
def process_usage_logs():
    """
    Periodically pops logs from Redis and bulk inserts to DB.
    Run this via Celery Beat every 10-30 seconds.
    """
    if not REDIS:
        return

    logs_to_insert = []
    
    # Pop up to BATCH_SIZE items
    for _ in range(BATCH_SIZE):
        # rpop is thread-safe
        raw = REDIS.rpop(USAGE_QUEUE_KEY)
        if not raw:
            break
        try:
            data = json.loads(raw)
            logs_to_insert.append(UsageLog(**data))
        except Exception as e:
            logger.error(f"Failed to parse log entry: {e}")

    if not logs_to_insert:
        return

    # Bulk Insert
    db = SessionLocal()
    try:
        db.bulk_save_objects(logs_to_insert)
        db.commit()
        logger.info(f"Flushed {len(logs_to_insert)} usage logs to DB.")
    except Exception as e:
        logger.exception("Failed to bulk insert usage logs")
        # Optional: Push back to Redis or Dead Letter Queue
    finally:
        db.close()
```
