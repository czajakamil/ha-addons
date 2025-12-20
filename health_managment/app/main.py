from fastapi import FastAPI, HTTPException

from .schemas import RootPayload
from .db import get_db_connection
from .processors import process_all_metrics

app = FastAPI(
    title="Health metrics ingest",
    version="0.1.0",
)


@app.post("/health_metric")
async def health_metric(payload: RootPayload):
    conn = get_db_connection()
    try:
        process_all_metrics(payload, conn)
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {"status": "ok"}