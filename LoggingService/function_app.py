import azure.functions as func
import json
import datetime
import os
import logging
import uuid

from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
CONTAINER_NAME = "logs"


def get_container():
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    return blob_service.get_container_client(CONTAINER_NAME)


@app.route(route="log", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def log_event(req: func.HttpRequest) -> func.HttpResponse:

    try:
        body = req.get_json()

        now = datetime.datetime.utcnow()

        log_record = {
            "event_id": str(uuid.uuid4()),
            "run_id": body.get("run_id"),
            "tenant_id": body.get("tenant_id"),
            "event_type": body.get("event_type"),
            "component": body.get("component"),
            "details": body.get("details"),
            "status": body.get("status"),
            "message": body.get("message"),
            "duration_seconds": body.get("duration_seconds"),
            "event_time": now.isoformat()
        }

        # partition-style blob path
        blob_name = (
            f"{now.strftime('%Y/%m/%d')}/"
            f"{log_record['event_id']}.json"
        )

        container = get_container()

        blob = container.get_blob_client(blob_name)

        blob.upload_blob(
            json.dumps(log_record),
            overwrite=False
        )

        return func.HttpResponse(
            json.dumps({"status": "logged"}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:

        logging.exception("Logging failure")

        return func.HttpResponse(
            json.dumps({
                "status": "failed",
                "error": str(e)
            }),
            mimetype="application/json",
            status_code=500
        )