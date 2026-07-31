"""Infrastructure handler. Business logic lives in backend.function_logic."""

import json
from typing import Any

from chask_foundation.backend.models import OrchestrationEvent
from backend import FunctionBackend


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)
    if "body" in event:
        body = event["body"]
        event = json.loads(body) if isinstance(body, str) else body
    orchestration_event = OrchestrationEvent.model_validate(event["orchestration_event"])
    result = FunctionBackend(orchestration_event).process_request()
    return {"statusCode": 200, "body": {"status": "ok", "result": {"message": result}}}
