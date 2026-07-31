"""Thin, validated client for the runtime required-data API."""

from __future__ import annotations

import json
import logging
from typing import Any

from api.pipeline_requests import pipeline_api_manager
from chask_foundation.backend.models import OrchestrationEvent

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

FIELD_TYPE_ALIASES = {
    "text": "texto", "number": "numero", "file": "archivo", "list": "lista",
    "selection": "seleccion", "boolean": "booleano",
}
FIELD_TYPES = {"texto", "numero", "archivo", "lista", "seleccion", "booleano"}
CONDITION_OPERATORS = {"equals", "one_of", "present", "missing"}


class RequestValidationError(ValueError):
    """Raised when tool arguments do not satisfy the runtime contract."""


class FunctionBackend:
    def __init__(self, orchestration_event: OrchestrationEvent):
        self.orchestration_event = orchestration_event

    def process_request(self) -> str:
        args = self._extract_tool_args()
        payload = self._build_payload(args)
        logger.info("Creating runtime required-data request for source node %s", payload["source_node_id"])
        response = pipeline_api_manager.call(
            "create_runtime_required_data_request",
            access_token=self.orchestration_event.access_token,
            organization_id=self.orchestration_event.organization.organization_id,
            **payload,
        )
        if not isinstance(response, dict):
            raise RuntimeError("runtime required-data API returned a non-object response")
        return json.dumps(response, ensure_ascii=False)

    def _build_payload(self, args: dict[str, Any]) -> dict[str, Any]:
        event = self.orchestration_event
        session_uuid = event.orchestration_session_uuid
        if not isinstance(session_uuid, str) or not session_uuid.strip():
            raise RequestValidationError("orchestration_session_uuid is required")

        pipeline_id = getattr(event, "pipeline_id", None)
        if pipeline_id is None:
            pipeline_id = (event.extra_params or {}).get("pipeline_id")
        try:
            pipeline_id = int(pipeline_id)
        except (TypeError, ValueError) as exc:
            raise RequestValidationError("pipeline_id must be an integer") from exc
        if pipeline_id < 1:
            raise RequestValidationError("pipeline_id must be a positive integer")

        schema_revision = self._positive_int(args.get("schema_revision", 1), "schema_revision")
        contract_version = self._positive_int(args.get("contract_version", 1), "contract_version")
        tool_call = self._first_tool_call()
        return {
            "pipeline_id": pipeline_id,
            "orchestration_session_uuid": session_uuid,
            "source_node_id": self._required_string(args, "source_node_id"),
            "reason": self._required_string(args, "reason"),
            "fields": self._validate_fields(args.get("fields")),
            "authorized_root_node_ids": self._string_list(args.get("authorized_root_node_ids"), "authorized_root_node_ids"),
            "resume_context": {
                "operator_turn_uuid": str(event.event_id),
                "tool_call_id": tool_call.get("id"),
            },
            "idempotency_key": self._required_string(args, "idempotency_key"),
            "schema_revision": schema_revision,
            "contract_version": contract_version,
            "_simulation": (event.extra_params or {}).get("_simulation"),
        }

    def _validate_fields(self, raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list) or not raw:
            raise RequestValidationError("fields array cannot be empty")
        fields = []
        for index, item in enumerate(raw):
            if not isinstance(item, dict):
                raise RequestValidationError(f"fields[{index}] must be an object")
            field_type = item.get("type")
            if not isinstance(field_type, str) or not field_type.strip():
                raise RequestValidationError(f"fields[{index}].type is required")
            field_type = FIELD_TYPE_ALIASES.get(field_type.strip(), field_type.strip())
            if field_type not in FIELD_TYPES:
                raise RequestValidationError(f"fields[{index}].type is invalid")
            field = {
                "field_id": self._required_string(item, "field_id", f"fields[{index}]"),
                "label": self._required_string(item, "label", f"fields[{index}]"),
                "type": field_type,
                "required": self._required_bool(item, "required", f"fields[{index}]"),
                "description": item.get("description"),
                "example": item.get("example"),
                "options": self._options(item.get("options"), field_type, index),
                "required_when": self._condition(item.get("required_when"), index),
                "aliases": self._string_array(item.get("aliases"), "aliases", index),
                "validation_hints": self._mapping(item.get("validation_hints"), "validation_hints", index),
            }
            fields.append(field)
        if not any(field["required"] for field in fields):
            raise RequestValidationError("fields must include at least one required field")
        return fields

    @staticmethod
    def _options(value: Any, field_type: str, index: int) -> list[Any]:
        if value is None:
            value = []
        if not isinstance(value, list):
            raise RequestValidationError(f"fields[{index}].options must be an array")
        if field_type == "seleccion" and not value:
            raise RequestValidationError(f"fields[{index}].options cannot be empty for seleccion")
        return list(value)

    def _condition(self, value: Any, index: int) -> Any:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise RequestValidationError(f"fields[{index}].required_when must be an object or null")
        self._condition_node(value, f"fields[{index}].required_when")
        return value

    def _condition_node(self, value: dict[str, Any], path: str) -> None:
        compositions = [key for key in ("all", "any") if key in value]
        if compositions:
            if len(compositions) != 1 or not isinstance(value[compositions[0]], list) or not value[compositions[0]]:
                raise RequestValidationError(f"{path} composition must be a non-empty array")
            for index, child in enumerate(value[compositions[0]]):
                if not isinstance(child, dict):
                    raise RequestValidationError(f"{path}.{compositions[0]}[{index}] must be an object")
                self._condition_node(child, f"{path}.{compositions[0]}[{index}]")
            return
        operator = value.get("operator")
        if operator not in CONDITION_OPERATORS:
            raise RequestValidationError(f"{path}.operator is invalid")
        if not isinstance(value.get("field_id"), str) or not value["field_id"].strip():
            raise RequestValidationError(f"{path}.field_id is required")
        if operator in {"equals", "one_of"} and "value" not in value:
            raise RequestValidationError(f"{path}.value is required")
        if operator == "one_of" and not isinstance(value.get("value"), list):
            raise RequestValidationError(f"{path}.value must be an array")

    @staticmethod
    def _required_string(mapping: dict[str, Any], key: str, prefix: str = "") -> str:
        value = mapping.get(key)
        if not isinstance(value, str) or not value.strip():
            raise RequestValidationError(f"{prefix + '.' if prefix else ''}{key} is required")
        return value.strip()

    @staticmethod
    def _required_bool(mapping: dict[str, Any], key: str, prefix: str) -> bool:
        if type(mapping.get(key)) is not bool:
            raise RequestValidationError(f"{prefix}.{key} must be boolean")
        return mapping[key]

    @staticmethod
    def _positive_int(value: Any, name: str) -> int:
        if type(value) is not int or value < 1:
            raise RequestValidationError(f"{name} must be a positive integer")
        return value

    @staticmethod
    def _string_list(value: Any, name: str) -> list[str]:
        if not isinstance(value, list) or not value or not all(isinstance(item, str) and item.strip() for item in value):
            raise RequestValidationError(f"{name} must be a non-empty array of strings")
        return [item.strip() for item in value]

    @staticmethod
    def _string_array(value: Any, name: str, index: int) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise RequestValidationError(f"fields[{index}].{name} must be an array of strings")
        return list(value)

    @staticmethod
    def _mapping(value: Any, name: str, index: int) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise RequestValidationError(f"fields[{index}].{name} must be an object")
        return dict(value)

    def _first_tool_call(self) -> dict[str, Any]:
        calls = (self.orchestration_event.extra_params or {}).get("tool_calls") or []
        return calls[0] if calls and isinstance(calls[0], dict) else {}

    def _extract_tool_args(self) -> dict[str, Any]:
        return self._first_tool_call().get("args") or {}
