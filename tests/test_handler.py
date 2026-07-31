import importlib
import json
import sys
import types
from types import SimpleNamespace

import pytest


def _install_handler_import_stubs(monkeypatch):
    """Load the infrastructure handler without requiring Lambda layers locally."""
    models = types.ModuleType("chask_foundation.backend.models")

    class ValidatedEvent:
        @classmethod
        def model_validate(cls, value):
            if not isinstance(value, dict):
                raise TypeError("orchestration_event must be an object")
            required = {"orchestration_session_uuid", "event_id", "organization", "access_token", "extra_params"}
            missing = required - value.keys()
            if missing:
                raise ValueError(f"missing event fields: {sorted(missing)}")
            organization = value["organization"]
            if not isinstance(organization, dict) or not organization.get("organization_id"):
                raise ValueError("organization.organization_id is required")
            return SimpleNamespace(
                orchestration_session_uuid=value["orchestration_session_uuid"],
                event_id=value["event_id"],
                pipeline_id=value.get("pipeline_id"),
                access_token=value["access_token"],
                organization=SimpleNamespace(**organization),
                extra_params=value["extra_params"],
            )

    models.OrchestrationEvent = ValidatedEvent
    backend_models = types.ModuleType("chask_foundation.backend")
    backend_models.models = models
    foundation = types.ModuleType("chask_foundation")
    foundation.backend = backend_models
    monkeypatch.setitem(sys.modules, "chask_foundation", foundation)
    monkeypatch.setitem(sys.modules, "chask_foundation.backend", backend_models)
    monkeypatch.setitem(sys.modules, "chask_foundation.backend.models", models)

    from src.backend.function_logic import FunctionBackend

    backend = types.ModuleType("backend")
    backend.FunctionBackend = FunctionBackend
    monkeypatch.setitem(sys.modules, "backend", backend)
    sys.modules.pop("src.handler", None)
    return importlib.import_module("src.handler")


def valid_event():
    return {
        "orchestration_session_uuid": "session-uuid",
        "event_id": "turn-uuid",
        "pipeline_id": 11233,
        "access_token": "token",
        "organization": {"organization_id": "org-uuid"},
        "extra_params": {
            "pipeline_id": 11233,
            "tool_calls": [{
                "id": "call-1",
                "args": {
                    "reason": "SKU inválido",
                    "source_node_id": "lookup-node",
                    "authorized_root_node_ids": ["lookup-node"],
                    "idempotency_key": "required-data:test:sku:v1",
                    "fields": [{
                        "field_id": "sku_resolution",
                        "label": "Reemplazo",
                        "type": "seleccion",
                        "required": True,
                        "options": ["replace", "remove"],
                    }],
                },
            }],
        },
    }


@pytest.mark.parametrize("event_factory", [lambda event: json.dumps(event), lambda event: {"body": json.dumps(event)}])
def test_lambda_handler_accepts_string_and_json_body(monkeypatch, event_factory):
    handler = _install_handler_import_stubs(monkeypatch)
    monkeypatch.setattr(handler.FunctionBackend, "process_request", lambda self: '{"request_uuid":"req-1"}')

    response = handler.lambda_handler(event_factory({"orchestration_event": valid_event()}), None)

    assert response == {
        "statusCode": 200,
        "body": {"status": "ok", "result": {"message": '{"request_uuid":"req-1"}'}},
    }


def test_lambda_handler_requires_orchestration_event_wrapper(monkeypatch):
    handler = _install_handler_import_stubs(monkeypatch)

    with pytest.raises(KeyError, match="orchestration_event"):
        handler.lambda_handler({"event_id": "turn-uuid"}, None)


def test_lambda_handler_validates_realistic_orchestration_event_boundary(monkeypatch):
    handler = _install_handler_import_stubs(monkeypatch)

    event = valid_event()
    del event["organization"]["organization_id"]
    with pytest.raises(ValueError, match="organization.organization_id"):
        handler.lambda_handler({"orchestration_event": event}, None)
