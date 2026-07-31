import json
from types import SimpleNamespace

import pytest

from src.backend.function_logic import FunctionBackend, RequestValidationError


def event(args, pipeline_id=11233):
    return SimpleNamespace(
        orchestration_session_uuid="session-uuid",
        event_id="turn-uuid",
        pipeline_id=pipeline_id,
        access_token="token",
        organization=SimpleNamespace(organization_id="org-uuid"),
        extra_params={"pipeline_id": pipeline_id, "tool_calls": [{"id": "call-1", "args": args}]},
    )


def valid_args():
    return {
        "reason": "SKU inválido",
        "source_node_id": "lookup-node",
        "authorized_root_node_ids": ["lookup-node"],
        "idempotency_key": "required-data:turn-uuid:sku:v1",
        "fields": [{"field_id": "sku_resolution", "label": "Reemplazo", "type": "selection", "required": True, "options": ["replace", "remove"]}],
    }


def test_build_payload_normalizes_types_and_resume_context():
    payload = FunctionBackend(event(valid_args()))._build_payload(valid_args())
    assert payload["fields"][0]["type"] == "seleccion"
    assert payload["resume_context"] == {"operator_turn_uuid": "turn-uuid", "tool_call_id": "call-1"}
    assert payload["pipeline_id"] == 11233


@pytest.mark.parametrize("key", ["reason", "source_node_id", "idempotency_key"])
def test_required_strings_are_rejected(key):
    args = valid_args()
    args[key] = ""
    with pytest.raises(RequestValidationError):
        FunctionBackend(event(args))._build_payload(args)


def test_fields_need_a_required_field():
    args = valid_args()
    args["fields"][0]["required"] = False
    with pytest.raises(RequestValidationError, match="at least one required"):
        FunctionBackend(event(args))._build_payload(args)


def test_selection_needs_options_and_condition_is_validated():
    args = valid_args()
    args["fields"][0]["options"] = []
    with pytest.raises(RequestValidationError, match="options"):
        FunctionBackend(event(args))._build_payload(args)

    args = valid_args()
    args["fields"][0]["required_when"] = {"operator": "one_of", "field_id": "kind", "value": "not-array"}
    with pytest.raises(RequestValidationError, match="array"):
        FunctionBackend(event(args))._build_payload(args)


def test_process_request_calls_existing_endpoint(monkeypatch):
    calls = []
    monkeypatch.setattr("src.backend.function_logic.pipeline_api_manager.call", lambda *a, **kw: calls.append((a, kw)) or {"request": {"required_data_request_uuid": "req-1"}})
    result = FunctionBackend(event(valid_args())).process_request()
    assert json.loads(result)["request"]["required_data_request_uuid"] == "req-1"
    assert calls[0][0] == ("create_runtime_required_data_request",)
    assert calls[0][1]["access_token"] == "token"
