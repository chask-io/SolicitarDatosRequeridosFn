import json
import sys
import types
from types import SimpleNamespace

import pytest

# The API and foundation layers are injected by Lambda. Stub only their import
# boundary so these unit tests can run without installing deployment layers.
pipeline_requests = types.ModuleType("api.pipeline_requests")
pipeline_requests.pipeline_api_manager = SimpleNamespace(call=None)
orchestrator_requests = types.ModuleType("api.orchestrator_requests")
orchestrator_requests.orchestrator_api_manager = SimpleNamespace(call=None)
api_package = types.ModuleType("api")
api_package.pipeline_requests = pipeline_requests
api_package.orchestrator_requests = orchestrator_requests
foundation_models = types.ModuleType("chask_foundation.backend.models")
foundation_models.OrchestrationEvent = object
foundation_backend = types.ModuleType("chask_foundation.backend")
foundation_backend.models = foundation_models
foundation_package = types.ModuleType("chask_foundation")
foundation_package.backend = foundation_backend
sys.modules.setdefault("api", api_package)
sys.modules.setdefault("api.pipeline_requests", pipeline_requests)
sys.modules.setdefault("api.orchestrator_requests", orchestrator_requests)
sys.modules.setdefault("chask_foundation", foundation_package)
sys.modules.setdefault("chask_foundation.backend", foundation_backend)
sys.modules.setdefault("chask_foundation.backend.models", foundation_models)

from src.backend.function_logic import FunctionBackend, RequestValidationError


class TestEvent(SimpleNamespace):
    def model_copy(self, deep=True):
        return TestEvent(**self.__dict__)

    def model_dump(self):
        return dict(self.__dict__)


def event(args, pipeline_id=11233, simulation=None):
    extra_params = {
        "pipeline_id": pipeline_id,
        "tool_calls": [{"id": "call-1", "args": args}],
    }
    if simulation is not None:
        extra_params["_simulation"] = simulation
    return TestEvent(
        orchestration_session_uuid="session-uuid",
        event_id="turn-uuid",
        pipeline_id=pipeline_id,
        access_token="token",
        organization=SimpleNamespace(organization_id="org-uuid"),
        extra_params=extra_params,
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


def test_build_payload_accepts_documented_sku_example():
    args = valid_args()
    args["fields"] = [
        {"field_id": "sku_1", "label": "SKU 1", "type": "texto", "required": True},
        {"field_id": "sku_3", "label": "SKU 3", "type": "texto", "required": True},
    ]

    payload = FunctionBackend(event(args))._build_payload(args)

    assert [(field["field_id"], field["type"]) for field in payload["fields"]] == [
        ("sku_1", "texto"),
        ("sku_3", "texto"),
    ]


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


def test_validation_does_not_create_request_or_response_event(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "src.backend.function_logic.pipeline_api_manager.call",
        lambda *a, **kw: calls.append(("pipeline", a, kw)),
    )
    monkeypatch.setattr(
        "src.backend.function_logic.orchestrator_api_manager.call",
        lambda *a, **kw: calls.append(("orchestrator", a, kw)),
    )
    args = valid_args()
    args["fields"] = []

    with pytest.raises(RequestValidationError, match="fields array cannot be empty"):
        FunctionBackend(event(args)).process_request()

    assert calls == []


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
    response_calls = []
    monkeypatch.setattr(
        "src.backend.function_logic.orchestrator_api_manager.call",
        lambda *a, **kw: response_calls.append((a, kw)) or (
            {"status_code": 201, "uuid": "response-1"}
            if a[0] == "evolve_event" else {}
        ),
    )
    simulation = {"is_simulation": True, "scenario_id": "scenario-1", "run_id": "run-1"}
    backend = FunctionBackend(event(valid_args(), simulation=simulation))
    result = backend.process_request()
    assert json.loads(result)["request"]["required_data_request_uuid"] == "req-1"
    assert backend.response_event_sent is True
    assert len(calls) == 1
    assert calls[0][0] == ("create_runtime_required_data_request",)
    assert calls[0][1] == {
        "access_token": "token",
        "organization_id": "org-uuid",
        "pipeline_id": 11233,
        "orchestration_session_uuid": "session-uuid",
        "source_node_id": "lookup-node",
        "reason": "SKU inválido",
        "fields": [{
            "field_id": "sku_resolution",
            "label": "Reemplazo",
            "type": "seleccion",
            "required": True,
            "description": None,
            "example": None,
            "options": ["replace", "remove"],
            "required_when": None,
            "aliases": [],
            "validation_hints": {},
        }],
        "authorized_root_node_ids": ["lookup-node"],
        "resume_context": {"operator_turn_uuid": "turn-uuid", "tool_call_id": "call-1"},
        "idempotency_key": "required-data:turn-uuid:sku:v1",
        "schema_revision": 1,
        "contract_version": 1,
        "_simulation": simulation,
    }
    assert response_calls[0][0] == ("evolve_event",)
    assert response_calls[0][1]["event_type"] == "function_call_response"
    assert response_calls[0][1]["source"] == "agent"
    assert response_calls[0][1]["target"] == "orchestrator"
    assert response_calls[0][1]["extra_params"] == {
        "tool_call_id": "call-1",
        "tool_name": None,
        "is_error": False,
        "original_source": "agent",
    }
    assert response_calls[1][0] == ("forward_oe_to_kafka",)
    assert response_calls[1][1]["orchestration_event"]["event_type"] == "function_call_response"
