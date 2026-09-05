#!/usr/bin/env python3
"""Run repeated real-model Mitsuro harness acceptance cycles.

The runner is intentionally dependency-free so it can execute beside the Honey
server. It exercises the actual HTTP/SSE surface, model provider, coding tools,
artifact persistence, process registry, and follow-up history rather than mocks.
"""

from __future__ import annotations

import argparse
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen
import uuid


class FailureClassification(str, Enum):
    """Stable acceptance result classes used by retry and reporting policy."""

    EXTERNAL_PROVIDER_TRANSIENT = "external_provider_transient"
    PRODUCT_OR_CONFIGURATION_TERMINAL = "product_or_configuration_terminal"


class AcceptanceFailure(RuntimeError):
    """A classified behavioral failure in an end-to-end acceptance cycle."""

    def __init__(
        self,
        message: str,
        classification: FailureClassification = (
            FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL
        ),
    ) -> None:
        super().__init__(message)
        self.classification = classification


_PROVIDER_ATTRIBUTION_MARKERS = (
    "api error:",
    "provider error",
    "provider_error",
    '"source": "provider"',
    '"origin": "provider"',
)
_HTTP_STATUS_PATTERN = re.compile(
    r"(?:api error:|http(?: status)?|status(?: code)?[:=]?)\s*"
    r"(429|502|503|504)\b",
    re.IGNORECASE,
)
_BARE_TRANSIENT_STATUS_PATTERN = re.compile(r"\b(429|502|503|504)\b")
_RATE_LIMIT_MARKERS = (
    "capacity",
    "rate limit",
    "rate_limit",
    "too many requests",
    "resource exhausted",
    "resource_exhausted",
    "resource has been exhausted",
)


def has_explicit_provider_attribution(value: Any) -> bool:
    if isinstance(value, dict):
        if value.get("failure_category") == "provider_error":
            return True
        if value.get("source") == "provider" or value.get("origin") == "provider":
            return True
        if isinstance(value.get("provider"), str) and bool(value["provider"].strip()):
            return True
        return any(has_explicit_provider_attribution(item) for item in value.values())
    if isinstance(value, list):
        return any(has_explicit_provider_attribution(item) for item in value)
    return False


def classify_failure_details(
    details: Any,
    *,
    http_status: int | None = None,
    provider_attributed: bool = False,
) -> FailureClassification:
    """Classify only explicit provider capacity/upstream failures as retryable.

    Authentication, payment, malformed requests, tool failures, local transport
    failures, and ambiguous HTTP errors intentionally remain terminal. This
    prevents a broken product or configuration from being hidden by the loop.
    """
    serialized = (
        details
        if isinstance(details, str)
        else json.dumps(details, sort_keys=True, default=str)
    )
    lowered = serialized.lower()

    if http_status in {400, 401, 402, 403}:
        return FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL

    attributed = (
        provider_attributed
        or has_explicit_provider_attribution(details)
        or any(marker in lowered for marker in _PROVIDER_ATTRIBUTION_MARKERS)
    )
    if not attributed:
        return FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL

    statuses: set[int] = set()
    if http_status is not None:
        statuses.add(http_status)
    statuses.update(int(match.group(1)) for match in _HTTP_STATUS_PATTERN.finditer(serialized))
    if attributed:
        statuses.update(
            int(match.group(1))
            for match in _BARE_TRANSIENT_STATUS_PATTERN.finditer(serialized)
        )

    if any(status in {502, 503, 504} for status in statuses):
        return FailureClassification.EXTERNAL_PROVIDER_TRANSIENT
    if 429 in statuses and any(marker in lowered for marker in _RATE_LIMIT_MARKERS):
        return FailureClassification.EXTERNAL_PROVIDER_TRANSIENT
    return FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL


def failure_classification(error: BaseException) -> FailureClassification:
    if isinstance(error, AcceptanceFailure):
        return error.classification
    return FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL


def failure_status(error: BaseException) -> str:
    classification = failure_classification(error)
    if classification is FailureClassification.EXTERNAL_PROVIDER_TRANSIENT:
        return classification.value
    return "fail"


def final_process_disposition(result: dict[str, Any]) -> str:
    """Describe the verified final-cycle process state without overclaiming."""
    return "retained" if result.get("process_retained") is True else "cleaned"


def classified_failure(
    message: str,
    details: Any,
    *,
    http_status: int | None = None,
    provider_attributed: bool = False,
) -> AcceptanceFailure:
    return AcceptanceFailure(
        message,
        classify_failure_details(
            details,
            http_status=http_status,
            provider_attributed=provider_attributed,
        ),
    )


class MitsuroApi:
    def __init__(self, base_url: str, timeout: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.agent_control_client_id = f"acceptance-{uuid.uuid4().hex}"

    def json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        headers: dict[str, str] | None = None,
    ) -> Any:
        data = None if payload is None else json.dumps(payload).encode()
        request_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if headers is not None:
            request_headers.update(headers)
        request = Request(
            f"{self.base_url}{path}",
            data=data,
            method=method,
            headers=request_headers,
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                body = response.read()
        except HTTPError as error:
            body = error.read().decode(errors="replace")
            raise classified_failure(
                f"{method} {path} returned HTTP {error.code}: {body}",
                body,
                http_status=error.code,
            ) from error
        except URLError as error:
            raise AcceptanceFailure(f"{method} {path} failed: {error}") from error
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError as error:
            raise AcceptanceFailure(
                f"{method} {path} returned non-JSON: {body[:500]!r}"
            ) from error

    def agent_mutation_headers(self, session_id: str) -> dict[str, str]:
        """Renew this runner's non-forcing Code Agent control authority.

        The server rejects the claim if a real client controls the session. The
        fresh token exists only long enough to build this request header; it is
        never cached, logged, or included in reports. Re-claiming before each
        mutation also keeps long acceptance cycles from relying on an expired
        15-second lease.
        """
        response = self.json_request(
            "POST",
            f"/api/sessions/{quote(session_id, safe='')}/control",
            {"client_id": self.agent_control_client_id, "force": False},
        )
        lease = response.get("lease") if isinstance(response, dict) else None
        token = lease.get("lease_token") if isinstance(lease, dict) else None
        resource_id = lease.get("resource_id") if isinstance(lease, dict) else None
        client_id = lease.get("client_id") if isinstance(lease, dict) else None
        require(
            isinstance(token, str) and bool(token),
            f"Code Agent control claim returned no lease token for {session_id}",
        )
        require(
            resource_id == session_id and client_id == self.agent_control_client_id,
            f"Code Agent control claim drifted for {session_id}",
        )
        return {"x-mitsuro-control-lease": token}

    def chat_incremental(
        self,
        payload: dict[str, Any],
        on_event: Callable[[dict[str, Any], list[dict[str, Any]]], bool] | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Read chat SSE events, optionally closing the client when asked.

        The callback runs after each parsed event. Returning true deliberately
        closes the HTTP response and leaves the server-side run to continue.
        The boolean return value records whether that deliberate disconnect
        happened.
        """
        headers = {
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
        }
        # Control leases are a Code-only compatibility contract. Chat and Hive
        # requests retain their prior behavior and never touch the Code-only
        # control endpoint.
        if payload.get("session_type") == "code":
            session_id = payload.get("session_id")
            require(
                isinstance(session_id, str) and bool(session_id),
                "Code Agent chat request requires a session_id",
            )
            headers.update(self.agent_mutation_headers(session_id))
        request = Request(
            f"{self.base_url}/api/chat",
            data=json.dumps(payload).encode(),
            method="POST",
            headers=headers,
        )
        events: list[dict[str, Any]] = []
        finish_seen = False
        disconnected = False
        try:
            with urlopen(request, timeout=self.timeout) as response:
                for raw_line in response:
                    line = raw_line.decode(errors="replace").strip()
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if not data or data == "[DONE]":
                        continue
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError as error:
                        raise AcceptanceFailure(f"malformed SSE data: {data[:500]}") from error
                    if finish_seen:
                        raise AcceptanceFailure(
                            f"event {event.get('type')} arrived after terminal finish"
                        )
                    events.append(event)
                    if event.get("type") == "finish":
                        finish_seen = True
                    if on_event is not None and on_event(event, events):
                        disconnected = True
                        break
        except HTTPError as error:
            body = error.read().decode(errors="replace")
            raise classified_failure(
                f"chat returned HTTP {error.code}: {body}",
                body,
                http_status=error.code,
            ) from error
        except URLError as error:
            raise AcceptanceFailure(f"chat stream failed: {error}") from error
        return events, disconnected

    def chat(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        events, disconnected = self.chat_incremental(payload)
        require(not disconnected, "chat stream disconnected unexpectedly")
        return events


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AcceptanceFailure(message)


def validate_candidate_base_url(value: str) -> str:
    parsed = urlparse(value)
    try:
        port = parsed.port
    except ValueError as error:
        raise AcceptanceFailure(f"candidate base URL had an invalid port: {value}") from error
    require(parsed.scheme == "http", f"candidate base URL must use http: {value}")
    require(
        parsed.hostname in {"127.0.0.1", "localhost"},
        f"candidate base URL must be loopback-only: {value}",
    )
    require(port is not None, f"candidate base URL must include an explicit port: {value}")
    require(port != 3000, "refusing to run acceptance against production port 3000")
    require(parsed.username is None and parsed.password is None, "base URL must not embed credentials")
    require(parsed.path in {"", "/"}, f"candidate base URL must not include a path: {value}")
    require(not parsed.query and not parsed.fragment, f"candidate base URL must be plain: {value}")
    return f"http://{parsed.hostname}:{port}"


def select_stable_exact_model(
    api: MitsuroApi,
    model_id: str,
    *,
    provider_id: str = "grok",
    timeout: float = 30.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    previous_identity: str | None = None
    stable_reads = 0
    latest: Any = None
    while time.monotonic() < deadline:
        response = api.json_request("GET", "/api/models")
        models = response.get("models") if isinstance(response, dict) else None
        require(isinstance(models, list), f"model catalog was malformed: {response}")
        candidates = [
            model
            for model in models
            if isinstance(model, dict)
            and model.get("id") == model_id
            and model.get("provider_id") == provider_id
        ]
        latest = candidates
        if len(candidates) == 1:
            model = candidates[0]
            key = model.get("key")
            revision = model.get("catalog_revision")
            valid = (
                isinstance(key, dict)
                and key.get("provider") == provider_id
                and key.get("model_id") == model_id
                and isinstance(revision, str)
                and bool(revision.strip())
                and model.get("catalog_source") == "live_dynamic"
                and model.get("supports_tools") is True
            )
            if valid:
                identity = json.dumps(
                    {
                        "key": key,
                        "catalog_revision": revision,
                        "catalog_source": model.get("catalog_source"),
                        "context_window": model.get("context_window"),
                        "max_output": model.get("max_output"),
                        "supports_tools": model.get("supports_tools"),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                stable_reads = stable_reads + 1 if identity == previous_identity else 1
                previous_identity = identity
                if stable_reads >= 2:
                    return model
            else:
                previous_identity = None
                stable_reads = 0
        else:
            previous_identity = None
            stable_reads = 0
        time.sleep(0.25)
    raise AcceptanceFailure(
        f"exact {provider_id} model {model_id!r} did not reach two stable live catalog reads: {latest}"
    )


def event_text(events: list[dict[str, Any]]) -> str:
    return "".join(
        str(event.get("delta", ""))
        for event in events
        if event.get("type") in {"text_delta", "text_delta_with_citations"}
    )


def tool_calls(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [event for event in events if event.get("type") == "tool_call_complete"]


_CORE_TOOL_LIFECYCLE_TYPES = (
    "tool_call_start",
    "tool_call_complete",
    "tool_executing",
    "tool_result",
)


def validate_complete_tool_lifecycles(
    events: list[dict[str, Any]],
    label: str,
    *,
    exact_calls: int | None = None,
    expected_name: str | None = None,
) -> list[str]:
    """Require one balanced, ordered lifecycle for every completed tool call.

    `tool_executing` is also the five-second liveness heartbeat for a long tool,
    so a call may emit it more than once. The identity-bearing start, complete,
    and result events remain unique.
    """
    unique_lifecycle: dict[str, dict[str, tuple[int, dict[str, Any]]]] = {
        event_type: {}
        for event_type in _CORE_TOOL_LIFECYCLE_TYPES
        if event_type != "tool_executing"
    }
    executing: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for index, event in enumerate(events):
        event_type = event.get("type")
        if event_type not in _CORE_TOOL_LIFECYCLE_TYPES:
            continue
        call_id = event.get("id")
        require(
            isinstance(call_id, str) and bool(call_id),
            f"{label}: {event_type} had no stable id: {event}",
        )
        if event_type == "tool_executing":
            executing.setdefault(call_id, []).append((index, event))
            continue
        require(
            call_id not in unique_lifecycle[event_type],
            f"{label}: duplicate {event_type} for {call_id}",
        )
        unique_lifecycle[event_type][call_id] = (index, event)

    id_sets = {
        event_type: set(events_by_id)
        for event_type, events_by_id in unique_lifecycle.items()
    }
    completed_ids = id_sets["tool_call_complete"]
    for event_type, event_ids in id_sets.items():
        require(
            event_ids == completed_ids,
            f"{label}: unbalanced tool lifecycle for {event_type}; "
            f"completed={sorted(completed_ids)}, observed={sorted(event_ids)}",
        )
    require(
        set(executing) == completed_ids,
        f"{label}: unbalanced tool lifecycle for tool_executing; "
        f"completed={sorted(completed_ids)}, observed={sorted(executing)}",
    )
    if exact_calls is not None:
        require(
            len(completed_ids) == exact_calls,
            f"{label}: expected exactly {exact_calls} tool calls, got "
            f"{len(completed_ids)} ({sorted(completed_ids)})",
        )

    ordered_ids = [
        call_id
        for call_id, _ in sorted(
            unique_lifecycle["tool_call_complete"].items(),
            key=lambda item: item[1][0],
        )
    ]
    for call_id in ordered_ids:
        executing_events = executing[call_id]
        positions = {
            event_type: unique_lifecycle[event_type][call_id][0]
            for event_type in unique_lifecycle
        }
        positions["tool_executing_first"] = executing_events[0][0]
        positions["tool_executing_last"] = executing_events[-1][0]
        require(
            positions["tool_call_start"]
            < positions["tool_call_complete"]
            < positions["tool_executing_first"]
            <= positions["tool_executing_last"]
            < positions["tool_result"],
            f"{label}: tool lifecycle for {call_id} was out of order: {positions}",
        )
        complete = unique_lifecycle["tool_call_complete"][call_id][1]
        name = complete.get("name")
        require(
            isinstance(name, str) and bool(name),
            f"{label}: completed tool {call_id} had no name: {complete}",
        )
        start = unique_lifecycle["tool_call_start"][call_id][1]
        require(
            start.get("name") == name,
            f"{label}: tool_call_start name for {call_id} did not match "
            f"{name!r}: {start}",
        )
        for _, event in executing_events:
            require(
                event.get("name") == name,
                f"{label}: tool_executing name for {call_id} did not match "
                f"{name!r}: {event}",
            )
        if expected_name is not None:
            require(
                name == expected_name,
                f"{label}: expected {expected_name}, got {name} for {call_id}",
            )
    return ordered_ids


def validate_trace_tool_lifecycles(
    trace_events: list[dict[str, Any]],
    label: str,
    *,
    exact_calls: int | None = None,
    expected_name: str | None = None,
) -> list[str]:
    """Apply stream lifecycle invariants to the canonical persisted trace."""
    normalized: list[dict[str, Any]] = []
    for event in trace_events:
        event_type = event.get("event_type")
        if event_type not in _CORE_TOOL_LIFECYCLE_TYPES:
            continue
        payload = event.get("payload")
        require(
            isinstance(payload, dict),
            f"{label}: trace {event_type} payload was malformed: {event}",
        )
        normalized.append({**payload, "type": event_type})
    return validate_complete_tool_lifecycles(
        normalized,
        label,
        exact_calls=exact_calls,
        expected_name=expected_name,
    )


_TOOL_LIFECYCLE_EVENT_TYPES = {
    "awaiting_input",
    "classifier_decision",
    "mode_change",
    "plan_complete",
    "plan_update",
    "user_message",
    "web_fetch_result",
    "web_search_results",
}
_TOOL_LIFECYCLE_PREFIXES = (
    "agent_background_",
    "delegated_",
    "server_tool_",
    "teammate_",
    "tool_",
)


def tool_lifecycle_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return every tool, server-tool, or delegated execution lifecycle event."""
    return [
        event
        for event in events
        if isinstance(event.get("type"), str)
        and (
            event["type"] in _TOOL_LIFECYCLE_EVENT_TYPES
            or event["type"].startswith(_TOOL_LIFECYCLE_PREFIXES)
        )
    ]


def validate_usage_events(events: list[dict[str, Any]], label: str) -> None:
    """Validate normalized usage snapshots when the provider exposes them."""
    fields = (
        "prompt_tokens",
        "input_tokens",
        "completion_tokens",
        "reasoning_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "total_tokens",
    )
    for index, event in enumerate(events):
        if event.get("type") != "usage":
            continue
        values: dict[str, int] = {}
        for field in fields:
            value = event.get(field)
            require(
                isinstance(value, int) and not isinstance(value, bool) and value >= 0,
                f"{label}: usage[{index}].{field} was not a non-negative integer: {value!r}",
            )
            values[field] = value

        represented_input = (
            values["prompt_tokens"]
            + values["cache_creation_input_tokens"]
            + values["cache_read_input_tokens"]
        )
        require(
            values["input_tokens"] == represented_input,
            f"{label}: usage[{index}] input buckets were inconsistent: {event}",
        )
        require(
            values["reasoning_tokens"] <= values["completion_tokens"],
            f"{label}: usage[{index}] reasoning exceeded completion: {event}",
        )
        require(
            values["total_tokens"]
            >= values["input_tokens"] + values["completion_tokens"],
            f"{label}: usage[{index}] total was smaller than input + completion: {event}",
        )
        require(
            values["total_tokens"] > 0,
            f"{label}: usage[{index}] exposed an all-zero token snapshot: {event}",
        )


def validate_stream(
    events: list[dict[str, Any]],
    label: str,
    *,
    expect_tools: bool | None = None,
) -> str:
    types = [event.get("type") for event in events]
    errors = [
        event
        for event in events
        if event.get("type") in {"error", "server_tool_error", "lagged"}
        or (event.get("type") == "tool_result" and event.get("is_error") is True)
    ]
    finishes = [event for event in events if event.get("type") == "finish"]
    calls = tool_calls(events)
    text = event_text(events).strip()

    if errors:
        # Tool/server/backpressure failures are always product-terminal even if
        # their payload happens to quote an upstream status. Only a provider-
        # attributed top-level error may enter the external retry lane.
        classification = FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL
        if all(event.get("type") == "error" for event in errors):
            classification = classify_failure_details(
                errors,
                provider_attributed=any(
                    finish.get("stop_reason") == "provider_error"
                    for finish in finishes
                ),
            )
        raise AcceptanceFailure(
            f"{label}: error events: {bounded_value_summary(errors)}",
            classification,
        )
    require(len(finishes) == 1, f"{label}: expected one finish, got {len(finishes)}")
    require(types[-1] == "finish", f"{label}: finish was not terminal: {types[-5:]}")
    require(
        finishes[0].get("stop_reason") == "completed",
        f"{label}: unexpected stop reason {finishes[0].get('stop_reason')}",
    )
    require(bool(text), f"{label}: assistant emitted no user-visible text")
    validate_usage_events(events, label)
    if expect_tools is True:
        require(bool(calls), f"{label}: expected coding tools but saw none")
        validate_complete_tool_lifecycles(events, label)
    elif expect_tools is False:
        lifecycle = tool_lifecycle_events(events)
        require(
            not lifecycle,
            f"{label}: unexpected tool/server/delegated lifecycle events: {lifecycle}",
        )
    return text


def validate_cancelled_stream(
    events: list[dict[str, Any]], label: str
) -> dict[str, Any]:
    types = [event.get("type") for event in events]
    require(bool(types), f"{label}: stream emitted no events")
    validate_usage_events(events, label)
    transport_errors = [
        event
        for event in events
        if event.get("type") in {"error", "server_tool_error", "lagged"}
    ]
    require(not transport_errors, f"{label}: transport errors: {transport_errors}")
    validated_call_ids = validate_complete_tool_lifecycles(
        events,
        label,
        exact_calls=1,
        expected_name="bash",
    )

    finishes = [event for event in events if event.get("type") == "finish"]
    require(len(finishes) == 1, f"{label}: expected one finish, got {len(finishes)}")
    require(types[-1] == "finish", f"{label}: finish was not terminal: {types[-5:]}")
    require(
        finishes[0].get("stop_reason") == "user_abort",
        f"{label}: expected user_abort, got {finishes[0].get('stop_reason')}",
    )

    cancelled_results: list[dict[str, Any]] = []
    for event in events:
        if event.get("type") != "tool_result" or event.get("is_error") is not True:
            continue
        output = event.get("output")
        if not isinstance(output, str):
            continue
        try:
            parsed = json.loads(output)
        except json.JSONDecodeError:
            continue
        if parsed.get("error", {}).get("code") == "cancelled":
            cancelled_results.append(parsed)

    require(
        len(cancelled_results) == 1,
        f"{label}: expected one structured cancelled tool result, got {cancelled_results}",
    )
    result_ids = [
        str(event.get("id"))
        for event in events
        if event.get("type") == "tool_result" and event.get("is_error") is True
    ]
    require(
        result_ids == validated_call_ids,
        f"{label}: cancelled result id did not match the Bash lifecycle: "
        f"results={result_ids}, calls={validated_call_ids}",
    )
    return cancelled_results[0]


def validate_failed_bash_stream(
    events: list[dict[str, Any]],
    label: str,
    expected_command: str,
    stderr_marker: str,
    expected_reply: str,
) -> tuple[str, str, dict[str, Any]]:
    """Validate one intentional Bash failure without treating it as stream failure."""
    require(bool(events), f"{label}: stream emitted no events")
    validate_usage_events(events, label)
    types = [event.get("type") for event in events]

    top_level_errors = [
        event
        for event in events
        if event.get("type") in {"error", "server_tool_error", "lagged"}
    ]
    require(
        not top_level_errors,
        f"{label}: top-level provider/server/backpressure errors: {top_level_errors}",
    )

    finishes = [event for event in events if event.get("type") == "finish"]
    require(len(finishes) == 1, f"{label}: expected one finish, got {len(finishes)}")
    require(types[-1] == "finish", f"{label}: finish was not terminal: {types[-8:]}")
    require(
        finishes[0].get("stop_reason") == "completed",
        f"{label}: expected completed finish, got {finishes[0]}",
    )
    validated_call_ids = validate_complete_tool_lifecycles(
        events,
        label,
        exact_calls=1,
        expected_name="bash",
    )

    complete_calls = [
        event for event in events if event.get("type") == "tool_call_complete"
    ]
    require(
        len(complete_calls) == 1,
        f"{label}: expected exactly one completed tool call, got {complete_calls}",
    )
    call = complete_calls[0]
    require(call.get("name") == "bash", f"{label}: tool was not Bash: {call}")
    call_id = call.get("id")
    require(
        isinstance(call_id, str) and bool(call_id),
        f"{label}: Bash call had no stable id: {call}",
    )
    require(
        validated_call_ids == [call_id],
        f"{label}: validated lifecycle did not match completed Bash call {call_id}",
    )
    arguments = call.get("arguments")
    require(isinstance(arguments, dict), f"{label}: Bash arguments malformed: {call}")
    require(
        arguments.get("command") == expected_command,
        f"{label}: Bash command drifted: {arguments.get('command')!r}",
    )
    require(
        arguments.get("run_in_background") is not True,
        f"{label}: Bash call unexpectedly used background mode: {arguments}",
    )
    require(
        arguments.get("timeout") == 60_000,
        f"{label}: Bash timeout drifted from 60000 ms: {arguments}",
    )

    starts = [event for event in events if event.get("type") == "tool_call_start"]
    executing = [event for event in events if event.get("type") == "tool_executing"]
    results = [event for event in events if event.get("type") == "tool_result"]
    require(
        len(starts) == len(results) == 1 and len(executing) >= 1,
        f"{label}: duplicated or missing tool lifecycle; "
        f"starts={starts}, executing={executing}, results={results}",
    )
    for lifecycle_name, lifecycle_event in (
        ("start", starts[0]),
        ("result", results[0]),
    ):
        require(
            lifecycle_event.get("id") == call_id,
            f"{label}: {lifecycle_name} id did not match {call_id}: {lifecycle_event}",
        )
    for lifecycle_event in executing:
        require(
            lifecycle_event.get("id") == call_id,
            f"{label}: executing id did not match {call_id}: {lifecycle_event}",
        )
    require(starts[0].get("name") == "bash", f"{label}: start was not Bash: {starts[0]}")
    for lifecycle_event in executing:
        require(
            lifecycle_event.get("name") == "bash",
            f"{label}: executing event was not Bash: {lifecycle_event}",
        )
    require(
        results[0].get("is_error") is True,
        f"{label}: expected failed tool result: {results[0]}",
    )

    forbidden_lifecycle = [
        event
        for event in events
        if event.get("type")
        in {
            "awaiting_input",
            "tool_approval_required",
            "tool_approved",
            "tool_denied",
            "server_tool_start",
            "server_tool_complete",
            "web_search_results",
            "web_fetch_result",
        }
        or str(event.get("type", "")).startswith(
            ("agent_background_", "delegated_", "teammate_")
        )
    ]
    require(
        not forbidden_lifecycle,
        f"{label}: unexpected secondary lifecycle events: {forbidden_lifecycle}",
    )

    positions = {
        event_type: next(
            index for index, event in enumerate(events) if event.get("type") == event_type
        )
        for event_type in (
            "tool_call_start",
            "tool_call_complete",
            "tool_result",
            "finish",
        )
    }
    executing_positions = [
        index for index, event in enumerate(events) if event.get("type") == "tool_executing"
    ]
    positions["tool_executing_first"] = executing_positions[0]
    positions["tool_executing_last"] = executing_positions[-1]
    require(
        positions["tool_call_start"]
        < positions["tool_call_complete"]
        < positions["tool_executing_first"]
        <= positions["tool_executing_last"]
        < positions["tool_result"]
        < positions["finish"],
        f"{label}: tool lifecycle was out of order: {positions}",
    )

    output = results[0].get("output")
    require(isinstance(output, str), f"{label}: tool result output was not text")
    try:
        envelope = json.loads(output)
    except json.JSONDecodeError as error:
        raise AcceptanceFailure(
            f"{label}: failed Bash result was not a structured envelope: {output}"
        ) from error
    require(isinstance(envelope, dict), f"{label}: result envelope malformed: {envelope}")
    require(
        envelope.get("ok") is False
        and envelope.get("error", {}).get("code") == "command_failed",
        f"{label}: unexpected failure envelope: {envelope}",
    )
    require(
        envelope.get("metadata", {}).get("exit_code") == 7,
        f"{label}: failed Bash exit code drifted: {envelope}",
    )
    require(
        envelope.get("metadata", {}).get("killed") is False,
        f"{label}: deterministic Bash failure was unexpectedly killed: {envelope}",
    )
    captured_output = envelope.get("data", {}).get("output")
    require(
        isinstance(captured_output, str) and stderr_marker in captured_output,
        f"{label}: stderr marker was absent from structured result: {envelope}",
    )

    text = event_text(events).strip()
    require(text == expected_reply, f"{label}: assistant reply drifted: {text!r}")
    compact_text = re.sub(r"\s+", "", text)
    compact_envelope = re.sub(
        r"\s+", "", json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    )
    require(
        output not in text
        and compact_envelope not in compact_text
        and '"ok":false' not in compact_text.lower()
        and '"error":{' not in compact_text.lower(),
        f"{label}: raw tool-result envelope leaked into assistant prose: {text}",
    )
    require(
        any(
            index > positions["tool_result"]
            and event.get("type") in {"text_delta", "text_delta_with_citations"}
            for index, event in enumerate(events)
        ),
        f"{label}: completed assistant response did not follow the failed result",
    )
    return text, call_id, envelope


def persisted_message_text(message: dict[str, Any]) -> str:
    """Return canonical visible text from one persisted model message."""
    content = message.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    return "\n\n".join(
        str(block.get("text", ""))
        for block in content
        if isinstance(block, dict)
        and block.get("type") == "text"
        and str(block.get("text", ""))
    )


def validate_live_steering_messages(
    messages: list[dict[str, Any]],
    label: str,
    *,
    steering_message: str,
    expected_reply: str,
) -> dict[str, int]:
    """Require one promoted steering message and no staging-role API leak."""
    pending_roles = [
        message.get("role")
        for message in messages
        if str(message.get("role", "")).startswith("pending_user")
    ]
    require(
        not pending_roles,
        f"{label}: GET session leaked pending steering roles: {pending_roles}",
    )

    steering_indices = [
        index
        for index, message in enumerate(messages)
        if message.get("role") == "user"
        and persisted_message_text(message) == steering_message
    ]
    require(
        len(steering_indices) == 1,
        f"{label}: expected steering exactly once in canonical user history, "
        f"got indices={steering_indices}",
    )
    reply_indices = [
        index
        for index, message in enumerate(messages)
        if message.get("role") == "assistant"
        and persisted_message_text(message).strip() == expected_reply
    ]
    require(
        len(reply_indices) == 1,
        f"{label}: expected one persisted steered reply, got indices={reply_indices}",
    )
    require(
        steering_indices[0] < reply_indices[0],
        f"{label}: persisted reply preceded its steering message: "
        f"steering={steering_indices[0]}, reply={reply_indices[0]}",
    )
    return {
        "steering_message_index": steering_indices[0],
        "steered_reply_index": reply_indices[0],
    }


def validate_live_steering_stream(
    events: list[dict[str, Any]],
    label: str,
    *,
    pending_id: str,
    steering_message: str,
    expected_reply: str,
    expected_command: str,
    ready_marker: str,
) -> dict[str, Any]:
    """Validate live steering across one foreground Bash/model boundary."""
    require(bool(events), f"{label}: stream emitted no events")
    validate_usage_events(events, label)

    top_level_errors = [
        event
        for event in events
        if event.get("type") in {"error", "server_tool_error", "lagged"}
    ]
    if top_level_errors:
        classification = FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL
        if all(event.get("type") == "error" for event in top_level_errors):
            classification = classify_failure_details(
                top_level_errors,
                provider_attributed=any(
                    event.get("type") == "finish"
                    and event.get("stop_reason") == "provider_error"
                    for event in events
                ),
            )
        raise AcceptanceFailure(
            f"{label}: top-level stream errors were emitted: {top_level_errors}",
            classification,
        )
    call_ids = validate_complete_tool_lifecycles(
        events,
        label,
        exact_calls=1,
        expected_name="bash",
    )
    call_id = call_ids[0]

    complete = next(
        event
        for event in events
        if event.get("type") == "tool_call_complete"
        and event.get("id") == call_id
    )
    arguments = complete.get("arguments")
    require(
        isinstance(arguments, dict),
        f"{label}: Bash arguments were malformed: {complete}",
    )
    require(
        arguments.get("command") == expected_command,
        f"{label}: Bash command drifted: {arguments.get('command')!r}",
    )
    require(
        arguments.get("run_in_background") is not True,
        f"{label}: Bash probe unexpectedly used background mode: {arguments}",
    )
    require(
        arguments.get("timeout") == 60_000,
        f"{label}: Bash timeout drifted from 60000 ms: {arguments}",
    )

    results = [
        (index, event)
        for index, event in enumerate(events)
        if event.get("type") == "tool_result" and event.get("id") == call_id
    ]
    require(
        len(results) == 1 and results[0][1].get("is_error") is not True,
        f"{label}: foreground Bash did not complete cleanly: {results}",
    )
    ready_indices = [
        index
        for index, event in enumerate(events)
        if event.get("type") == "tool_output_delta"
        and ready_marker in str(event.get("delta", ""))
    ]
    require(
        len(ready_indices) == 1,
        f"{label}: expected one READY output delta, got indices={ready_indices}",
    )

    injected = [
        (index, event)
        for index, event in enumerate(events)
        if event.get("type") == "steering_injected"
        and event.get("pending_id") == pending_id
    ]
    require(
        len(injected) == 1,
        f"{label}: expected one steering_injected for {pending_id}, got {injected}",
    )
    injection_index, injection_event = injected[0]
    require(
        injection_event.get("message") == steering_message,
        f"{label}: injected steering text drifted: {injection_event}",
    )
    all_injected = [
        event for event in events if event.get("type") == "steering_injected"
    ]
    require(
        len(all_injected) == 1,
        f"{label}: unexpected additional steering injections: {all_injected}",
    )

    continuing_turns = [
        index
        for index, event in enumerate(events)
        if event.get("type") == "turn_complete"
        and event.get("has_more") is True
        and index < injection_index
    ]
    require(
        len(continuing_turns) == 1,
        f"{label}: expected one TurnComplete(has_more=true) before steering, "
        f"got indices={continuing_turns}",
    )
    continuing_turn_index = continuing_turns[0]
    require(
        ready_indices[0]
        < results[0][0]
        < continuing_turn_index
        < injection_index,
        f"{label}: READY/result/turn/steering order was invalid: "
        f"ready={ready_indices[0]}, result={results[0][0]}, "
        f"turn={continuing_turn_index}, steering={injection_index}",
    )

    later_tool_events = [
        event
        for event in events[injection_index + 1 :]
        if event.get("type") in _CORE_TOOL_LIFECYCLE_TYPES
    ]
    require(
        not later_tool_events,
        f"{label}: model called another tool after steering: {later_tool_events}",
    )
    reply_events = [
        event
        for event in events[injection_index + 1 :]
        if event.get("type") in {"text_delta", "text_delta_with_citations"}
    ]
    reply_text = event_text(reply_events).strip()
    require(
        reply_text == expected_reply,
        f"{label}: assistant did not follow the exact steered marker: {reply_text!r}",
    )
    first_reply_index = next(
        index
        for index, event in enumerate(events)
        if index > injection_index
        and event.get("type") in {"text_delta", "text_delta_with_citations"}
    )

    finishes = [
        (index, event)
        for index, event in enumerate(events)
        if event.get("type") == "finish"
    ]
    require(len(finishes) == 1, f"{label}: expected one finish, got {finishes}")
    finish_index, finish = finishes[0]
    require(
        finish_index == len(events) - 1 and finish.get("stop_reason") == "completed",
        f"{label}: finish was not terminal completed: {finish}",
    )
    require(
        injection_index < first_reply_index < finish_index,
        f"{label}: steered reply ordering was invalid: steering={injection_index}, "
        f"reply={first_reply_index}, finish={finish_index}",
    )
    return {
        "tool_call_id": call_id,
        "ready_index": ready_indices[0],
        "tool_result_index": results[0][0],
        "continuing_turn_index": continuing_turn_index,
        "steering_injected_index": injection_index,
        "steered_reply_index": first_reply_index,
        "finish_index": finish_index,
        "reply": reply_text,
    }


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def append_json_record(path: Path, value: Any) -> None:
    """Durably append one attempt without replacing prior invocation history."""
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, sort_keys=True, default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def external_retry_delay(base_seconds: float, retry_number: int) -> float:
    return min(60.0, base_seconds * (2 ** max(0, retry_number - 1)))


def wait_for_exact_text(path: Path, expected: str, timeout: float, label: str) -> None:
    deadline = time.monotonic() + timeout
    latest: str | None = None
    while time.monotonic() < deadline:
        try:
            latest = path.read_text()
        except (FileNotFoundError, OSError):
            latest = None
        if latest == expected:
            return
        time.sleep(0.1)
    raise AcceptanceFailure(
        f"{label} did not contain the exact expected text within {timeout:.1f}s; "
        f"path={path}, latest={latest!r}"
    )


def wait_for_session_idle(
    api: MitsuroApi, session_id: str, timeout: float = 120.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    latest: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        latest = api.json_request("GET", f"/api/sessions/{session_id}/state")
        if latest.get("agent_state") == "idle":
            return latest
        time.sleep(0.2)
    raise AcceptanceFailure(
        f"session {session_id} did not return idle within {timeout:.1f}s; latest={latest}"
    )


def wait_for_session_settled(
    api: MitsuroApi, session_id: str, timeout: float = 120.0
) -> dict[str, Any]:
    """Wait until the parent and every durable delegated graph have settled.

    A detached delegation intentionally lets the parent become idle while Hive
    Agents continue. Proofs that only wait for ``agent_state == idle`` can
    therefore score an in-flight graph as a terminal failure. A requested
    parent continuation is also part of the turn: pending/queued means the
    final parent synthesis has not happened yet.
    """
    terminal_group_states = {"complete", "degraded", "failed", "cancelled"}
    terminal_continuation_states = {"not_requested", "promoted"}
    deadline = time.monotonic() + timeout
    latest: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        latest = api.json_request("GET", f"/api/sessions/{session_id}/state")
        groups = latest.get("delegation_groups") or []
        groups_settled = all(
            group.get("state") in terminal_group_states
            and group.get("parent_continuation_state")
            in terminal_continuation_states
            for group in groups
        )
        if latest.get("agent_state") == "idle" and groups_settled:
            return latest
        time.sleep(0.5)
    raise AcceptanceFailure(
        f"session {session_id} did not fully settle within {timeout:.1f}s; "
        f"latest={latest}"
    )


def require_clean_idle_state(state: dict[str, Any], label: str) -> None:
    require(state.get("agent_state") == "idle", f"{label}: session not idle: {state}")
    require(
        not state.get("pending_interactions"),
        f"{label}: pending interactions remained: {state}",
    )
    require(state.get("recovery") is None, f"{label}: recovery state remained: {state}")


def session_messages(api: MitsuroApi, session_id: str) -> list[dict[str, Any]]:
    persisted = api.json_request("GET", f"/api/sessions/{session_id}")
    messages = persisted.get("messages", [])
    require(isinstance(messages, list), f"session messages were malformed: {persisted}")
    return messages


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def validate_trace_response(trace: Any, label: str) -> dict[str, Any]:
    require(isinstance(trace, dict), f"{label}: trace response was malformed: {trace}")
    events = trace.get("events")
    require(isinstance(events, list), f"{label}: trace events were malformed: {trace}")

    sequences: list[int] = []
    for index, event in enumerate(events):
        require(isinstance(event, dict), f"{label}: trace event {index} was malformed")
        sequence = event.get("sequence")
        require(
            isinstance(sequence, int)
            and not isinstance(sequence, bool)
            and sequence > 0,
            f"{label}: trace sequence at index {index} was invalid: {sequence!r}",
        )
        sequences.append(sequence)

    require(
        len(sequences) == len(set(sequences)),
        f"{label}: trace sequences were not unique: {sequences}",
    )
    require(
        all(previous < current for previous, current in zip(sequences, sequences[1:])),
        f"{label}: trace sequences were not strictly increasing: {sequences}",
    )

    latest_sequence = trace.get("latest_sequence")
    if sequences:
        require(
            isinstance(latest_sequence, int)
            and not isinstance(latest_sequence, bool)
            and latest_sequence == sequences[-1],
            f"{label}: latest_sequence {latest_sequence!r} did not match "
            f"last event {sequences[-1]}",
        )
    else:
        require(
            latest_sequence is None,
            f"{label}: empty trace exposed latest_sequence={latest_sequence!r}",
        )

    summary = trace.get("summary", {})
    require(isinstance(summary, dict), f"{label}: trace summary was malformed: {trace}")
    return summary


def wait_for_completed_trace_run(
    api: MitsuroApi,
    session_id: str,
    label: str,
    *,
    after_sequence: int = 0,
    timeout: float = 5.0,
    poll_interval: float = 0.05,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Wait for one newly completed run to become durably observable.

    Runtime traces are intentionally forwarded to SSE before their compact
    SQLite batch is flushed. A terminal SSE event and idle session therefore
    do not imply that the trace endpoint has already crossed the same boundary.
    Poll only across that bounded persistence window, and require both the
    canonical ``finished`` event and provider-call accounting for the same run
    before accepting the snapshot as complete.

    A newly persisted non-completed terminal is definitive and fails
    immediately; the wait must never turn a real provider/cancellation failure
    into a timing retry.
    """
    require(
        isinstance(after_sequence, int)
        and not isinstance(after_sequence, bool)
        and after_sequence >= 0,
        f"{label}: invalid trace cursor {after_sequence!r}",
    )
    deadline = time.monotonic() + timeout
    latest_trace: dict[str, Any] | None = None
    latest_summary: dict[str, Any] | None = None

    while True:
        trace = api.json_request(
            "GET", f"/api/sessions/{session_id}/trace?limit=1000"
        )
        summary = validate_trace_response(trace, label)
        latest_trace = trace
        latest_summary = summary
        new_events = [
            event
            for event in trace["events"]
            if event.get("sequence", 0) > after_sequence
        ]
        finished = [
            event for event in new_events if event.get("event_type") == "finished"
        ]
        non_completed = [
            event
            for event in finished
            if event.get("stop_reason") != "completed"
            or event.get("payload", {}).get("stop_reason") != "completed"
        ]
        require(
            not non_completed,
            f"{label}: newly persisted run did not complete: {non_completed}",
        )

        completed_run_ids = {
            event.get("run_id")
            for event in finished
            if isinstance(event.get("run_id"), str) and event.get("run_id")
        }
        accounted_run_ids = {
            event.get("run_id")
            for event in new_events
            if event.get("event_type") == "provider_call"
            and isinstance(event.get("run_id"), str)
            and event.get("run_id")
        }
        if completed_run_ids & accounted_run_ids:
            return trace, summary

        if time.monotonic() >= deadline:
            break
        time.sleep(max(0.0, poll_interval))

    latest_new_events = (
        [
            event
            for event in latest_trace["events"]
            if event.get("sequence", 0) > after_sequence
        ]
        if latest_trace is not None
        else []
    )
    latest_types = [event.get("event_type") for event in latest_new_events]

    def run_ids_for(event_type: str) -> list[str]:
        return sorted(
            {
                event["run_id"]
                for event in latest_new_events
                if event.get("event_type") == event_type
                and isinstance(event.get("run_id"), str)
                and event.get("run_id")
            }
        )

    raise AcceptanceFailure(
        f"{label}: trace did not durably expose a newly completed, provider-accounted "
        f"run after sequence {after_sequence} within {timeout:.1f}s; "
        f"budget_run_ids={run_ids_for('run_budget_resolved')}, "
        f"finished_run_ids={run_ids_for('finished')}, "
        f"provider_call_run_ids={run_ids_for('provider_call')}, "
        f"latest_summary={latest_summary}, latest_event_types={latest_types}"
    )


def wait_for_settled_trace_runs(
    api: MitsuroApi,
    session_id: str,
    label: str,
    *,
    expected_runs: int,
    timeout: float = 5.0,
    poll_interval: float = 0.05,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Wait until every expected run has a durable terminal and accounting row.

    A trace read can legitimately land between compact SQLite batches after the
    session has already returned to idle. Requiring the expected run count keeps
    an older, already-settled snapshot from satisfying a newer follow-up run.
    Structural contradictions still fail immediately; only missing suffix rows
    are allowed to converge during the bounded persistence window.
    """
    require(
        isinstance(expected_runs, int)
        and not isinstance(expected_runs, bool)
        and expected_runs > 0,
        f"{label}: invalid expected run count {expected_runs!r}",
    )
    deadline = time.monotonic() + timeout
    latest_summary: dict[str, Any] | None = None
    latest_budget_ids: set[str] = set()
    latest_finished_ids: set[str] = set()
    latest_provider_call_ids: set[str] = set()

    while True:
        trace = api.json_request(
            "GET", f"/api/sessions/{session_id}/trace?limit=1000"
        )
        summary = validate_trace_response(trace, label)
        latest_summary = summary
        budget_ids: list[str] = []
        finished_ids: list[str] = []
        provider_call_ids: set[str] = set()

        for event in trace["events"]:
            event_type = event.get("event_type")
            if event_type not in {
                "run_budget_resolved",
                "finished",
                "provider_call",
            }:
                continue
            run_id = event.get("run_id")
            if event_type == "provider_call" and not (
                isinstance(run_id, str) and run_id
            ):
                # Auxiliary provider work, such as title generation, is not an
                # orchestrated run boundary.
                continue
            require(
                isinstance(run_id, str) and bool(run_id),
                f"{label}: {event_type} event lacked a run id: {event}",
            )
            if event_type == "run_budget_resolved":
                budget_ids.append(run_id)
            elif event_type == "finished":
                finished_ids.append(run_id)
            else:
                provider_call_ids.add(run_id)

        require(
            len(finished_ids) == len(set(finished_ids)),
            f"{label}: duplicate terminal run ids: {finished_ids}",
        )
        budget_id_set = set(budget_ids)
        finished_id_set = set(finished_ids)
        require(
            finished_id_set <= budget_id_set,
            f"{label}: terminal runs lacked prior budget events: "
            f"budgets={sorted(budget_id_set)}, finished={sorted(finished_id_set)}",
        )
        require(
            len(budget_id_set) <= expected_runs,
            f"{label}: observed more runs than expected: "
            f"expected={expected_runs}, budgets={sorted(budget_id_set)}",
        )

        latest_budget_ids = budget_id_set
        latest_finished_ids = finished_id_set
        latest_provider_call_ids = provider_call_ids
        if (
            len(budget_id_set) == expected_runs
            and finished_id_set == budget_id_set
            and budget_id_set <= provider_call_ids
        ):
            validate_trace_run_budgets(trace, label)
            require(
                summary.get("total_runs") == expected_runs,
                f"{label}: summary total_runs={summary.get('total_runs')}, "
                f"expected {expected_runs}",
            )
            return trace, summary

        if time.monotonic() >= deadline:
            break
        time.sleep(max(0.0, poll_interval))

    raise AcceptanceFailure(
        f"{label}: trace did not durably settle {expected_runs} expected runs "
        f"within {timeout:.1f}s; budget_run_ids={sorted(latest_budget_ids)}, "
        f"finished_run_ids={sorted(latest_finished_ids)}, "
        f"provider_call_run_ids={sorted(latest_provider_call_ids)}, "
        f"latest_summary={latest_summary}"
    )


def trace_summary(
    api: MitsuroApi,
    session_id: str,
    *,
    expected_runs: int,
    exact_tool_calls: int | None = None,
    expected_tool_name: str | None = None,
) -> dict[str, Any]:
    label = f"session {session_id}"
    trace, summary = wait_for_settled_trace_runs(
        api,
        session_id,
        label,
        expected_runs=expected_runs,
    )
    if exact_tool_calls is not None:
        validate_trace_tool_lifecycles(
            trace["events"],
            label,
            exact_calls=exact_tool_calls,
            expected_name=expected_tool_name,
        )
        require(
            summary.get("tool_calls") == exact_tool_calls,
            f"{label}: trace summary tool_calls={summary.get('tool_calls')}, "
            f"expected {exact_tool_calls}",
        )
    return summary


def trace_event_count(summary: dict[str, Any], event_type: str) -> int | None:
    counts = summary.get("event_counts")
    if not isinstance(counts, list):
        return None
    for count in counts:
        if isinstance(count, dict) and count.get("event_type") == event_type:
            value = count.get("count")
            return value if isinstance(value, int) and not isinstance(value, bool) else None
    return 0


def pid_is_running(pid: int) -> bool:
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        fields = stat_path.read_text().split()
        if len(fields) >= 3:
            return fields[2] != "Z"
    except (FileNotFoundError, PermissionError, OSError):
        pass

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def probe_pid_belongs_to(pid: int, run_dir: Path) -> bool:
    proc_dir = Path(f"/proc/{pid}")
    try:
        if proc_dir.joinpath("cwd").resolve() == run_dir.resolve():
            return True
    except (FileNotFoundError, PermissionError, OSError):
        pass
    try:
        command = proc_dir.joinpath("cmdline").read_bytes().replace(b"\0", b" ")
        return str(run_dir).encode() in command
    except (FileNotFoundError, PermissionError, OSError):
        return False


def read_probe_pids(run_dir: Path) -> list[int]:
    pids: list[int] = []
    for name in ("disconnect.pid", "child.pid", "parent.pid"):
        try:
            pid = int(run_dir.joinpath(name).read_text().strip())
        except (FileNotFoundError, ValueError, OSError):
            continue
        if pid > 0 and pid not in pids:
            pids.append(pid)
    return pids


def cleanup_probe_pids(run_dir: Path, known_pids: list[int]) -> dict[str, Any]:
    pids = list(dict.fromkeys([*known_pids, *read_probe_pids(run_dir)]))
    report: dict[str, Any] = {
        "pids": pids,
        "running_before_cleanup": [pid for pid in pids if pid_is_running(pid)],
        "signal_attempts": [],
        "running_after_cleanup": [],
        "errors": [],
    }
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for pid in pids:
            if pid_is_running(pid) and probe_pid_belongs_to(pid, run_dir):
                try:
                    os.kill(pid, sig)
                    report["signal_attempts"].append(
                        {"pid": pid, "signal": sig.name, "status": "sent"}
                    )
                except (ProcessLookupError, PermissionError, OSError) as error:
                    report["signal_attempts"].append(
                        {
                            "pid": pid,
                            "signal": sig.name,
                            "status": "failed",
                            "error": str(error),
                        }
                    )
                    report["errors"].append(
                        f"failed to send {sig.name} to probe PID {pid}: {error}"
                    )
        if sig == signal.SIGTERM:
            time.sleep(0.25)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and any(pid_is_running(pid) for pid in pids):
        time.sleep(0.05)
    report["running_after_cleanup"] = [pid for pid in pids if pid_is_running(pid)]
    if report["running_after_cleanup"]:
        report["errors"].append(
            f"probe PIDs remained alive after TERM/KILL: "
            f"{report['running_after_cleanup']}"
        )
    report["status"] = "pass" if not report["errors"] else "fail"
    return report


def require_probe_quiet(
    run_dir: Path,
    pids: list[int],
    forbidden_artifacts: list[Path],
    grace: float = 5.0,
) -> None:
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        appeared = [str(path) for path in forbidden_artifacts if path.exists()]
        require(not appeared, f"cancelled probe created orphan artifacts: {appeared}")
        time.sleep(0.1)

    running = [pid for pid in pids if pid_is_running(pid)]
    require(not running, f"cancelled probe left processes running: {running}")


def wait_for_process_status(
    api: MitsuroApi, process_id: str, expected: str, timeout: float = 10.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    latest: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        latest = api.json_request("GET", f"/api/processes/{process_id}")
        if latest.get("status_code") == expected:
            return latest
        time.sleep(0.2)
    raise AcceptanceFailure(
        f"process {process_id} did not reach {expected}; latest={latest}"
    )


def fetch_text(url: str, timeout: float = 5.0) -> tuple[int, str, str]:
    request = Request(url, headers={"Accept": "*/*"})
    try:
        with urlopen(request, timeout=timeout) as response:
            return (
                response.status,
                response.headers.get("Content-Type", ""),
                response.read().decode(errors="replace"),
            )
    except (HTTPError, URLError, TimeoutError) as error:
        raise AcceptanceFailure(f"preview request {url} failed: {error}") from error


def assert_preview_unresponsive(url: str, timeout: float = 5.0) -> None:
    """Require a sustained absence of HTTP responses while a process is suspended.

    A suspended listener intentionally keeps its socket bound, so TCP refusal is
    the wrong contract here. Connection errors and request timeouts both prove
    unresponsiveness; an HTTP error status still proves that the server replied.
    """
    deadline = time.monotonic() + timeout
    quiet_since: float | None = None
    quiet_period = min(0.5, max(0.1, timeout / 4.0))
    last_observation = "not probed"
    while time.monotonic() < deadline:
        remaining = max(0.05, deadline - time.monotonic())
        try:
            with urlopen(Request(url), timeout=min(0.25, remaining)) as response:
                response.read(1)
                last_observation = f"HTTP {response.status} response"
                quiet_since = None
        except HTTPError as error:
            last_observation = f"HTTP {error.code} response"
            quiet_since = None
        except (URLError, TimeoutError, ConnectionError) as error:
            last_observation = f"{type(error).__name__}: {error}"
            now = time.monotonic()
            if quiet_since is None:
                quiet_since = now
            if now - quiet_since >= quiet_period:
                return
        time.sleep(0.05)
    raise AcceptanceFailure(
        f"preview remained HTTP-responsive while suspended; it never stayed "
        f"unresponsive for {quiet_period:.2f}s within {timeout:.1f}s: {url}; "
        f"last_observation={last_observation}"
    )


def assert_preview_port_quiet(url: str, timeout: float = 5.0) -> None:
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port
    require(
        host is not None and port is not None,
        f"preview URL did not contain an explicit host and port: {url}",
    )
    deadline = time.monotonic() + timeout
    quiet_since: float | None = None
    quiet_period = min(0.25, max(0.05, timeout / 4.0))
    last_observation = "not probed"
    while time.monotonic() < deadline:
        remaining = max(0.05, deadline - time.monotonic())
        try:
            with socket.create_connection(
                (host, port), timeout=min(0.25, remaining)
            ):
                last_observation = "TCP connection succeeded"
                quiet_since = None
        except (ConnectionRefusedError, ConnectionResetError) as error:
            last_observation = f"{type(error).__name__}: {error}"
            now = time.monotonic()
            if quiet_since is None:
                quiet_since = now
            if now - quiet_since >= quiet_period:
                return
        except TimeoutError as error:
            # A timeout can mean a live but wedged listener. It is never proof
            # that the assigned port is quiet.
            last_observation = f"timeout while connecting: {error}"
            quiet_since = None
        except OSError as error:
            # Only an explicit connection refusal/reset proves unavailability.
            # DNS, routing, descriptor, and other local errors remain failures.
            last_observation = f"{type(error).__name__}: {error}"
            quiet_since = None
        time.sleep(0.05)
    raise AcceptanceFailure(
        f"preview port did not stay strictly refused/reset for {quiet_period:.2f}s "
        f"within {timeout:.1f}s after process stop: {url}; "
        f"last_observation={last_observation}"
    )


def create_session(
    api: MitsuroApi,
    run_dir: Path,
    model: dict[str, Any],
    label: str,
    *,
    permission_mode: str = "autonomous",
) -> str:
    response = api.json_request(
        "POST",
        "/api/sessions",
        {
            "title": label,
            "model": model["id"],
            "model_key": model["key"],
            "project_dir": str(run_dir),
            "workspace_mode": "created",
            "session_type": "code",
            "permission_mode": permission_mode,
        },
    )
    session_id = response.get("id")
    require(bool(session_id), f"session create returned no id: {response}")
    require(response.get("working_dir") == str(run_dir), "session working_dir drifted")
    require(response.get("project_dir") == str(run_dir), "session project_dir drifted")
    require(response.get("model") == model["id"], f"session model drifted: {response}")
    require(response.get("model_key") == model["key"], f"session model key drifted: {response}")
    require(
        response.get("model_catalog_revision") == model.get("catalog_revision"),
        f"session model revision drifted: {response}",
    )
    return str(session_id)


def chat_payload(
    session_id: str,
    message: str,
    model: dict[str, Any] | None,
    *,
    permission_mode: str = "autonomous",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "session_id": session_id,
        "session_type": "code",
        "message": message,
        "permission_mode": permission_mode,
        "thinking_enabled": "off",
    }
    if model is not None:
        payload["model"] = model["id"]
        payload["model_key"] = model["key"]
    return payload


def require_session_model(
    api: MitsuroApi,
    session_id: str,
    expected_model: dict[str, Any],
    label: str,
) -> str:
    persisted = api.json_request("GET", f"/api/sessions/{session_id}")
    session = persisted.get("session") if isinstance(persisted, dict) else None
    require(isinstance(session, dict), f"{label}: persisted session was malformed: {persisted}")
    actual_model = session.get("model")
    require(
        actual_model == expected_model["id"],
        f"{label}: persisted model drifted from {expected_model['id']!r} to {actual_model!r}",
    )
    require(
        session.get("model_key") == expected_model["key"],
        f"{label}: persisted exact model key drifted: {session}",
    )
    require(
        session.get("model_catalog_revision") == expected_model.get("catalog_revision"),
        f"{label}: persisted model revision drifted: {session}",
    )
    return str(actual_model)


def process_for_cycle(
    api: MitsuroApi, run_dir: Path, port: int
) -> dict[str, Any]:
    matches = [
        process
        for process in api.json_request("GET", "/api/processes")
        if process.get("working_dir") == str(run_dir)
        and str(port)
        in f"{process.get('command', '')} {process.get('description', '')}"
    ]
    failed = [process for process in matches if process.get("status_code") == "failed"]
    running = [process for process in matches if process.get("status_code") == "running"]
    require(not failed, f"cycle registered failed background processes: {failed}")
    require(len(running) == 1, f"expected one running preview process, got {matches}")
    return running[0]


def active_processes_for_dir(api: MitsuroApi, run_dir: Path) -> list[dict[str, Any]]:
    return [
        process
        for process in api.json_request("GET", "/api/processes")
        if process.get("working_dir") == str(run_dir)
        and process.get("status_code") in {"running", "suspended"}
    ]


def verify_artifacts(run_dir: Path, marker: str) -> None:
    required = [run_dir / "server.py", run_dir / "tests" / "test_server.py", run_dir / "README.md"]
    missing = [str(path) for path in required if not path.is_file()]
    require(not missing, f"missing expected artifacts: {missing}")
    require(marker in (run_dir / "server.py").read_text(), "server.py lost marker")
    result = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        cwd=run_dir,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    require(
        result.returncode == 0,
        f"independent artifact tests failed:\n{result.stdout}\n{result.stderr}",
    )


def bounded_value_summary(value: Any, limit: int = 2_000) -> dict[str, Any]:
    """Return diagnostic text without persisting an unbounded model/tool payload."""
    if isinstance(value, str):
        rendered = value
    else:
        rendered = json.dumps(value, sort_keys=True, default=str)
    return {
        "preview": rendered[:limit],
        "characters": len(rendered),
        "truncated": len(rendered) > limit,
    }


def summarize_stream_evidence(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Keep compact stream evidence even when strict validation rejects a turn."""
    event_counts: dict[str, int] = {}
    for event in events:
        event_type = str(event.get("type", "unknown"))
        event_counts[event_type] = event_counts.get(event_type, 0) + 1
    errors = [
        {
            "type": event.get("type"),
            "id": event.get("id"),
            "name": event.get("name"),
            "is_error": event.get("is_error"),
            "details": bounded_value_summary(event),
        }
        for event in events
        if event.get("type") in {"error", "server_tool_error", "lagged"}
        or (event.get("type") == "tool_result" and event.get("is_error") is True)
    ]
    finishes = [event for event in events if event.get("type") == "finish"]
    return {
        "event_counts": event_counts,
        "error_events": errors,
        "finish_events": finishes,
        "assistant_text": bounded_value_summary(event_text(events).strip()),
        "completed_tool_calls": [
            {
                "id": call.get("id"),
                "name": call.get("name"),
                "arguments": bounded_value_summary(call.get("arguments")),
            }
            for call in tool_calls(events)
        ],
    }


def _captured_evidence(
    check: Callable[[], Any],
) -> dict[str, Any]:
    """Capture a diagnostic check without replacing the cycle's first failure."""
    try:
        return {"status": "captured", "value": check()}
    except Exception as error:
        return {
            "status": "capture_failed",
            "failure_classification": failure_classification(error).value,
            "error": str(error),
        }


def collect_failed_cycle_evidence(
    api: MitsuroApi,
    run_dir: Path,
    session_id: str | None,
    marker: str,
    port: int,
) -> dict[str, Any]:
    """Harvest independent evidence after a strict cycle assertion fails.

    The evidence is diagnostic only: a recovered tool error still fails the
    clean-streak gate. Capturing the rest of the outcome prevents the first
    assertion from hiding artifact, persistence, trace, endpoint, or process
    behavior that is needed to diagnose and fix the harness.
    """

    required = (
        run_dir / "server.py",
        run_dir / "tests" / "test_server.py",
        run_dir / "README.md",
    )

    def artifact_evidence() -> dict[str, Any]:
        present = {str(path.relative_to(run_dir)): path.is_file() for path in required}
        server_path = run_dir / "server.py"
        marker_present = (
            marker in server_path.read_text() if server_path.is_file() else False
        )
        test_result: dict[str, Any] = {"ran": False}
        tests_dir = run_dir / "tests"
        if tests_dir.is_dir():
            completed = subprocess.run(
                [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
                cwd=run_dir,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            test_result = {
                "ran": True,
                "returncode": completed.returncode,
                "stdout": bounded_value_summary(completed.stdout, 4_000),
                "stderr": bounded_value_summary(completed.stderr, 4_000),
            }
        return {
            "expected_files": present,
            "marker_present_in_server": marker_present,
            "independent_tests": test_result,
            "validation_passed": (
                all(present.values())
                and marker_present
                and test_result.get("ran") is True
                and test_result.get("returncode") == 0
            ),
        }

    def process_evidence() -> dict[str, Any]:
        matches = [
            process
            for process in api.json_request("GET", "/api/processes")
            if process.get("working_dir") == str(run_dir)
        ]
        process_summaries = [
            {
                "id": process.get("id"),
                "pid": process.get("pid"),
                "status": process.get("status"),
                "status_code": process.get("status_code"),
                "exit_code": process.get("exit_code"),
                "command": bounded_value_summary(process.get("command"), 1_000),
                "description": bounded_value_summary(
                    process.get("description"), 1_000
                ),
                "error": bounded_value_summary(process.get("error"), 1_000),
            }
            for process in matches
        ]
        return {
            "matching_processes": process_summaries,
            "active_process_ids": [
                process.get("id")
                for process in matches
                if process.get("status_code") in {"running", "suspended"}
            ],
            "port_matching_process_ids": [
                process.get("id")
                for process in matches
                if str(port)
                in f"{process.get('command', '')} {process.get('description', '')}"
            ],
        }

    def endpoint_evidence(path: str) -> dict[str, Any]:
        url = f"http://127.0.0.1:{port}{path}"
        request = Request(url, headers={"Accept": "*/*"})
        try:
            with urlopen(request, timeout=5.0) as response:
                status = response.status
                content_type = response.headers.get("Content-Type", "")
                body = response.read().decode(errors="replace")
        except HTTPError as error:
            status = error.code
            content_type = error.headers.get("Content-Type", "")
            body = error.read().decode(errors="replace")
        except (URLError, TimeoutError, ConnectionError) as error:
            return {
                "reachable": False,
                "url": url,
                "transport_error": bounded_value_summary(str(error), 500),
                "expected_payload": False,
            }

        parse_error: str | None = None
        if path == "/health":
            try:
                expected = json.loads(body) == {"ok": True, "marker": marker}
            except json.JSONDecodeError as error:
                expected = False
                parse_error = str(error)
        else:
            expected = body == marker
        return {
            "reachable": True,
            "url": url,
            "status_code": status,
            "content_type": content_type,
            "body": bounded_value_summary(body),
            "expected_payload": expected,
            "parse_error": parse_error,
        }

    evidence: dict[str, Any] = {
        "artifacts": _captured_evidence(artifact_evidence),
        "processes_before_cleanup": _captured_evidence(process_evidence),
        "health_endpoint": _captured_evidence(
            lambda: endpoint_evidence("/health")
        ),
        "artifact_endpoint": _captured_evidence(
            lambda: endpoint_evidence("/artifact")
        ),
    }
    if session_id is None:
        evidence["session"] = {
            "status": "not_created",
            "value": None,
        }
        return evidence

    def persistence_evidence() -> dict[str, Any]:
        persisted = api.json_request("GET", f"/api/sessions/{session_id}")
        messages = persisted.get("messages", [])
        require(isinstance(messages, list), f"session messages malformed: {persisted}")
        last_message = messages[-1] if messages else None
        last_summary: dict[str, Any] | None = None
        if isinstance(last_message, dict):
            content = last_message.get("content")
            blocks = content if isinstance(content, list) else []
            text = "".join(
                str(block.get("text", ""))
                for block in blocks
                if isinstance(block, dict) and block.get("type") == "text"
            )
            if not text and isinstance(content, str):
                text = content
            last_summary = {
                "role": last_message.get("role"),
                "content_types": [
                    block.get("type")
                    for block in blocks
                    if isinstance(block, dict)
                ],
                "tool_names": [
                    block.get("name")
                    for block in blocks
                    if isinstance(block, dict)
                    and block.get("type") == "tool_use"
                ],
                "tool_result_errors": sum(
                    block.get("type") == "tool_result"
                    and block.get("is_error") is True
                    for block in blocks
                    if isinstance(block, dict)
                ),
                "text": bounded_value_summary(text),
            }
        return {
            "message_count": len(messages),
            "user_messages": sum(message.get("role") == "user" for message in messages),
            "assistant_messages": sum(
                message.get("role") == "assistant" for message in messages
            ),
            "marker_present": marker in json.dumps(messages),
            "last_message": last_summary,
        }

    def state_evidence() -> dict[str, Any]:
        state = api.json_request("GET", f"/api/sessions/{session_id}/state")
        pending = state.get("pending_interactions")
        pending_count = len(pending) if isinstance(pending, list) else None
        recovery = state.get("recovery")
        return {
            "agent_state": state.get("agent_state"),
            "pending_interaction_count": pending_count,
            "pending_interactions": bounded_value_summary(pending),
            "recovery_present": recovery is not None,
            "recovery": bounded_value_summary(recovery),
            "clean_idle": (
                state.get("agent_state") == "idle"
                and not pending
                and recovery is None
            ),
        }

    def trace_evidence() -> dict[str, Any]:
        trace = api.json_request(
            "GET", f"/api/sessions/{session_id}/trace?limit=1000"
        )
        summary = validate_trace_response(trace, f"failed session {session_id}")
        return {
            "summary": summary,
            "latest_sequence": trace.get("latest_sequence"),
            "event_count": len(trace.get("events", [])),
        }

    evidence["session"] = {
        "persistence": _captured_evidence(persistence_evidence),
        "state": _captured_evidence(state_evidence),
        "trace": _captured_evidence(trace_evidence),
    }
    return evidence


def validate_trace_run_budgets(
    trace: dict[str, Any],
    label: str,
) -> list[dict[str, Any]]:
    """Require one explicit unlimited-default budget for every terminal run."""
    budget_events_by_run: dict[str, list[dict[str, Any]]] = {}
    finished_run_ids: list[str] = []

    for event in trace.get("events", []):
        event_type = event.get("event_type")
        if event_type not in {"run_budget_resolved", "finished"}:
            continue

        run_id = event.get("run_id")
        require(
            isinstance(run_id, str) and bool(run_id),
            f"{label}: {event_type} event lacked a run id: {event}",
        )
        if event_type == "finished":
            require(
                run_id not in finished_run_ids,
                f"{label}: run {run_id} emitted duplicate terminal events",
            )
            finished_run_ids.append(run_id)
            continue

        payload = event.get("payload")
        require(
            isinstance(payload, dict),
            f"{label}: run {run_id} budget payload was malformed: {payload}",
        )
        budget_events_by_run.setdefault(run_id, []).append(payload)

    require(finished_run_ids, f"{label}: trace omitted terminal runs")
    require(
        set(budget_events_by_run) == set(finished_run_ids),
        f"{label}: budget run ids did not match terminal run ids: "
        f"budgets={sorted(budget_events_by_run)}, finished={sorted(finished_run_ids)}",
    )

    resolved: list[dict[str, Any]] = []
    for run_id in finished_run_ids:
        budgets = budget_events_by_run[run_id]
        require(
            len(budgets) == 1,
            f"{label}: run {run_id} emitted {len(budgets)} budget events: {budgets}",
        )
        budget = budgets[0]
        require(
            budget.get("max_turns") is None
            and budget.get("source") == "unlimited_default",
            f"{label}: run {run_id} retained a hidden or non-default turn cap: {budget}",
        )
        resolved.append(
            {
                "run_id": run_id,
                "max_turns": budget.get("max_turns"),
                "source": budget.get("source"),
            }
        )
    return resolved


def validate_trace_exact_runtime(
    trace: dict[str, Any],
    model: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    snapshots: list[dict[str, Any]] = []
    for event in trace.get("events", []):
        payload = event.get("payload")
        if not isinstance(payload, dict):
            continue
        if event.get("event_type") == "provider_request_prepared":
            diagnostics = payload.get("diagnostics")
            if isinstance(diagnostics, dict):
                snapshots.append(diagnostics)

    require(snapshots, f"{label}: trace omitted provider request snapshots")
    for index, snapshot in enumerate(snapshots):
        require(
            snapshot.get("model_key") == model["key"],
            f"{label}: request {index} exact model key drifted: {snapshot}",
        )
        require(
            snapshot.get("catalog_revision") == model.get("catalog_revision"),
            f"{label}: request {index} catalog revision drifted: {snapshot}",
        )
        effective = snapshot.get("effective_request")
        require(
            isinstance(effective, dict) and effective.get("model") == model["id"],
            f"{label}: request {index} effective model drifted: {snapshot}",
        )
        manifest = snapshot.get("prompt_manifest")
        prompt_hash = manifest.get("prompt_hash") if isinstance(manifest, dict) else None
        require(
            isinstance(prompt_hash, str)
            and len(prompt_hash) == 64
            and all(character in "0123456789abcdef" for character in prompt_hash),
            f"{label}: request {index} lacked a redacted prompt hash: {snapshot}",
        )

    run_budgets = validate_trace_run_budgets(trace, label)
    return {
        "model_key": model["key"],
        "catalog_revision": model.get("catalog_revision"),
        "request_count": len(snapshots),
        "run_budgets": run_budgets,
        "prompt_hashes": [
            snapshot.get("prompt_manifest", {}).get("prompt_hash")
            for snapshot in snapshots
        ],
    }


def verify_persistence_and_trace(
    api: MitsuroApi,
    session_id: str,
    marker: str,
    expected_user_turns: int,
    *,
    exact_model: dict[str, Any],
    exact_tool_calls: int | None = None,
    expected_tool_name: str | None = None,
) -> dict[str, Any]:
    persisted = api.json_request("GET", f"/api/sessions/{session_id}")
    messages = persisted.get("messages", [])
    user_count = sum(message.get("role") == "user" for message in messages)
    assistant_count = sum(message.get("role") == "assistant" for message in messages)
    serialized = json.dumps(messages)
    require(user_count >= expected_user_turns, f"only {user_count} user messages persisted")
    require(
        assistant_count >= expected_user_turns,
        f"only {assistant_count} assistant messages persisted",
    )
    require(marker in serialized, "persisted conversation lost artifact marker")

    state = api.json_request("GET", f"/api/sessions/{session_id}/state")
    require(state.get("agent_state") == "idle", f"session did not return idle: {state}")
    require(not state.get("pending_interactions"), f"session retained pending input: {state}")
    require(state.get("recovery") is None, f"completed session retained recovery state: {state}")

    label = f"session {session_id}"
    trace, summary = wait_for_settled_trace_runs(
        api,
        session_id,
        label,
        expected_runs=expected_user_turns,
    )
    if exact_tool_calls is not None:
        validate_trace_tool_lifecycles(
            trace["events"],
            label,
            exact_calls=exact_tool_calls,
            expected_name=expected_tool_name,
        )
        require(
            summary.get("tool_calls") == exact_tool_calls,
            f"{label}: trace summary tool_calls={summary.get('tool_calls')}, "
            f"expected {exact_tool_calls}",
        )
    for key in ("tool_errors", "server_tool_errors", "agent_errors", "provider_failures"):
        require(summary.get(key) == 0, f"trace {key}={summary.get(key)}")
    require(summary.get("last_stop_reason") == "completed", f"bad trace summary: {summary}")
    summary = dict(summary)
    summary["exact_runtime"] = validate_trace_exact_runtime(trace, exact_model, label)
    return summary


def cleanup_cycle_processes(
    api: MitsuroApi,
    run_dir: Path,
    *,
    port: int | None = None,
    known_process_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Kill and verify every runner-owned process after an unsuccessful cycle.

    Cleanup never throws so callers can preserve the first acceptance failure,
    but its structured status must be checked by strict cycle paths. A successful
    kill request alone is insufficient: registry state and the assigned preview
    port are both verified before cleanup is considered complete.
    """
    report: dict[str, Any] = {
        "status": "running",
        "run_dir": str(run_dir),
        "known_process_ids": list(known_process_ids or []),
        "discovered_active_process_ids": [],
        "kill_attempts": [],
        "active_processes_after_cleanup": [],
        "preview_unavailable": None,
        "warnings": [],
        "errors": [],
    }

    candidates: dict[str, dict[str, Any] | None] = {
        process_id: None for process_id in known_process_ids or []
    }
    try:
        processes = api.json_request("GET", "/api/processes")
        require(isinstance(processes, list), f"process list was malformed: {processes}")
        for process in processes:
            if (
                process.get("working_dir") == str(run_dir)
                and process.get("status_code") in {"running", "suspended"}
            ):
                process_id = str(process.get("id"))
                candidates[process_id] = process
                report["discovered_active_process_ids"].append(process_id)
    except Exception as error:
        report["errors"].append(f"initial process discovery failed: {error}")

    for process_id in candidates:
        attempt: dict[str, Any] = {"process_id": process_id, "status": "running"}
        try:
            current = api.json_request("GET", f"/api/processes/{process_id}")
            attempt["status_before"] = current.get("status_code")
            if current.get("status_code") in {"running", "suspended"}:
                try:
                    api.json_request("POST", f"/api/processes/{process_id}/kill")
                except Exception as error:
                    # A natural exit can race the kill request. Verify the final
                    # state below before deciding whether cleanup actually failed.
                    attempt["kill_request_error"] = str(error)

            deadline = time.monotonic() + 10.0
            latest = current
            while (
                latest.get("status_code") in {"running", "suspended"}
                and time.monotonic() < deadline
            ):
                time.sleep(0.2)
                latest = api.json_request("GET", f"/api/processes/{process_id}")
            attempt["status_after"] = latest.get("status_code")
            if latest.get("status_code") in {"running", "suspended"}:
                raise AcceptanceFailure(
                    f"process remained active after cleanup: {latest}"
                )
            attempt["status"] = "verified"
        except Exception as error:
            attempt.update({"status": "failed", "error": str(error)})
            report["errors"].append(
                f"process {process_id} cleanup failed: {error}"
            )
        report["kill_attempts"].append(attempt)

    try:
        report["active_processes_after_cleanup"] = active_processes_for_dir(
            api, run_dir
        )
        if report["active_processes_after_cleanup"]:
            report["errors"].append(
                "runner-owned processes remained active after cleanup"
            )
    except Exception as error:
        report["errors"].append(f"final process verification failed: {error}")

    if port is not None:
        try:
            assert_preview_port_quiet(f"http://127.0.0.1:{port}/health")
            report["preview_unavailable"] = True
        except Exception as error:
            report["preview_unavailable"] = False
            report["errors"].append(str(error))

    failed_attempts = [
        attempt
        for attempt in report["kill_attempts"]
        if attempt.get("status") != "verified"
    ]
    report["failed_attempts"] = failed_attempts
    cleanup_complete = (
        not report["errors"]
        and not failed_attempts
        and not report["active_processes_after_cleanup"]
        and (port is None or report["preview_unavailable"] is True)
    )
    report["status"] = "pass" if cleanup_complete else "fail"
    return report


def cleanup_resilience_lane(
    api: MitsuroApi,
    run_dir: Path,
    *,
    session_id: str | None = None,
    probe_pids: list[int] | None = None,
    port: int | None = None,
) -> dict[str, Any]:
    """Return acceptance-grade cleanup evidence for one resilience lane."""
    report: dict[str, Any] = {
        "status": "running",
        "run_dir": str(run_dir),
        "session_id": session_id,
        "session_state_before_cleanup": None,
        "session_state_after_cleanup": None,
        "session_cancel_sent": False,
        "probe_cleanup": None,
        "process_cleanup": None,
        "intervention_required": False,
        "errors": [],
    }

    if session_id is not None:
        try:
            state = api.json_request("GET", f"/api/sessions/{session_id}/state")
            report["session_state_before_cleanup"] = state
            clean_before = (
                state.get("agent_state") == "idle"
                and not state.get("pending_interactions")
                and state.get("recovery") is None
            )
            if not clean_before:
                report["intervention_required"] = True
                response = api.json_request(
                    "POST",
                    f"/api/sessions/{quote(session_id, safe='')}/cancel",
                    headers=api.agent_mutation_headers(session_id),
                )
                require(
                    response.get("ok") is True,
                    f"resilience cleanup cancel response was {response}",
                )
                report["session_cancel_sent"] = True
            final_state = wait_for_session_idle(api, session_id)
            require_clean_idle_state(final_state, "resilience cleanup")
            report["session_state_after_cleanup"] = final_state
        except Exception as error:
            report["errors"].append(f"session cleanup verification failed: {error}")

    if probe_pids is not None:
        probe_cleanup = cleanup_probe_pids(run_dir, probe_pids)
        report["probe_cleanup"] = probe_cleanup
        if probe_cleanup.get("running_before_cleanup"):
            report["intervention_required"] = True
        if probe_cleanup.get("status") != "pass":
            report["errors"].append(
                f"probe cleanup was incomplete: {probe_cleanup}"
            )

    process_cleanup = cleanup_cycle_processes(api, run_dir, port=port)
    report["process_cleanup"] = process_cleanup
    if process_cleanup.get("discovered_active_process_ids"):
        report["intervention_required"] = True
    if process_cleanup.get("status") != "pass":
        report["errors"].append(
            f"registered process cleanup was incomplete: {process_cleanup}"
        )

    report["status"] = "pass" if not report["errors"] else "fail"
    return report


def cleanup_gated_error(
    lane: str,
    prior_error: BaseException | None,
    result_was_pass: bool,
    cleanup: dict[str, Any],
) -> BaseException | None:
    """Make cleanup evidence part of the lane's pass/fail contract."""
    cleanup_problem: str | None = None
    if cleanup.get("status") != "pass":
        cleanup_problem = f"cleanup was incomplete: {cleanup}"
    elif result_was_pass and cleanup.get("intervention_required") is True:
        cleanup_problem = (
            "lane appeared to pass but cleanup found unexpected live activity: "
            f"{cleanup}"
        )
    if cleanup_problem is None:
        return prior_error
    if prior_error is None:
        return AcceptanceFailure(f"{lane}: {cleanup_problem}")
    return AcceptanceFailure(f"{lane}: {prior_error}; {cleanup_problem}")


def run_cycle(
    api: MitsuroApi,
    root: Path,
    model: dict[str, Any],
    port: int,
    cycle: int,
    keep_process: bool,
) -> dict[str, Any]:
    marker = f"mitsuro-e2e-{cycle}-{uuid.uuid4().hex[:10]}"
    run_dir = root / f"cycle-{cycle:03d}-{marker.rsplit('-', 1)[-1]}"
    run_dir.mkdir(parents=True, exist_ok=False)
    session_id: str | None = None
    process_id: str | None = None
    trace_cursor = 0
    result: dict[str, Any] = {
        "cycle": cycle,
        "session_id": None,
        "run_dir": str(run_dir),
        "marker": marker,
        "port": port,
        "model": model["id"],
        "model_key": model["key"],
        "model_catalog_revision": model.get("catalog_revision"),
        "phase": "session_create",
    }

    try:
        session_id = create_session(api, run_dir, model, f"Harness E2E cycle {cycle}")
        result["session_id"] = session_id
        result["phase"] = "greeting"
        greeting_events = api.chat(chat_payload(session_id, "Sup boss", model))
        result["greeting_stream"] = summarize_stream_evidence(greeting_events)
        greeting_text = validate_stream(
            greeting_events, "greeting", expect_tools=False
        )
        greeting_trace, _ = wait_for_completed_trace_run(
            api,
            session_id,
            "greeting trace",
            after_sequence=trace_cursor,
        )
        trace_cursor = greeting_trace["latest_sequence"]

        build_prompt = f"""Build and run a tiny dependency-free Python HTTP service in this empty demo workspace.

Acceptance contract:
- Create server.py using only the Python standard library.
- server.py must accept --host and --port command-line arguments.
- GET /health must return JSON with exactly ok=true and marker={marker!r}.
- GET /artifact must return the exact plain-text marker {marker!r}.
- Create tests/test_server.py with meaningful unittest coverage of both response payloads.
  Tests may call a pure routing helper or briefly use an HTTPServer bound to
  127.0.0.1 port 0; do not manually instantiate BaseHTTPRequestHandler and do
  not bind the requested long-lived port {port} during tests.
- Create a concise README.md with run and test commands.
- Run the tests.
- Start it with `python3 server.py --host 127.0.0.1 --port {port}` as a harness-tracked background process, never with a shell ampersand.
- Verify both live endpoints.
- Leave exactly one healthy server running and report the Mitsuro process registry UUID
  (the hyphenated process_id returned by the background tool), files, test result,
  and endpoint checks. If you also report the operating-system PID, label it separately.

Work autonomously. Do not install packages and do not ask styling or product questions."""
        result["phase"] = "build"
        build_events = api.chat(chat_payload(session_id, build_prompt, model))
        result["build_stream"] = summarize_stream_evidence(build_events)
        build_text = validate_stream(build_events, "build", expect_tools=True)
        build_trace, _ = wait_for_completed_trace_run(
            api,
            session_id,
            "build trace",
            after_sequence=trace_cursor,
        )
        trace_cursor = build_trace["latest_sequence"]
        require(marker in build_text, "build response omitted marker")
        require(str(port) in build_text, "build response omitted port")

        result["phase"] = "artifact_and_preview_validation"
        verify_artifacts(run_dir, marker)
        process = process_for_cycle(api, run_dir, port)
        process_id = str(process["id"])
        require(process.get("error") is None, f"running process reported error: {process}")

        health_status, health_type, health_body = fetch_text(
            f"http://127.0.0.1:{port}/health"
        )
        artifact_status, _, artifact_body = fetch_text(
            f"http://127.0.0.1:{port}/artifact"
        )
        require(health_status == 200, f"health status was {health_status}")
        require("application/json" in health_type, f"health content type was {health_type}")
        require(json.loads(health_body) == {"ok": True, "marker": marker}, "bad health JSON")
        require(artifact_status == 200 and artifact_body == marker, "bad artifact response")

        continuity_prompt = (
            "Without calling any tool and without changing or restarting anything, "
            "state the exact marker, port, and Mitsuro process registry UUID from this "
            "session in one sentence. The registry UUID is the hyphenated process_id "
            "returned by the background tool, not the numeric operating-system PID. "
            "Use only conversation context."
        )
        persisted_model_before = require_session_model(
            api, session_id, model, "continuity before follow-up"
        )
        result["phase"] = "continuity"
        continuity_payload = chat_payload(session_id, continuity_prompt, None)
        require(
            "model" not in continuity_payload and "model_key" not in continuity_payload,
            "continuity follow-up unexpectedly sent a model override",
        )
        continuity_events = api.chat(continuity_payload)
        result["continuity_stream"] = summarize_stream_evidence(continuity_events)
        continuity_text = validate_stream(
            continuity_events, "continuity", expect_tools=False
        )
        continuity_trace, _ = wait_for_completed_trace_run(
            api,
            session_id,
            "continuity trace",
            after_sequence=trace_cursor,
        )
        trace_cursor = continuity_trace["latest_sequence"]
        for expected in (marker, str(port), process_id):
            require(expected in continuity_text, f"continuity response omitted {expected}")
        persisted_model_after = require_session_model(
            api, session_id, model, "continuity after follow-up"
        )

        result["phase"] = "persistence_and_process_lifecycle"
        summary = verify_persistence_and_trace(
            api, session_id, marker, 3, exact_model=model
        )

        api.json_request("POST", f"/api/processes/{process_id}/suspend")
        wait_for_process_status(api, process_id, "suspended")
        assert_preview_unresponsive(f"http://127.0.0.1:{port}/health")
        api.json_request("POST", f"/api/processes/{process_id}/resume")
        wait_for_process_status(api, process_id, "running")
        _, _, resumed_health = fetch_text(f"http://127.0.0.1:{port}/health")
        require(json.loads(resumed_health).get("marker") == marker, "resume lost service state")

        if not keep_process:
            api.json_request("POST", f"/api/processes/{process_id}/kill")
            wait_for_process_status(api, process_id, "killed")
            assert_preview_port_quiet(f"http://127.0.0.1:{port}/health")

        result.update(
            {
                "status": "pass",
                "phase": "complete",
                "process_id": process_id,
                "process_retained": keep_process,
                "greeting": greeting_text,
                "build_tool_calls": [call.get("name") for call in tool_calls(build_events)],
                "usage_events": sum(
                    event.get("type") == "usage"
                    for event in greeting_events + build_events + continuity_events
                ),
                "model_continuity": {
                    "request_override_omitted": True,
                    "persisted_before": persisted_model_before,
                    "persisted_after": persisted_model_after,
                },
                "trace_summary": summary,
            }
        )
    except Exception as error:
        failed_phase = str(result.get("phase", "unknown"))
        result["failure_evidence"] = collect_failed_cycle_evidence(
            api,
            run_dir,
            session_id,
            marker,
            port,
        )
        cleanup = cleanup_cycle_processes(
            api,
            run_dir,
            port=port,
            known_process_ids=[process_id] if process_id is not None else None,
        )
        result["cleanup"] = cleanup
        final_error: BaseException = error
        if cleanup.get("status") != "pass":
            final_error = AcceptanceFailure(
                f"{error}; failed-cycle cleanup was incomplete: {cleanup}",
                FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL,
            )
        result.update(
            {
                "status": failure_status(final_error),
                "phase": failed_phase,
                "failure_classification": failure_classification(final_error).value,
                "error": str(final_error),
            }
        )
        (run_dir / "acceptance-result.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n"
        )
        try:
            setattr(final_error, "acceptance_result", result)
        except (AttributeError, TypeError):
            pass
        if final_error is error:
            raise
        raise final_error from error

    (run_dir / "acceptance-result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    return result


def run_disconnect_lane(
    api: MitsuroApi,
    resilience_dir: Path,
    model: dict[str, Any],
) -> dict[str, Any]:
    lane = "sse-disconnect"
    run_dir = resilience_dir / lane
    run_dir.mkdir(parents=True, exist_ok=False)
    marker = f"mitsuro-disconnect-{uuid.uuid4().hex[:12]}"
    ready = f"READY:{marker}"
    artifact = run_dir / "disconnect-complete.txt"
    run_dir.joinpath("disconnect_probe.py").write_text(
        "from pathlib import Path\n"
        "import os\n"
        "import time\n\n"
        f"MARKER = {marker!r}\n"
        "Path('disconnect.pid').write_text(str(os.getpid()))\n"
        f"print({ready!r}, flush=True)\n"
        "time.sleep(2.0)\n"
        "Path('disconnect-complete.txt').write_text(MARKER)\n"
        "print(f'DONE:{MARKER}', flush=True)\n"
    )

    session_id = create_session(api, run_dir, model, "Harness SSE disconnect resilience")
    result: dict[str, Any] = {
        "lane": lane,
        "session_id": session_id,
        "run_dir": str(run_dir),
        "marker": marker,
        "status": "running",
    }
    result_path = run_dir / "acceptance-result.json"
    pids: list[int] = []
    lane_error: BaseException | None = None

    try:
        prompt = (
            "Run exactly python3 disconnect_probe.py as one foreground Bash tool call "
            "with a 60000 ms timeout. Do not inspect or edit the pre-created probe, do "
            "not use background mode, and do not call another tool. The probe itself "
            "is expected to write its completion artifact. After it finishes, reply "
            f"with exactly DISCONNECT-COMPLETED:{marker}."
        )

        def disconnect_on_ready(
            event: dict[str, Any], _events: list[dict[str, Any]]
        ) -> bool:
            return (
                event.get("type") == "tool_output_delta"
                and ready in str(event.get("delta", ""))
            )

        events, disconnected = api.chat_incremental(
            chat_payload(session_id, prompt, model),
            on_event=disconnect_on_ready,
        )
        validate_usage_events(events, lane)
        require(disconnected, f"{lane}: READY output was not observed before stream end")
        pids = read_probe_pids(run_dir)
        require(
            len(pids) == 1,
            f"{lane}: READY arrived without exactly one foreground probe PID: {pids}",
        )
        require(
            any(
                event.get("type") == "tool_output_delta"
                and ready in str(event.get("delta", ""))
                for event in events
            ),
            f"{lane}: disconnect did not happen on the foreground READY signal",
        )
        require(
            not any(event.get("type") == "finish" for event in events),
            f"{lane}: stream finished before the deliberate disconnect",
        )

        wait_for_exact_text(
            artifact, marker, 20.0, "disconnect completion artifact"
        )
        state = wait_for_session_idle(api, session_id)
        require_clean_idle_state(state, lane)
        require_probe_quiet(run_dir, pids, [], grace=0.5)
        require(
            not active_processes_for_dir(api, run_dir),
            f"{lane}: foreground probe was incorrectly retained as a background process",
        )

        messages = session_messages(api, session_id)
        serialized = json.dumps(messages)
        require(
            f"DISCONNECT-COMPLETED:{marker}" in serialized,
            f"{lane}: persisted assistant completion was lost after disconnect",
        )
        summary = verify_persistence_and_trace(
            api,
            session_id,
            marker,
            1,
            exact_model=model,
            exact_tool_calls=1,
            expected_tool_name="bash",
        )

        recovery_marker = f"DISCONNECT-RECOVERED:{marker}"
        followup_events = api.chat(
            chat_payload(
                session_id,
                f"Without calling any tool, reply exactly {recovery_marker}",
                model,
            )
        )
        followup_text = validate_stream(
            followup_events, f"{lane} follow-up", expect_tools=False
        )
        require(
            followup_text == recovery_marker,
            f"{lane}: recovery follow-up drifted from {recovery_marker}: "
            f"{followup_text!r}",
        )
        final_summary = verify_persistence_and_trace(
            api,
            session_id,
            marker,
            2,
            exact_model=model,
            exact_tool_calls=1,
            expected_tool_name="bash",
        )

        result.update(
            {
                "status": "pass",
                "artifact": str(artifact),
                "disconnect_event_types": [
                    event.get("type") for event in events
                ],
                "trace_summary_after_disconnect": summary,
                "trace_summary_after_followup": final_summary,
                "followup": followup_text,
            }
        )
    except Exception as error:
        lane_error = error
        result.update(
            {
                "status": failure_status(error),
                "failure_classification": failure_classification(error).value,
                "error": str(error),
            }
        )
    finally:
        result_was_pass = result.get("status") == "pass"
        cleanup = cleanup_resilience_lane(
            api,
            run_dir,
            session_id=session_id,
            probe_pids=pids,
        )
        result["cleanup"] = cleanup
        lane_error = cleanup_gated_error(
            lane, lane_error, result_was_pass, cleanup
        )
        if lane_error is not None:
            result.update(
                {
                    "status": failure_status(lane_error),
                    "failure_classification": failure_classification(
                        lane_error
                    ).value,
                    "error": str(lane_error),
                }
            )
        write_json(result_path, result)
    if lane_error is not None:
        raise lane_error
    return result


def run_failed_bash_lane(
    api: MitsuroApi,
    resilience_dir: Path,
    model: dict[str, Any],
) -> dict[str, Any]:
    lane = "failed-bash-recovery"
    run_dir = resilience_dir / lane
    run_dir.mkdir(parents=True, exist_ok=False)
    marker = f"mitsuro-failed-bash-{uuid.uuid4().hex[:12]}"
    prelude_reply = f"PRELUDE-READY:{marker}"
    stderr_marker = f"EXPECTED-STDERR:{marker}"
    failure_reply = f"FAILED-BASH-HANDLED:{marker}"
    recovery_reply = f"FAILED-BASH-RECOVERED:{marker}"
    command = (
        'python3 -c "import sys; '
        f"print('{stderr_marker}', file=sys.stderr); "
        'sys.exit(7)"'
    )

    session_id = create_session(api, run_dir, model, "Harness failed Bash resilience")
    result: dict[str, Any] = {
        "lane": lane,
        "session_id": session_id,
        "run_dir": str(run_dir),
        "marker": marker,
        "status": "running",
    }
    result_path = run_dir / "acceptance-result.json"
    lane_error: BaseException | None = None

    try:
        prelude_events = api.chat(
            chat_payload(
                session_id,
                f"Without calling any tool, reply exactly {prelude_reply}",
                model,
            )
        )
        prelude_text = validate_stream(
            prelude_events, f"{lane} prelude", expect_tools=False
        )
        require(
            prelude_text == prelude_reply,
            f"{lane}: prelude reply drifted: {prelude_text!r}",
        )
        require_clean_idle_state(
            wait_for_session_idle(api, session_id), f"{lane} prelude"
        )

        prelude_messages = session_messages(api, session_id)
        require(
            len(prelude_messages) >= 2,
            f"{lane}: prelude did not persist a complete exchange: {prelude_messages}",
        )
        require(
            prelude_reply in json.dumps(prelude_messages),
            f"{lane}: prelude reply was absent from persistence",
        )
        prelude_snapshot = canonical_json_bytes(prelude_messages)
        prelude_sha256 = hashlib.sha256(prelude_snapshot).hexdigest()

        prelude_trace, prelude_summary = wait_for_completed_trace_run(
            api,
            session_id,
            f"{lane} prelude trace",
        )
        for key in (
            "tool_errors",
            "server_tool_errors",
            "agent_errors",
            "provider_failures",
        ):
            require(
                prelude_summary.get(key) == 0,
                f"{lane}: prelude trace {key}={prelude_summary.get(key)}",
            )
        require(
            prelude_summary.get("last_stop_reason") == "completed",
            f"{lane}: prelude trace did not complete: {prelude_summary}",
        )
        prelude_trace_events = prelude_trace["events"]

        failed_prompt = f"""This is an intentional negative-path acceptance test.
Call the Bash tool exactly once with this exact foreground command:
`{command}`

Use a 60000 ms timeout and do not use background mode. Do not inspect files, call
another tool, retry, alter, or wrap the command. Do not narrate before the call.
The command is expected to write its marker to stderr and exit 7. After that one
expected failure, reply exactly {failure_reply}"""
        failed_events = api.chat(
            chat_payload(session_id, failed_prompt, model)
        )
        failure_text, tool_call_id, envelope = validate_failed_bash_stream(
            failed_events,
            lane,
            command,
            stderr_marker,
            failure_reply,
        )
        require_clean_idle_state(
            wait_for_session_idle(api, session_id), f"{lane} failed turn"
        )
        require(
            not active_processes_for_dir(api, run_dir),
            f"{lane}: foreground failed command entered the process registry",
        )

        failed_messages = session_messages(api, session_id)
        require(
            len(failed_messages) > len(prelude_messages),
            f"{lane}: failed turn was not appended to persistence",
        )
        persisted_prelude_after_failure = canonical_json_bytes(
            failed_messages[: len(prelude_messages)]
        )
        require(
            persisted_prelude_after_failure == prelude_snapshot
            and failed_messages[: len(prelude_messages)] == prelude_messages,
            f"{lane}: failed tool turn mutated persisted prelude content",
        )
        failed_appended_messages = failed_messages[len(prelude_messages) :]
        failed_appended_blocks = [
            block
            for message in failed_appended_messages
            for block in (
                message.get("content", [])
                if isinstance(message.get("content"), list)
                else []
            )
            if isinstance(block, dict)
        ]
        persisted_tool_uses = [
            block
            for block in failed_appended_blocks
            if block.get("type") == "tool_use"
        ]
        persisted_tool_results = [
            block
            for block in failed_appended_blocks
            if block.get("type") == "tool_result"
        ]
        require(
            len(persisted_tool_uses) == len(persisted_tool_results) == 1,
            f"{lane}: persisted tool lifecycle duplicated or disappeared: "
            f"uses={persisted_tool_uses}, results={persisted_tool_results}",
        )
        require(
            persisted_tool_uses[0].get("id") == tool_call_id
            and persisted_tool_uses[0].get("name") == "bash",
            f"{lane}: persisted Bash call did not match stream: {persisted_tool_uses}",
        )
        require(
            persisted_tool_results[0].get("tool_use_id") == tool_call_id
            and persisted_tool_results[0].get("is_error") is True,
            f"{lane}: persisted failed result did not match stream: "
            f"{persisted_tool_results}",
        )
        persisted_failure_prose = [
            "".join(
                str(block.get("text", ""))
                for block in message.get("content", [])
                if isinstance(block, dict) and block.get("type") == "text"
            ).strip()
            for message in failed_appended_messages
            if message.get("role") == "assistant"
            and isinstance(message.get("content"), list)
        ]
        persisted_failure_prose = [
            text for text in persisted_failure_prose if text
        ]
        require(
            persisted_failure_prose == [failure_reply],
            f"{lane}: expected one clean persisted assistant response, got "
            f"{persisted_failure_prose}",
        )
        require(
            all(
                json.dumps(envelope, sort_keys=True) not in text
                for text in persisted_failure_prose
            ),
            f"{lane}: raw result envelope leaked into persisted assistant prose",
        )

        failed_trace, failed_summary = wait_for_completed_trace_run(
            api,
            session_id,
            f"{lane} failed-turn trace",
            after_sequence=prelude_trace_events[-1]["sequence"],
        )
        failed_trace_events = failed_trace["events"]
        require(
            failed_trace_events[: len(prelude_trace_events)] == prelude_trace_events,
            f"{lane}: failed turn mutated earlier trace events",
        )
        failed_delta = failed_trace_events[len(prelude_trace_events) :]
        failed_delta_types = [event.get("event_type") for event in failed_delta]
        require(bool(failed_delta_types), f"{lane}: failed turn wrote no trace events")

        trace_lifecycle: dict[str, list[tuple[int, dict[str, Any]]]] = {}
        for event_type in (
            "tool_call_start",
            "tool_call_complete",
            "tool_executing",
            "tool_result",
            "finished",
        ):
            trace_lifecycle[event_type] = [
                (index, event)
                for index, event in enumerate(failed_delta)
                if event.get("event_type") == event_type
            ]
            if event_type == "tool_executing":
                require(
                    bool(trace_lifecycle[event_type]),
                    f"{lane}: trace expected at least one {event_type}, got []",
                )
            else:
                require(
                    len(trace_lifecycle[event_type]) == 1,
                    f"{lane}: trace expected one {event_type}, got "
                    f"{trace_lifecycle[event_type]}",
                )
        trace_positions = {
            event_type: matches[0][0]
            for event_type, matches in trace_lifecycle.items()
            if event_type != "tool_executing"
        }
        trace_positions["tool_executing_first"] = trace_lifecycle["tool_executing"][0][0]
        trace_positions["tool_executing_last"] = trace_lifecycle["tool_executing"][-1][0]
        require(
            trace_positions["tool_call_start"]
            < trace_positions["tool_call_complete"]
            < trace_positions["tool_executing_first"]
            <= trace_positions["tool_executing_last"]
            < trace_positions["tool_result"]
            < trace_positions["finished"],
            f"{lane}: failed-turn trace lifecycle was out of order: {trace_positions}",
        )
        for event_type in (
            "tool_call_start",
            "tool_call_complete",
            "tool_result",
        ):
            payload = trace_lifecycle[event_type][0][1].get("payload", {})
            require(
                payload.get("id") == tool_call_id,
                f"{lane}: trace {event_type} id did not match {tool_call_id}: {payload}",
            )
        for _, executing_event in trace_lifecycle["tool_executing"]:
            payload = executing_event.get("payload", {})
            require(
                payload.get("id") == tool_call_id and payload.get("name") == "bash",
                f"{lane}: trace tool_executing did not match Bash call "
                f"{tool_call_id}: {payload}",
            )
        require(
            trace_lifecycle["tool_call_complete"][0][1]
            .get("payload", {})
            .get("name")
            == "bash",
            f"{lane}: trace completed tool was not Bash",
        )
        require(
            trace_lifecycle["tool_result"][0][1]
            .get("payload", {})
            .get("is_error")
            is True,
            f"{lane}: trace lost failed tool-result state",
        )
        finished_trace = trace_lifecycle["finished"][0][1]
        require(
            finished_trace.get("stop_reason") == "completed"
            and finished_trace.get("payload", {}).get("stop_reason") == "completed",
            f"{lane}: failed turn did not trace a completed stop: {finished_trace}",
        )

        require(
            failed_summary.get("tool_calls") == 1
            and failed_summary.get("tool_errors") == 1,
            f"{lane}: trace did not expose exactly one failed tool: {failed_summary}",
        )
        for key in ("server_tool_errors", "agent_errors", "provider_failures"):
            require(
                failed_summary.get(key) == 0,
                f"{lane}: failed-turn trace {key}={failed_summary.get(key)}",
            )
        require(
            failed_summary.get("last_stop_reason") == "completed",
            f"{lane}: failed-turn trace did not complete: {failed_summary}",
        )
        for event_type in (
            "tool_call_start",
            "tool_call_complete",
            "tool_executing",
            "tool_result",
        ):
            count = trace_event_count(failed_summary, event_type)
            if count is not None:
                if event_type == "tool_executing":
                    require(
                        count >= 1,
                        f"{lane}: trace count {event_type}={count}, expected at least 1",
                    )
                else:
                    require(
                        count == 1,
                        f"{lane}: trace count {event_type}={count}, expected 1",
                    )

        failed_messages_snapshot = canonical_json_bytes(failed_messages)
        recovery_events = api.chat(
            chat_payload(
                session_id,
                f"Without calling any tool, reply exactly {recovery_reply}",
                model,
            )
        )
        recovery_text = validate_stream(
            recovery_events, f"{lane} recovery", expect_tools=False
        )
        require(
            recovery_text == recovery_reply,
            f"{lane}: recovery reply drifted: {recovery_text!r}",
        )
        require_clean_idle_state(
            wait_for_session_idle(api, session_id), f"{lane} recovery"
        )

        recovered_messages = session_messages(api, session_id)
        require(
            canonical_json_bytes(recovered_messages[: len(prelude_messages)])
            == prelude_snapshot
            and recovered_messages[: len(prelude_messages)] == prelude_messages,
            f"{lane}: recovery mutated persisted prelude content",
        )
        require(
            canonical_json_bytes(recovered_messages[: len(failed_messages)])
            == failed_messages_snapshot
            and recovered_messages[: len(failed_messages)] == failed_messages,
            f"{lane}: recovery mutated persisted failed-turn history",
        )
        recovery_appended_messages = recovered_messages[len(failed_messages) :]
        recovery_appended_blocks = [
            block
            for message in recovery_appended_messages
            for block in (
                message.get("content", [])
                if isinstance(message.get("content"), list)
                else []
            )
            if isinstance(block, dict)
        ]
        require(
            not any(
                block.get("type") in {"tool_use", "tool_result"}
                for block in recovery_appended_blocks
            ),
            f"{lane}: recovery persisted an unexpected tool lifecycle: "
            f"{recovery_appended_blocks}",
        )
        persisted_recovery_prose = [
            "".join(
                str(block.get("text", ""))
                for block in message.get("content", [])
                if isinstance(block, dict) and block.get("type") == "text"
            ).strip()
            for message in recovery_appended_messages
            if message.get("role") == "assistant"
            and isinstance(message.get("content"), list)
        ]
        persisted_recovery_prose = [text for text in persisted_recovery_prose if text]
        require(
            persisted_recovery_prose == [recovery_reply],
            f"{lane}: exact recovery was not preserved: {persisted_recovery_prose}",
        )

        recovery_trace, recovery_summary = wait_for_completed_trace_run(
            api,
            session_id,
            f"{lane} recovery trace",
            after_sequence=failed_trace_events[-1]["sequence"],
        )
        recovery_trace_events = recovery_trace["events"]
        require(
            recovery_trace_events[: len(failed_trace_events)] == failed_trace_events,
            f"{lane}: recovery mutated prior trace events",
        )
        recovery_delta = recovery_trace_events[len(failed_trace_events) :]
        recovery_tool_events = [
            event
            for event in recovery_delta
            if event.get("event_type")
            in {
                "tool_call_start",
                "tool_call_complete",
                "tool_executing",
                "tool_result",
                "server_tool_start",
                "server_tool_complete",
                "server_tool_error",
            }
        ]
        require(
            not recovery_tool_events,
            f"{lane}: recovery emitted tool lifecycle events: {recovery_tool_events}",
        )
        recovery_finishes = [
            event
            for event in recovery_delta
            if event.get("event_type") == "finished"
        ]
        require(
            len(recovery_finishes) == 1
            and recovery_finishes[0].get("stop_reason") == "completed",
            f"{lane}: recovery trace terminal event was invalid: {recovery_finishes}",
        )
        require(
            recovery_summary.get("tool_calls") == 1
            and recovery_summary.get("tool_errors") == 1,
            f"{lane}: recovery changed failed-tool counts: {recovery_summary}",
        )
        for key in ("server_tool_errors", "agent_errors", "provider_failures"):
            require(
                recovery_summary.get(key) == 0,
                f"{lane}: recovery trace {key}={recovery_summary.get(key)}",
            )
        require(
            recovery_summary.get("last_stop_reason") == "completed",
            f"{lane}: recovery trace did not complete: {recovery_summary}",
        )

        result.update(
            {
                "status": "pass",
                "prelude": prelude_text,
                "prelude_message_count": len(prelude_messages),
                "prelude_snapshot_sha256": prelude_sha256,
                "failed_command": command,
                "failed_tool_call_id": tool_call_id,
                "failed_tool_result": envelope,
                "failed_reply": failure_text,
                "recovery": recovery_text,
                "prelude_unchanged_after_failure": True,
                "prelude_unchanged_after_recovery": True,
                "failed_turn_unchanged_after_recovery": True,
                "persisted_tool_call_count": len(persisted_tool_uses),
                "persisted_tool_result_count": len(persisted_tool_results),
                "persisted_completed_response_count": len(persisted_failure_prose),
                "trace_summary_after_failure": failed_summary,
                "trace_summary_after_recovery": recovery_summary,
            }
        )
    except Exception as error:
        # An intentional tool failure is accepted only when every invariant
        # above holds. Any lane defect is product-terminal and must not be
        # disguised as a retryable provider-capacity event.
        terminal = AcceptanceFailure(f"{lane}: {error}")
        lane_error = terminal
        result.update(
            {
                "status": "fail",
                "failure_classification": terminal.classification.value,
                "error": str(terminal),
            }
        )
    finally:
        result_was_pass = result.get("status") == "pass"
        cleanup = cleanup_resilience_lane(
            api,
            run_dir,
            session_id=session_id,
        )
        result["cleanup"] = cleanup
        lane_error = cleanup_gated_error(
            lane, lane_error, result_was_pass, cleanup
        )
        if lane_error is not None:
            result.update(
                {
                    "status": "fail",
                    "failure_classification": failure_classification(
                        lane_error
                    ).value,
                    "error": str(lane_error),
                }
            )
        write_json(result_path, result)
    if lane_error is not None:
        raise lane_error
    return result


def run_live_steering_lane(
    api: MitsuroApi,
    resilience_dir: Path,
    model: dict[str, Any],
) -> dict[str, Any]:
    lane = "live-steering"
    run_dir = resilience_dir / lane
    run_dir.mkdir(parents=True, exist_ok=False)
    marker = f"mitsuro-live-steering-{uuid.uuid4().hex[:12]}"
    ready = f"READY:{marker}"
    completed = f"DONE:{marker}"
    original_reply = f"ORIGINAL-REPLY:{marker}"
    steered_reply = f"STEERED-REPLY:{marker}"
    steering_message = (
        "Do not call another tool. Ignore the earlier reply instruction and reply "
        f"exactly {steered_reply}"
    )
    command = "python3 live_steering_probe.py"
    artifact = run_dir / "live-steering-complete.txt"
    run_dir.joinpath("live_steering_probe.py").write_text(
        "from pathlib import Path\n"
        "import os\n"
        "import time\n\n"
        f"MARKER = {marker!r}\n"
        "Path('parent.pid').write_text(str(os.getpid()))\n"
        f"print({ready!r}, flush=True)\n"
        "time.sleep(2.0)\n"
        "Path('live-steering-complete.txt').write_text(MARKER)\n"
        f"print({completed!r}, flush=True)\n"
    )

    session_id = create_session(api, run_dir, model, "Harness live steering resilience")
    result: dict[str, Any] = {
        "lane": lane,
        "session_id": session_id,
        "run_dir": str(run_dir),
        "marker": marker,
        "status": "running",
    }
    result_path = run_dir / "acceptance-result.json"
    pids: list[int] = []
    steer_response: dict[str, Any] | None = None
    lane_error: BaseException | None = None

    try:
        prompt = (
            f"The acceptance marker for this probe is {marker}. "
            f"Run exactly {command} as one foreground Bash tool call with a 60000 ms "
            "timeout. Do not inspect or edit the pre-created probe, do not use "
            "background mode, do not narrate before the call, and do not call another "
            f"tool. After the probe finishes, reply exactly {original_reply}"
        )

        def steer_on_ready(
            event: dict[str, Any], _events: list[dict[str, Any]]
        ) -> bool:
            nonlocal steer_response, pids
            if (
                steer_response is not None
                or event.get("type") != "tool_output_delta"
                or ready not in str(event.get("delta", ""))
            ):
                return False

            pids = read_probe_pids(run_dir)
            require(
                len(pids) == 1,
                f"{lane}: READY arrived without exactly one foreground probe PID: {pids}",
            )
            response = api.json_request(
                "POST",
                "/api/chat/steer",
                {
                    "session_id": session_id,
                    "message": steering_message,
                    "content": [],
                },
                headers=api.agent_mutation_headers(session_id),
            )
            require(
                isinstance(response, dict),
                f"{lane}: steer response was malformed: {response}",
            )
            require(
                response.get("status") in {"accepted", "queued"},
                f"{lane}: steer was neither accepted nor queued: {response}",
            )
            require(
                isinstance(response.get("pending_id"), str)
                and bool(response["pending_id"]),
                f"{lane}: steer response had no pending_id: {response}",
            )
            steer_response = response
            return False

        events, disconnected = api.chat_incremental(
            chat_payload(session_id, prompt, model),
            on_event=steer_on_ready,
        )
        require(not disconnected, f"{lane}: client disconnected unexpectedly")
        require(
            steer_response is not None,
            f"{lane}: no steering request was sent after the READY tool-output delta",
        )
        pending_id = str(steer_response["pending_id"])
        stream_evidence = validate_live_steering_stream(
            events,
            lane,
            pending_id=pending_id,
            steering_message=steering_message,
            expected_reply=steered_reply,
            expected_command=command,
            ready_marker=ready,
        )

        wait_for_exact_text(artifact, marker, 5.0, "live steering completion artifact")
        state = wait_for_session_idle(api, session_id)
        require_clean_idle_state(state, lane)
        require_probe_quiet(run_dir, pids, [], grace=0.5)
        require(
            not active_processes_for_dir(api, run_dir),
            f"{lane}: foreground probe was retained as a background process",
        )

        persisted = api.json_request("GET", f"/api/sessions/{session_id}")
        messages = persisted.get("messages", []) if isinstance(persisted, dict) else []
        require(
            isinstance(messages, list),
            f"{lane}: GET session returned malformed messages: {persisted}",
        )
        message_evidence = validate_live_steering_messages(
            messages,
            lane,
            steering_message=steering_message,
            expected_reply=steered_reply,
        )

        trace, trace_summary_result = wait_for_settled_trace_runs(
            api,
            session_id,
            f"{lane} trace",
            expected_runs=1,
        )
        validate_trace_tool_lifecycles(
            trace["events"],
            f"{lane} trace",
            exact_calls=1,
            expected_name="bash",
        )
        trace_injections = [
            (index, event)
            for index, event in enumerate(trace["events"])
            if event.get("event_type") == "steering_injected"
            and event.get("payload", {}).get("pending_id") == pending_id
        ]
        require(
            len(trace_injections) == 1,
            f"{lane}: trace did not preserve one steering injection: {trace_injections}",
        )
        trace_injection_index = trace_injections[0][0]
        trace_continuations = [
            index
            for index, event in enumerate(trace["events"])
            if index < trace_injection_index
            and event.get("event_type") == "turn_complete"
            and event.get("payload", {}).get("has_more") is True
        ]
        require(
            len(trace_continuations) == 1,
            f"{lane}: trace lost TurnComplete(has_more=true) ordering: "
            f"{trace_continuations}",
        )
        require(
            trace_continuations[0] < trace_injection_index,
            f"{lane}: trace steering preceded its continuing turn boundary",
        )
        for key in (
            "tool_errors",
            "server_tool_errors",
            "agent_errors",
            "provider_failures",
        ):
            require(
                trace_summary_result.get(key) == 0,
                f"{lane}: trace {key}={trace_summary_result.get(key)}",
            )
        require(
            trace_summary_result.get("tool_calls") == 1
            and trace_summary_result.get("last_stop_reason") == "completed",
            f"{lane}: trace did not expose one completed tool turn: "
            f"{trace_summary_result}",
        )

        result.update(
            {
                "status": "pass",
                "steer_response": steer_response,
                "steering_message": steering_message,
                "steered_reply": steered_reply,
                "stream_evidence": stream_evidence,
                "message_evidence": message_evidence,
                "trace_summary": trace_summary_result,
                "event_types": [event.get("type") for event in events],
                "artifact": str(artifact),
            }
        )
    except Exception as error:
        lane_error = error
        result.update(
            {
                "status": failure_status(error),
                "failure_classification": failure_classification(error).value,
                "error": str(error),
            }
        )
    finally:
        result_was_pass = result.get("status") == "pass"
        cleanup = cleanup_resilience_lane(
            api,
            run_dir,
            session_id=session_id,
            probe_pids=pids,
        )
        result["cleanup"] = cleanup
        lane_error = cleanup_gated_error(
            lane, lane_error, result_was_pass, cleanup
        )
        if lane_error is not None:
            result.update(
                {
                    "status": failure_status(lane_error),
                    "failure_classification": failure_classification(
                        lane_error
                    ).value,
                    "error": str(lane_error),
                }
            )
        write_json(result_path, result)
    if lane_error is not None:
        raise lane_error
    return result


def run_cancel_lane(
    api: MitsuroApi,
    resilience_dir: Path,
    model: dict[str, Any],
) -> dict[str, Any]:
    lane = "explicit-cancel"
    run_dir = resilience_dir / lane
    run_dir.mkdir(parents=True, exist_ok=False)
    marker = f"mitsuro-cancel-{uuid.uuid4().hex[:12]}"
    ready = f"READY:{marker}"
    orphan_artifact = run_dir / "cancel-orphan.txt"
    parent_artifact = run_dir / "cancel-parent-complete.txt"

    child_script = run_dir / "cancel_child.py"
    child_script.write_text(
        "from pathlib import Path\n"
        "import time\n\n"
        f"MARKER = {marker!r}\n"
        "time.sleep(3.0)\n"
        "Path('cancel-orphan.txt').write_text(MARKER)\n"
    )
    run_dir.joinpath("cancel_probe.py").write_text(
        "from pathlib import Path\n"
        "import os\n"
        "import subprocess\n"
        "import sys\n"
        "import time\n\n"
        f"MARKER = {marker!r}\n"
        "child_script = Path(__file__).with_name('cancel_child.py')\n"
        "child = subprocess.Popen([sys.executable, str(child_script)])\n"
        "Path('parent.pid').write_text(str(os.getpid()))\n"
        "Path('child.pid').write_text(str(child.pid))\n"
        f"print({ready!r}, flush=True)\n"
        "time.sleep(30.0)\n"
        "Path('cancel-parent-complete.txt').write_text(MARKER)\n"
    )

    session_id = create_session(api, run_dir, model, "Harness explicit cancel resilience")
    result: dict[str, Any] = {
        "lane": lane,
        "session_id": session_id,
        "run_dir": str(run_dir),
        "marker": marker,
        "status": "running",
    }
    result_path = run_dir / "acceptance-result.json"
    pids: list[int] = []
    cancellation_sent = False
    lane_error: BaseException | None = None

    try:
        prompt = (
            f"The acceptance marker for this probe is {marker}. "
            "Run exactly python3 cancel_probe.py as one foreground Bash tool call "
            "with a 60000 ms timeout. Do not inspect or edit the pre-created probe, do "
            "not use background mode, and do not call another tool. Wait for it to "
            "finish before replying."
        )

        def cancel_on_ready(
            event: dict[str, Any], _events: list[dict[str, Any]]
        ) -> bool:
            nonlocal cancellation_sent, pids
            if (
                cancellation_sent
                or event.get("type") != "tool_output_delta"
                or ready not in str(event.get("delta", ""))
            ):
                return False

            pids = read_probe_pids(run_dir)
            require(len(pids) == 2, f"{lane}: READY arrived without both probe PIDs")
            response = api.json_request(
                "POST",
                f"/api/sessions/{quote(session_id, safe='')}/cancel",
                headers=api.agent_mutation_headers(session_id),
            )
            require(response.get("ok") is True, f"{lane}: cancel response was {response}")
            cancellation_sent = True
            return False

        events, disconnected = api.chat_incremental(
            chat_payload(session_id, prompt, model),
            on_event=cancel_on_ready,
        )
        require(not disconnected, f"{lane}: client disconnected unexpectedly")
        require(cancellation_sent, f"{lane}: cancel was never sent after READY")
        cancelled_result = validate_cancelled_stream(events, lane)

        state = wait_for_session_idle(api, session_id)
        require_clean_idle_state(state, lane)
        require_probe_quiet(
            run_dir,
            pids,
            [orphan_artifact, parent_artifact],
            grace=5.0,
        )
        require(
            not active_processes_for_dir(api, run_dir),
            f"{lane}: cancelled foreground probe was retained as a background process",
        )

        messages = session_messages(api, session_id)
        serialized = json.dumps(messages)
        require(marker in serialized, f"{lane}: persisted conversation lost marker")
        require(
            '"cancelled"' in serialized
            and "Tool execution cancelled by user" in serialized,
            f"{lane}: persisted conversation lost structured cancellation",
        )
        user_count = sum(message.get("role") == "user" for message in messages)
        assistant_count = sum(message.get("role") == "assistant" for message in messages)
        require(user_count >= 1, f"{lane}: cancelled user turn was not persisted")
        require(assistant_count >= 1, f"{lane}: cancelled assistant tool call was not persisted")

        cancelled_summary = trace_summary(
            api,
            session_id,
            expected_runs=1,
            exact_tool_calls=1,
            expected_tool_name="bash",
        )
        require(
            cancelled_summary.get("last_stop_reason") == "user_abort",
            f"{lane}: trace did not end in user_abort: {cancelled_summary}",
        )
        for key in ("server_tool_errors", "agent_errors", "provider_failures"):
            require(
                cancelled_summary.get(key) == 0,
                f"{lane}: trace {key}={cancelled_summary.get(key)}",
            )
        require(
            cancelled_summary.get("tool_errors") == 1,
            f"{lane}: trace did not expose exactly one cancelled tool error: "
            f"{cancelled_summary}",
        )

        recovery_marker = f"CANCEL-RECOVERED:{marker}"
        followup_events = api.chat(
            chat_payload(
                session_id,
                f"Without calling any tool, reply exactly {recovery_marker}",
                model,
            )
        )
        followup_text = validate_stream(
            followup_events, f"{lane} recovery follow-up", expect_tools=False
        )
        require(
            followup_text == recovery_marker,
            f"{lane}: recovery follow-up drifted from {recovery_marker}: "
            f"{followup_text!r}",
        )
        final_state = wait_for_session_idle(api, session_id)
        require_clean_idle_state(final_state, f"{lane} recovery follow-up")
        final_summary = trace_summary(
            api,
            session_id,
            expected_runs=2,
            exact_tool_calls=1,
            expected_tool_name="bash",
        )
        require(
            final_summary.get("last_stop_reason") == "completed",
            f"{lane}: recovery follow-up did not complete: {final_summary}",
        )
        for key in ("server_tool_errors", "agent_errors", "provider_failures"):
            require(
                final_summary.get(key) == 0,
                f"{lane}: recovery trace {key}={final_summary.get(key)}",
            )

        result.update(
            {
                "status": "pass",
                "parent_pid": int(run_dir.joinpath("parent.pid").read_text()),
                "child_pid": int(run_dir.joinpath("child.pid").read_text()),
                "cancelled_tool_result": cancelled_result,
                "event_types": [event.get("type") for event in events],
                "trace_summary_after_cancel": cancelled_summary,
                "trace_summary_after_followup": final_summary,
                "followup": followup_text,
            }
        )
    except Exception as error:
        lane_error = error
        result.update(
            {
                "status": failure_status(error),
                "failure_classification": failure_classification(error).value,
                "error": str(error),
            }
        )
    finally:
        result_was_pass = result.get("status") == "pass"
        cleanup = cleanup_resilience_lane(
            api,
            run_dir,
            session_id=session_id,
            probe_pids=pids,
        )
        result["cleanup"] = cleanup
        lane_error = cleanup_gated_error(
            lane, lane_error, result_was_pass, cleanup
        )
        if lane_error is not None:
            result.update(
                {
                    "status": failure_status(lane_error),
                    "failure_classification": failure_classification(
                        lane_error
                    ).value,
                    "error": str(lane_error),
                }
            )
        write_json(result_path, result)
    if lane_error is not None:
        raise lane_error
    return result


def run_direct_tool_policy_lane(
    api: MitsuroApi,
    resilience_dir: Path,
    port: int,
) -> dict[str, Any]:
    lane = "direct-tool-wildcard-rejection"
    run_dir = resilience_dir / lane
    run_dir.mkdir(parents=True, exist_ok=False)
    result: dict[str, Any] = {
        "lane": lane,
        "run_dir": str(run_dir),
        "port": port,
        "status": "running",
    }
    result_path = run_dir / "acceptance-result.json"
    payload = {
        "tool_name": "bash",
        "params": {
            "command": f"python3 -m http.server {port} --bind 0.0.0.0",
            "description": "Attempt forbidden wildcard preview listener",
            "run_in_background": True,
        },
        "working_dir": str(run_dir),
    }
    lane_error: BaseException | None = None

    try:
        tools = api.json_request("GET", "/api/tools")
        require(
            any(tool.get("name") == "bash" for tool in tools),
            f"{lane}: runtime tool catalog did not expose bash",
        )
        response = api.json_request("POST", "/api/tools/execute", payload)
        require(response.get("is_error") is True, f"{lane}: call was not rejected: {response}")
        output = response.get("output")
        require(isinstance(output, str), f"{lane}: response output was not text: {response}")
        try:
            envelope = json.loads(output)
        except json.JSONDecodeError as error:
            raise AcceptanceFailure(
                f"{lane}: rejection was not a structured envelope: {output}"
            ) from error
        require(
            envelope.get("ok") is False
            and envelope.get("error", {}).get("code") == "blocked_by_policy",
            f"{lane}: unexpected policy envelope: {envelope}",
        )
        message = str(envelope.get("error", {}).get("message", "")).lower()
        require(
            "127.0.0.1" in message or "loopback" in message,
            f"{lane}: rejection did not explain loopback binding: {envelope}",
        )

        matches = [
            process
            for process in api.json_request("GET", "/api/processes")
            if process.get("working_dir") == str(run_dir)
            or str(port)
            in f"{process.get('command', '')} {process.get('description', '')}"
        ]
        require(not matches, f"{lane}: rejected call registered processes: {matches}")
        assert_preview_port_quiet(f"http://127.0.0.1:{port}", timeout=1.0)

        result.update(
            {
                "status": "pass",
                "request_contract": payload,
                "rejection": envelope,
            }
        )
    except Exception as error:
        lane_error = error
        result.update(
            {
                "status": failure_status(error),
                "failure_classification": failure_classification(error).value,
                "error": str(error),
            }
        )
    finally:
        result_was_pass = result.get("status") == "pass"
        cleanup = cleanup_resilience_lane(api, run_dir, port=port)
        result["cleanup"] = cleanup
        lane_error = cleanup_gated_error(
            lane, lane_error, result_was_pass, cleanup
        )
        if lane_error is not None:
            result.update(
                {
                    "status": failure_status(lane_error),
                    "failure_classification": failure_classification(
                        lane_error
                    ).value,
                    "error": str(lane_error),
                }
            )
        write_json(result_path, result)
    if lane_error is not None:
        raise lane_error
    return result


def run_resilience_suite(
    api: MitsuroApi,
    resilience_dir: Path,
    model: dict[str, Any],
    direct_tool_port: int,
) -> list[dict[str, Any]]:
    resilience_dir.mkdir(parents=True, exist_ok=False)
    lanes: list[tuple[str, Callable[[], dict[str, Any]]]] = [
        (
            "sse-disconnect",
            lambda: run_disconnect_lane(api, resilience_dir, model),
        ),
        (
            "failed-bash-recovery",
            lambda: run_failed_bash_lane(api, resilience_dir, model),
        ),
        (
            "live-steering",
            lambda: run_live_steering_lane(api, resilience_dir, model),
        ),
        (
            "explicit-cancel",
            lambda: run_cancel_lane(api, resilience_dir, model),
        ),
        (
            "direct-tool-wildcard-rejection",
            lambda: run_direct_tool_policy_lane(
                api, resilience_dir, direct_tool_port
            ),
        ),
    ]

    results: list[dict[str, Any]] = []
    failures: list[tuple[str, BaseException]] = []
    for name, run_lane in lanes:
        print(f"\n=== resilience: {name} ===", flush=True)
        try:
            result = run_lane()
            print(f"PASS resilience={name}", flush=True)
        except Exception as error:
            failures.append((name, error))
            result_path = resilience_dir / name / "acceptance-result.json"
            try:
                result = json.loads(result_path.read_text())
            except (FileNotFoundError, json.JSONDecodeError, OSError):
                result = {
                    "lane": name,
                    "status": "fail",
                    "failure_classification": failure_classification(error).value,
                    "error": str(error),
                }
            print(f"FAIL resilience={name}: {error}", file=sys.stderr, flush=True)
        results.append(result)

    summary_path = resilience_dir / "resilience-summary.json"
    write_json(summary_path, results)
    if failures:
        classification = (
            FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL
            if any(
                failure_classification(error)
                is FailureClassification.PRODUCT_OR_CONFIGURATION_TERMINAL
                for _, error in failures
            )
            else FailureClassification.EXTERNAL_PROVIDER_TRANSIENT
        )
        suite_error = AcceptanceFailure(
            "resilience suite failed: "
            + "; ".join(f"{name}: {error}" for name, error in failures),
            classification,
        )
        setattr(suite_error, "resilience_results", results)
        setattr(suite_error, "resilience_summary", str(summary_path))
        raise suite_error
    print(f"resilience summary: {summary_path}", flush=True)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        required=True,
        help="explicit loopback candidate URL; production port 3000 is rejected",
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--model", default="grok-4.5")
    parser.add_argument(
        "--cycles",
        type=int,
        default=3,
        help="required number of consecutive clean real-agent cycles (default: 3)",
    )
    parser.add_argument("--start-port", type=int, default=5210)
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument(
        "--external-retries",
        type=int,
        default=3,
        help=(
            "bounded full-attempt retries for narrowly classified external provider "
            "capacity/upstream failures (default: 3)"
        ),
    )
    parser.add_argument(
        "--external-backoff",
        type=float,
        default=5.0,
        help="initial external provider retry delay in seconds (default: 5)",
    )
    parser.add_argument(
        "--skip-resilience",
        action="store_true",
        help="run only the normal coding cycles, without resilience lanes",
    )
    parser.add_argument(
        "--retain-final-process",
        action="store_true",
        help="opt in to retaining the final preview process instead of verifying cleanup",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.base_url = validate_candidate_base_url(args.base_url)
    require(args.cycles > 0, "cycles must be positive")
    require(args.external_retries >= 0, "external-retries must be non-negative")
    require(args.external_backoff >= 0, "external-backoff must be non-negative")
    # Each external failure can invalidate at most cycles - 1 prior clean
    # passes, so this is a strict upper bound on ports consumed by all attempts.
    max_cycle_attempts = args.cycles * (args.external_retries + 1)
    direct_tool_port = args.start_port + max_cycle_attempts + 10
    require(
        1024 <= args.start_port and direct_tool_port <= 65535,
        "invalid port range",
    )
    args.root.mkdir(parents=True, exist_ok=True)
    api = MitsuroApi(args.base_url, args.timeout)
    exact_model: dict[str, Any] | None = None
    invocation_id = str(uuid.uuid4())
    attempt_log_path = args.root / "acceptance-attempts.jsonl"
    summary_path = args.root / "acceptance-summary.json"
    final_summary_path = args.root / "acceptance-final-summary.json"
    clean_results: list[dict[str, Any]] = []
    attempt_records: list[dict[str, Any]] = []
    clean_streak = 0
    external_failures = 0
    final_status = "running"
    final_error: str | None = None
    final_classification: str | None = None
    resilience_results: list[dict[str, Any]] | None = None
    failure_cleanup_results: list[dict[str, Any]] = []

    def persist_summaries() -> None:
        write_json(summary_path, clean_results)
        write_json(
            final_summary_path,
            {
                "invocation_id": invocation_id,
                "status": final_status,
                "failure_classification": final_classification,
                "error": final_error,
                "model": args.model,
                "model_key": exact_model.get("key") if exact_model else None,
                "model_catalog_revision": (
                    exact_model.get("catalog_revision") if exact_model else None
                ),
                "requested_consecutive_clean_cycles": args.cycles,
                "achieved_consecutive_clean_cycles": clean_streak,
                "attempt_count": len(attempt_records),
                "external_provider_transient_attempts": external_failures,
                "external_retry_limit": args.external_retries,
                "attempt_log": str(attempt_log_path),
                "attempts": attempt_records,
                "clean_cycle_results": clean_results,
                "resilience_skipped": args.skip_resilience,
                "resilience_results": resilience_results,
                "failure_cleanup_results": failure_cleanup_results,
            },
        )

    def record_attempt(record: dict[str, Any]) -> None:
        record["elapsed_seconds"] = round(
            time.monotonic() - float(record.pop("_started_monotonic")), 3
        )
        attempt_records.append(record)
        append_json_record(attempt_log_path, record)
        persist_summaries()

    try:
        health = api.json_request("GET", "/health")
        require(health.get("status") == "ok", f"server health failed: {health}")
        exact_model = select_stable_exact_model(api, args.model)
        print(
            f"Mitsuro harness E2E: model={args.model} "
            f"revision={exact_model.get('catalog_revision')} "
            f"clean_cycles={args.cycles} root={args.root}",
            flush=True,
        )

        cycle_attempt = 0
        while clean_streak < args.cycles:
            cycle_attempt += 1
            streak_position = clean_streak + 1
            port = args.start_port + cycle_attempt - 1
            print(
                f"\n=== cycle attempt {cycle_attempt}; "
                f"clean streak {streak_position}/{args.cycles} ===",
                flush=True,
            )
            record: dict[str, Any] = {
                "invocation_id": invocation_id,
                "attempt": len(attempt_records) + 1,
                "kind": "real_agent_cycle",
                "cycle_attempt": cycle_attempt,
                "clean_streak_before": clean_streak,
                "target_streak_position": streak_position,
                "port": port,
                "model": args.model,
                "model_key": exact_model["key"],
                "model_catalog_revision": exact_model.get("catalog_revision"),
                "started_at_epoch_seconds": time.time(),
                "_started_monotonic": time.monotonic(),
            }
            cycle_error: BaseException | None = None
            try:
                result = run_cycle(
                    api,
                    args.root,
                    exact_model,
                    port,
                    cycle_attempt,
                    keep_process=(
                        args.retain_final_process and streak_position == args.cycles
                    ),
                )
                clean_streak += 1
                clean_results.append(result)
                record.update(
                    {
                        "status": "pass",
                        "counts_toward_clean_streak": True,
                        "clean_streak_after": clean_streak,
                        "session_id": result["session_id"],
                        "run_dir": result["run_dir"],
                        "process_id": result["process_id"],
                    }
                )
                print(
                    f"PASS attempt={cycle_attempt} streak={clean_streak}/{args.cycles} "
                    f"session={result['session_id']} process={result['process_id']}",
                    flush=True,
                )
            except Exception as error:
                cycle_error = error
                classification = failure_classification(error)
                if classification is FailureClassification.EXTERNAL_PROVIDER_TRANSIENT:
                    external_failures += 1
                    clean_streak = 0
                    clean_results.clear()
                    record_status = classification.value
                else:
                    record_status = "fail"
                record.update(
                    {
                        "status": record_status,
                        "failure_classification": classification.value,
                        "counts_toward_clean_streak": False,
                        "clean_streak_after": clean_streak,
                        "error": str(error),
                    }
                )
                failed_result = getattr(error, "acceptance_result", None)
                if isinstance(failed_result, dict):
                    failed_run_dir = failed_result.get("run_dir")
                    record.update(
                        {
                            "session_id": failed_result.get("session_id"),
                            "run_dir": failed_run_dir,
                            "failed_phase": failed_result.get("phase"),
                            "cleanup_status": (
                                failed_result.get("cleanup", {}).get("status")
                                if isinstance(failed_result.get("cleanup"), dict)
                                else None
                            ),
                            "acceptance_result": (
                                str(Path(str(failed_run_dir)) / "acceptance-result.json")
                                if failed_run_dir
                                else None
                            ),
                        }
                    )
            finally:
                record_attempt(record)

            if cycle_error is None:
                continue
            classification = failure_classification(cycle_error)
            if classification is not FailureClassification.EXTERNAL_PROVIDER_TRANSIENT:
                raise cycle_error
            if external_failures > args.external_retries:
                raise AcceptanceFailure(
                    f"external provider retry budget exhausted after "
                    f"{external_failures} transient attempts: {cycle_error}",
                    FailureClassification.EXTERNAL_PROVIDER_TRANSIENT,
                )
            delay = external_retry_delay(args.external_backoff, external_failures)
            print(
                f"EXTERNAL provider transient; clean streak reset; retrying in "
                f"{delay:.1f}s ({external_failures}/{args.external_retries})",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)

        if not args.skip_resilience:
            resilience_attempt = 0
            base_resilience_dir = args.root / "resilience"
            if base_resilience_dir.exists():
                base_resilience_dir = args.root / f"resilience-{invocation_id[:8]}"
            while True:
                resilience_attempt += 1
                resilience_dir = (
                    base_resilience_dir
                    if resilience_attempt == 1
                    else args.root
                    / f"{base_resilience_dir.name}-retry-{resilience_attempt:02d}"
                )
                record = {
                    "invocation_id": invocation_id,
                    "attempt": len(attempt_records) + 1,
                    "kind": "resilience_suite",
                    "resilience_attempt": resilience_attempt,
                    "run_dir": str(resilience_dir),
                    "model": args.model,
                    "model_key": exact_model["key"],
                    "model_catalog_revision": exact_model.get("catalog_revision"),
                    "started_at_epoch_seconds": time.time(),
                    "_started_monotonic": time.monotonic(),
                }
                resilience_error: BaseException | None = None
                try:
                    resilience_results = run_resilience_suite(
                        api,
                        resilience_dir,
                        exact_model,
                        direct_tool_port,
                    )
                    record.update(
                        {
                            "status": "pass",
                            "counts_toward_clean_streak": False,
                            "lane_count": len(resilience_results),
                        }
                    )
                except Exception as error:
                    resilience_error = error
                    partial_results = getattr(error, "resilience_results", None)
                    if isinstance(partial_results, list):
                        resilience_results = partial_results
                    classification = failure_classification(error)
                    if classification is FailureClassification.EXTERNAL_PROVIDER_TRANSIENT:
                        external_failures += 1
                        record_status = classification.value
                    else:
                        record_status = "fail"
                    record.update(
                        {
                            "status": record_status,
                            "failure_classification": classification.value,
                            "counts_toward_clean_streak": False,
                            "lane_count": (
                                len(partial_results)
                                if isinstance(partial_results, list)
                                else None
                            ),
                            "resilience_summary": getattr(
                                error, "resilience_summary", None
                            ),
                            "error": str(error),
                        }
                    )
                finally:
                    record_attempt(record)

                if resilience_error is None:
                    break
                classification = failure_classification(resilience_error)
                if classification is not FailureClassification.EXTERNAL_PROVIDER_TRANSIENT:
                    raise resilience_error
                if external_failures > args.external_retries:
                    raise AcceptanceFailure(
                        "external provider retry budget exhausted during resilience "
                        f"after {external_failures} transient attempts: {resilience_error}",
                        FailureClassification.EXTERNAL_PROVIDER_TRANSIENT,
                    )
                delay = external_retry_delay(args.external_backoff, external_failures)
                print(
                    f"EXTERNAL provider transient in resilience suite; retrying in "
                    f"{delay:.1f}s ({external_failures}/{args.external_retries})",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(delay)

        final_status = "pass"
        persist_summaries()
        print(
            f"\nPASS {len(clean_results)} consecutive real-agent cycles; "
            f"final process {clean_results[-1]['process_id']} "
            f"{final_process_disposition(clean_results[-1])}"
            + ("; resilience lanes passed" if not args.skip_resilience else ""),
            flush=True,
        )
        print(f"summary: {summary_path}", flush=True)
        print(f"final summary: {final_summary_path}", flush=True)
        return 0
    except Exception as error:
        for cycle_result in clean_results:
            if cycle_result.get("process_retained") is not True:
                continue
            cleanup = cleanup_cycle_processes(
                api,
                Path(str(cycle_result["run_dir"])),
                port=int(cycle_result["port"]),
                known_process_ids=[str(cycle_result["process_id"])],
            )
            failure_cleanup_results.append(
                {
                    "run_dir": cycle_result["run_dir"],
                    "process_id": cycle_result["process_id"],
                    "port": cycle_result["port"],
                    "cleanup": cleanup,
                }
            )
            cycle_result["process_retained_on_cycle_success"] = True
            cycle_result["process_retained"] = cleanup.get("status") != "pass"
            cycle_result["failure_cleanup"] = cleanup

        cleanup_failures = [
            result
            for result in failure_cleanup_results
            if result.get("cleanup", {}).get("status") != "pass"
        ]
        if cleanup_failures:
            error = AcceptanceFailure(
                f"{error}; retained-process cleanup was incomplete: {cleanup_failures}"
            )
        classification = failure_classification(error)
        final_status = (
            "external_provider_exhausted"
            if classification is FailureClassification.EXTERNAL_PROVIDER_TRANSIENT
            else "fail"
        )
        final_classification = classification.value
        final_error = str(error)
        persist_summaries()
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AcceptanceFailure, subprocess.TimeoutExpired) as error:
        print(f"FAIL: {error}", file=sys.stderr, flush=True)
        raise SystemExit(1)
