"""Test unitari per i modelli di dati."""

import json
from orchestratore_llm.models import (
    BatchMetrics,
    BatchResult,
    ExecutionStatus,
    LLMRequest,
    LLMResponse,
    LLMTaskType,
    TaskExecutionResult,
    TokenUsage,
)


def test_llm_request_creation_and_serialization():
    req = LLMRequest(
        prompt="Test prompt",
        task_type=LLMTaskType.CLASSIFICATION,
        document_id="DOC-99",
        parameters={"temperature": 0.2},
    )
    d = req.to_dict()
    assert d["prompt"] == "Test prompt"
    assert d["task_type"] == "classification"
    assert d["document_id"] == "DOC-99"
    assert d["parameters"]["temperature"] == 0.2

    reconstructed = LLMRequest.from_dict(d)
    assert reconstructed.request_id == req.request_id
    assert reconstructed.prompt == req.prompt
    assert reconstructed.task_type == LLMTaskType.CLASSIFICATION
    assert reconstructed.document_id == "DOC-99"


def test_token_usage_calculation():
    usage = TokenUsage(prompt_tokens=15, completion_tokens=35)
    assert usage.total_tokens == 50
    d = usage.to_dict()
    assert d["total_tokens"] == 50


def test_batch_result_filtering_and_document_grouping():
    req1 = LLMRequest(prompt="p1", document_id="DOC-1")
    req2 = LLMRequest(prompt="p2", document_id="DOC-1")
    req3 = LLMRequest(prompt="p3", document_id="DOC-2")

    res1 = TaskExecutionResult(
        request=req1,
        status=ExecutionStatus.SUCCESS,
        response=LLMResponse(
            request_id=req1.request_id,
            content="ok1",
            provider="test",
            model="m1",
            usage=TokenUsage(10, 10, 20),
        ),
    )
    res2 = TaskExecutionResult(
        request=req2,
        status=ExecutionStatus.FAILED,
        error="Rate limit exceeded",
        error_type="RateLimitError",
        is_transient=True,
    )
    res3 = TaskExecutionResult(
        request=req3,
        status=ExecutionStatus.SUCCESS,
        response=LLMResponse(
            request_id=req3.request_id,
            content="ok3",
            provider="test",
            model="m1",
            usage=TokenUsage(5, 5, 10),
        ),
    )

    batch = BatchResult(results=[res1, res2, res3], metrics=BatchMetrics())

    assert len(batch.successful()) == 2
    assert len(batch.failed()) == 1
    assert batch.failed()[0].request_id == req2.request_id

    doc1_results = batch.get_by_document("DOC-1")
    assert len(doc1_results) == 2
    assert batch.results_by_id[req1.request_id] == res1
