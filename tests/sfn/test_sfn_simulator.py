"""Unit tests for Step Functions ASL simulator semantics."""

import threading

import pytest

import creatify_as.sfn.fake_client
import creatify_as.sfn.simulator


@pytest.fixture
def recorded_sleep_delays() -> list[float]:
  return []


@pytest.fixture
def simulator(
  recorded_sleep_delays: list[float],
) -> creatify_as.sfn.simulator.StateMachineSimulator:
  def record_sleep(delay_seconds: float) -> None:
    recorded_sleep_delays.append(delay_seconds)

  return creatify_as.sfn.simulator.StateMachineSimulator(
    config=creatify_as.sfn.simulator.StateMachineSimulatorConfig(step_delay_seconds=0.0),
    sleep_fn=record_sleep,
    time_fn=lambda: 0.0,
  )


def test_task_step_delay_runs_before_handler(
  recorded_sleep_delays: list[float],
) -> None:
  def setup_handler(payload: dict) -> dict:
    return {"execution_document": {"ok": True}}

  simulator = creatify_as.sfn.simulator.StateMachineSimulator(
    config=creatify_as.sfn.simulator.StateMachineSimulatorConfig(step_delay_seconds=5.0),
    sleep_fn=recorded_sleep_delays.append,
    time_fn=lambda: 0.0,
    task_handler_by_function_key={"step_delay__setup": setup_handler},
  )

  definition = {
    "StartAt": "Setup",
    "States": {
      "Setup": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_delay__setup",
          "Payload": {"execution_document.$": "$"},
        },
        "ResultSelector": {"execution_document.$": "$.Payload.execution_document"},
        "OutputPath": "$.execution_document",
        "End": True,
      },
    },
  }

  state_machine_arn = simulator.register_state_machine("step_delay_demo", definition)

  start_response = simulator.start_execution(
    state_machine_arn_or_name=state_machine_arn,
    execution_name="test-step-delay",
    execution_input={},
  )

  simulator.wait_for_execution(start_response["executionArn"], timeout_seconds=5.0)

  assert 5.0 in recorded_sleep_delays


def test_linear_task_chain_passes_execution_document(
  simulator: creatify_as.sfn.simulator.StateMachineSimulator,
) -> None:
  invocation_sequence: list[str] = []

  def setup_handler(payload: dict) -> dict:
    invocation_sequence.append("setup")

    return {
      "execution_document": {
        "step_count": 1,
      },
    }

  def complete_handler(payload: dict) -> dict:
    invocation_sequence.append("complete")

    document = payload["execution_document"]

    return {
      "execution_document": {
        "step_count": document["step_count"] + 1,
        "finished": True,
      },
    }

  definition = {
    "StartAt": "Setup",
    "States": {
      "Setup": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_linear__1_setup",
          "Payload": {
            "execution_document.$": "$",
          },
        },
        "ResultSelector": {
          "execution_document.$": "$.Payload.execution_document",
        },
        "OutputPath": "$.execution_document",
        "Next": "Complete",
      },
      "Complete": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_linear__2_complete",
          "Payload": {
            "execution_document.$": "$",
          },
        },
        "ResultSelector": {
          "execution_document.$": "$.Payload.execution_document",
        },
        "OutputPath": "$.execution_document",
        "End": True,
      },
    },
  }

  state_machine_arn = simulator.register_state_machine("linear_demo", definition)

  start_response = simulator.start_execution(
    state_machine_arn_or_name=state_machine_arn,
    execution_name="test-linear",
    execution_input={},
    task_handler_by_function_key={
      "step_linear__1_setup": setup_handler,
      "step_linear__2_complete": complete_handler,
    },
  )

  status_name = simulator.wait_for_execution(start_response["executionArn"], timeout_seconds=5.0)
  describe_payload = simulator.describe_execution(start_response["executionArn"])

  assert status_name == "SUCCEEDED"
  assert invocation_sequence == ["setup", "complete"]
  assert describe_payload["output"] == '{"step_count": 2, "finished": true}'


def test_choice_branch_boolean_equals(
  simulator: creatify_as.sfn.simulator.StateMachineSimulator,
) -> None:
  def path_a_handler(payload: dict) -> dict:
    return {"execution_document": {"branch": "a"}}

  def path_b_handler(payload: dict) -> dict:
    return {"execution_document": {"branch": "b"}}

  definition = {
    "StartAt": "BranchChoice",
    "States": {
      "BranchChoice": {
        "Type": "Choice",
        "Choices": [
          {
            "Variable": "$.launch_complete_early",
            "BooleanEquals": True,
            "Next": "PathA",
          },
        ],
        "Default": "PathB",
      },
      "PathA": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_choice__path_a",
          "Payload": {"execution_document.$": "$"},
        },
        "ResultSelector": {"execution_document.$": "$.Payload.execution_document"},
        "OutputPath": "$.execution_document",
        "End": True,
      },
      "PathB": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_choice__path_b",
          "Payload": {"execution_document.$": "$"},
        },
        "ResultSelector": {"execution_document.$": "$.Payload.execution_document"},
        "OutputPath": "$.execution_document",
        "End": True,
      },
    },
  }

  state_machine_arn = simulator.register_state_machine("choice_demo", definition)

  start_response = simulator.start_execution(
    state_machine_arn_or_name=state_machine_arn,
    execution_name="test-choice",
    execution_input={"launch_complete_early": True},
    task_handler_by_function_key={
      "step_choice__path_a": path_a_handler,
      "step_choice__path_b": path_b_handler,
    },
  )

  simulator.wait_for_execution(start_response["executionArn"], timeout_seconds=5.0)
  describe_payload = simulator.describe_execution(start_response["executionArn"])

  assert '"branch": "a"' in describe_payload["output"]


def test_task_catch_routes_to_error_handler(
  simulator: creatify_as.sfn.simulator.StateMachineSimulator,
) -> None:
  def failing_handler(payload: dict) -> dict:
    raise RuntimeError("boom")

  def record_error_handler(payload: dict) -> dict:
    return {
      "execution_document": {
        "recovered": True,
        "error_present": "error" in payload,
      },
    }

  definition = {
    "StartAt": "Setup",
    "States": {
      "Setup": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_catch__setup",
          "Payload": {"execution_document.$": "$"},
        },
        "ResultSelector": {"execution_document.$": "$.Payload.execution_document"},
        "OutputPath": "$.execution_document",
        "Catch": [
          {
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error",
            "Next": "RecordError",
          },
        ],
        "Next": "ShouldNotRun",
      },
      "RecordError": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_catch__record_error",
          "Payload.$": "$",
        },
        "ResultSelector": {"execution_document.$": "$.Payload.execution_document"},
        "OutputPath": "$.execution_document",
        "End": True,
      },
      "ShouldNotRun": {
        "Type": "Succeed",
      },
    },
  }

  state_machine_arn = simulator.register_state_machine("catch_demo", definition)

  start_response = simulator.start_execution(
    state_machine_arn_or_name=state_machine_arn,
    execution_name="test-catch",
    execution_input={},
    task_handler_by_function_key={
      "step_catch__setup": failing_handler,
      "step_catch__record_error": record_error_handler,
    },
  )

  status_name = simulator.wait_for_execution(start_response["executionArn"], timeout_seconds=5.0)
  describe_payload = simulator.describe_execution(start_response["executionArn"])

  assert status_name == "SUCCEEDED"
  assert '"recovered": true' in describe_payload["output"]


def test_map_max_concurrency_runs_items_in_parallel(
  simulator: creatify_as.sfn.simulator.StateMachineSimulator,
  recorded_sleep_delays: list[float],
) -> None:
  concurrent_count_lock = threading.Lock()
  concurrent_count = 0
  peak_concurrent_count = 0
  concurrency_barrier = threading.Barrier(2)

  def slow_item_handler(payload: dict) -> dict:
    nonlocal concurrent_count
    nonlocal peak_concurrent_count

    with concurrent_count_lock:
      concurrent_count = concurrent_count + 1
      peak_concurrent_count = max(peak_concurrent_count, concurrent_count)

    concurrency_barrier.wait(timeout=2.0)

    with concurrent_count_lock:
      concurrent_count = concurrent_count - 1

    return {
      "execution_document": {
        "item_id": payload["map_item_input"]["item_id"],
      },
    }

  definition = {
    "StartAt": "MapItems",
    "States": {
      "MapItems": {
        "Type": "Map",
        "ItemsPath": "$.item_ids",
        "MaxConcurrency": 2,
        "ItemSelector": {
          "map_item_input": {
            "item_id.$": "$$.Map.Item.Value",
          },
        },
        "ItemProcessor": {
          "ProcessorConfig": {"Mode": "INLINE"},
          "StartAt": "ProcessItem",
          "States": {
            "ProcessItem": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Parameters": {
                "FunctionName": "step_map__process_item",
                "Payload.$": "$",
              },
              "ResultSelector": {"execution_document.$": "$.Payload.execution_document"},
              "OutputPath": "$.execution_document",
              "End": True,
            },
          },
        },
        "ResultPath": "$.map_results",
        "End": True,
      },
    },
  }

  state_machine_arn = simulator.register_state_machine("map_demo", definition)

  start_response = simulator.start_execution(
    state_machine_arn_or_name=state_machine_arn,
    execution_name="test-map",
    execution_input={"item_ids": ["a", "b", "c", "d"]},
    task_handler_by_function_key={
      "step_map__process_item": slow_item_handler,
    },
  )

  status_name = simulator.wait_for_execution(start_response["executionArn"], timeout_seconds=10.0)

  assert status_name == "SUCCEEDED"
  assert peak_concurrent_count >= 2


def test_wait_and_task_retry_use_sleep_fn(
  simulator: creatify_as.sfn.simulator.StateMachineSimulator,
  recorded_sleep_delays: list[float],
) -> None:
  attempt_count = 0

  def flaky_handler(payload: dict) -> dict:
    nonlocal attempt_count

    attempt_count = attempt_count + 1

    if attempt_count < 2:
      raise RuntimeError("not yet")

    return {"execution_document": {"ready": True}}

  definition = {
    "StartAt": "WaitStep",
    "States": {
      "WaitStep": {
        "Type": "Wait",
        "Seconds": 3,
        "Next": "FlakyTask",
      },
      "FlakyTask": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_retry__flaky",
          "Payload": {"execution_document.$": "$"},
        },
        "ResultSelector": {"execution_document.$": "$.Payload.execution_document"},
        "OutputPath": "$.execution_document",
        "Retry": [
          {
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 2,
            "MaxAttempts": 2,
            "BackoffRate": 1.0,
          },
        ],
        "End": True,
      },
    },
  }

  state_machine_arn = simulator.register_state_machine("wait_retry_demo", definition)

  start_response = simulator.start_execution(
    state_machine_arn_or_name=state_machine_arn,
    execution_name="test-wait-retry",
    execution_input={},
    task_handler_by_function_key={"step_retry__flaky": flaky_handler},
  )

  status_name = simulator.wait_for_execution(start_response["executionArn"], timeout_seconds=5.0)

  assert status_name == "SUCCEEDED"
  assert 3.0 in recorded_sleep_delays
  assert 2.0 in recorded_sleep_delays


def test_missing_task_handler_fails_execution(
  simulator: creatify_as.sfn.simulator.StateMachineSimulator,
) -> None:
  definition = {
    "StartAt": "Setup",
    "States": {
      "Setup": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_missing__setup",
          "Payload": {"execution_document.$": "$"},
        },
        "End": True,
      },
    },
  }

  state_machine_arn = simulator.register_state_machine("missing_handler_demo", definition)

  start_response = simulator.start_execution(
    state_machine_arn_or_name=state_machine_arn,
    execution_name="test-missing",
    execution_input={},
    task_handler_by_function_key={},
  )

  status_name = simulator.wait_for_execution(start_response["executionArn"], timeout_seconds=5.0)
  describe_payload = simulator.describe_execution(start_response["executionArn"])

  assert status_name == "FAILED"
  assert describe_payload["error"] == "States.Runtime"


def test_fake_client_start_and_describe() -> None:
  def setup_handler(payload: dict) -> dict:
    return {"execution_document": {"via": "fake_client"}}

  definition = {
    "StartAt": "Setup",
    "States": {
      "Setup": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "step_fake__setup",
          "Payload": {"execution_document.$": "$"},
        },
        "ResultSelector": {"execution_document.$": "$.Payload.execution_document"},
        "OutputPath": "$.execution_document",
        "End": True,
      },
    },
  }

  client = creatify_as.sfn.fake_client.FakeSfnClient(
    creatify_as.sfn.simulator.StateMachineSimulator(
      config=creatify_as.sfn.simulator.StateMachineSimulatorConfig(step_delay_seconds=0.0),
      sleep_fn=lambda delay_seconds: None,
      time_fn=lambda: 0.0,
      task_handler_by_function_key={"step_fake__setup": setup_handler},
    ),
  )

  fake_client_state_machine_arn = client._simulator.register_state_machine(
    "fake_client_demo",
    definition,
  )

  start_response = client.start_execution(
    stateMachineArn=fake_client_state_machine_arn,
    name="exec-fake",
    input="{}",
  )

  client._simulator.wait_for_execution(start_response["executionArn"], timeout_seconds=5.0)
  describe_payload = client.describe_execution(executionArn=start_response["executionArn"])

  assert describe_payload["status"] == "SUCCEEDED"
  assert '"via": "fake_client"' in describe_payload["output"]
