"""In-memory Step Functions ASL execution for tests without live AWS calls."""

import dataclasses
import datetime
import json
import secrets
import threading
import time
import typing

import creatify_as.sfn.asl_subgraph_engine


@dataclasses.dataclass
class StateMachineSimulatorConfig:
  """Identity defaults for one Step Functions simulator instance."""

  region_name: str = "us-east-1"
  account_id: str = "123456789012"
  step_delay_seconds: float = 5.0


@dataclasses.dataclass
class _StateMachineRecord:
  """One registered state machine definition."""

  state_machine_name: str
  state_machine_arn: str
  definition: dict[str, typing.Any]


@dataclasses.dataclass
class _ExecutionRecord:
  """Mutable execution row updated by the manager thread."""

  execution_arn: str
  execution_name: str
  state_machine_arn: str
  status_name: str
  input_document: typing.Any
  output_document: typing.Any | None
  error_name: str | None
  cause_plaintext: str | None
  start_timestamp_seconds: float
  stop_timestamp_seconds: float | None
  lock: threading.Lock
  done_event: threading.Event
  task_handler_by_function_key: dict[str, typing.Callable[..., dict[str, typing.Any]]]


class StateMachineSimulator:
  """Simulate Standard workflows from ASL with a manager thread and task handler map."""

  def __init__(
    self,
    *,
    config: StateMachineSimulatorConfig | None = None,
    sleep_fn: typing.Callable[[float], None] | None = None,
    time_fn: typing.Callable[[], float] | None = None,
    task_handler_by_function_key: dict[str, typing.Callable[..., dict[str, typing.Any]]] | None = None,
  ) -> None:
    self._config = config or StateMachineSimulatorConfig()

    if sleep_fn is None:
      sleep_fn = time.sleep

    if time_fn is None:
      time_fn = time.time

    self._sleep_fn = sleep_fn
    self._time_fn = time_fn
    self._default_task_handler_by_function_key = (
      task_handler_by_function_key or {}
    )
    self._state_machine_by_arn: dict[str, _StateMachineRecord] = {}
    self._state_machine_by_name: dict[str, _StateMachineRecord] = {}
    self._execution_by_arn: dict[str, _ExecutionRecord] = {}
    self._simulator_lock = threading.Lock()

  def register_state_machine(
    self,
    state_machine_name: str,
    definition: dict[str, typing.Any],
  ) -> str:
    """Register one ASL definition and return its synthetic ARN."""

    state_machine_arn = (
      "arn:aws:states:{region}:{account_id}:stateMachine:{name}".format(
        region=self._config.region_name,
        account_id=self._config.account_id,
        name=state_machine_name,
      )
    )

    state_machine_record = _StateMachineRecord(
      state_machine_name=state_machine_name,
      state_machine_arn=state_machine_arn,
      definition=definition,
    )

    with self._simulator_lock:
      self._state_machine_by_arn[state_machine_arn] = state_machine_record
      self._state_machine_by_name[state_machine_name] = state_machine_record

    return state_machine_arn

  def start_execution(
    self,
    *,
    state_machine_arn_or_name: str,
    execution_name: str,
    execution_input: dict[str, typing.Any],
    task_handler_by_function_key: dict[str, typing.Callable[..., dict[str, typing.Any]]] | None = None,
  ) -> dict[str, typing.Any]:
    """Start one execution asynchronously and return a boto-shaped response."""

    state_machine_record = self._resolve_state_machine_record(state_machine_arn_or_name)

    execution_suffix = secrets.token_hex(4)
    execution_arn = (
      "{state_machine_arn}:{execution_name}-{suffix}".format(
        state_machine_arn=state_machine_record.state_machine_arn,
        execution_name=execution_name,
        suffix=execution_suffix,
      )
    )

    if task_handler_by_function_key is None:
      merged_handlers = dict(self._default_task_handler_by_function_key)
    else:
      merged_handlers = dict(task_handler_by_function_key)

    start_timestamp_seconds = self._time_fn()

    execution_record = _ExecutionRecord(
      execution_arn=execution_arn,
      execution_name=execution_name,
      state_machine_arn=state_machine_record.state_machine_arn,
      status_name="RUNNING",
      input_document=execution_input,
      output_document=None,
      error_name=None,
      cause_plaintext=None,
      start_timestamp_seconds=start_timestamp_seconds,
      stop_timestamp_seconds=None,
      lock=threading.Lock(),
      done_event=threading.Event(),
      task_handler_by_function_key=merged_handlers,
    )

    with self._simulator_lock:
      self._execution_by_arn[execution_arn] = execution_record

    manager_thread = threading.Thread(
      target=self._run_execution_manager_thread,
      args=(execution_arn, state_machine_record),
      daemon=True,
      name="creatify-as-sfn-{execution_name}".format(execution_name=execution_name),
    )
    manager_thread.start()

    start_date = datetime.datetime.fromtimestamp(
      start_timestamp_seconds,
      tz=datetime.timezone.utc,
    )

    return {
      "executionArn": execution_arn,
      "startDate": start_date,
    }

  def wait_for_execution(
    self,
    execution_arn: str,
    *,
    timeout_seconds: float = 30.0,
  ) -> str:
    """Block until the execution reaches a terminal status and return status name."""

    execution_record = self._resolve_execution_record(execution_arn)

    if not execution_record.done_event.wait(timeout=timeout_seconds):
      raise TimeoutError(
        "execution {execution_arn} did not finish within {timeout_seconds}s".format(
          execution_arn=execution_arn,
          timeout_seconds=timeout_seconds,
        ),
      )

    with execution_record.lock:
      return execution_record.status_name

  def describe_execution(self, execution_arn: str) -> dict[str, typing.Any]:
    """Return one boto-shaped DescribeExecution response."""

    execution_record = self._resolve_execution_record(execution_arn)

    with execution_record.lock:
      start_date = datetime.datetime.fromtimestamp(
        execution_record.start_timestamp_seconds,
        tz=datetime.timezone.utc,
      )

      describe_payload: dict[str, typing.Any] = {
        "executionArn": execution_record.execution_arn,
        "stateMachineArn": execution_record.state_machine_arn,
        "name": execution_record.execution_name,
        "status": execution_record.status_name,
        "startDate": start_date,
        "input": json.dumps(execution_record.input_document),
      }

      if execution_record.output_document is not None:
        describe_payload["output"] = json.dumps(execution_record.output_document)

      if execution_record.stop_timestamp_seconds is not None:
        describe_payload["stopDate"] = datetime.datetime.fromtimestamp(
          execution_record.stop_timestamp_seconds,
          tz=datetime.timezone.utc,
        )

      if execution_record.error_name is not None:
        describe_payload["error"] = execution_record.error_name

      if execution_record.cause_plaintext is not None:
        describe_payload["cause"] = execution_record.cause_plaintext

      return describe_payload

  def _resolve_state_machine_record(
    self,
    state_machine_arn_or_name: str,
  ) -> _StateMachineRecord:
    """Return one registered state machine or raise ValueError."""

    with self._simulator_lock:
      if state_machine_arn_or_name in self._state_machine_by_arn:
        return self._state_machine_by_arn[state_machine_arn_or_name]

      if state_machine_arn_or_name in self._state_machine_by_name:
        return self._state_machine_by_name[state_machine_arn_or_name]

    raise ValueError(
      "unknown state machine {state_machine_arn_or_name}".format(
        state_machine_arn_or_name=state_machine_arn_or_name,
      ),
    )

  def _resolve_execution_record(self, execution_arn: str) -> _ExecutionRecord:
    """Return one execution record or raise ValueError."""

    with self._simulator_lock:
      if execution_arn in self._execution_by_arn:
        return self._execution_by_arn[execution_arn]

    raise ValueError(
      "unknown execution_arn={execution_arn}".format(
        execution_arn=execution_arn,
      ),
    )

  def _run_execution_manager_thread(
    self,
    execution_arn: str,
    state_machine_record: _StateMachineRecord,
  ) -> None:
    """Walk top-level states on a manager thread with one worker thread per state."""

    execution_record = self._resolve_execution_record(execution_arn)

    subgraph_engine = creatify_as.sfn.asl_subgraph_engine.AslSubgraphEngine(
      task_handler_by_function_key=execution_record.task_handler_by_function_key,
      sleep_fn=self._sleep_fn,
      step_delay_seconds=self._config.step_delay_seconds,
      execution_id=execution_arn,
      execution_name=execution_record.execution_name,
      state_machine_id=state_machine_record.state_machine_arn,
    )

    current_state_name: str | None = str(state_machine_record.definition["StartAt"])
    current_document = execution_record.input_document

    try:
      while current_state_name is not None:
        state_outcome = self._run_one_state_on_worker_thread(
          subgraph_engine,
          state_machine_record.definition,
          current_state_name,
          current_document,
        )

        if state_outcome.terminal_outcome is not None:
          terminal_outcome = state_outcome.terminal_outcome

          with execution_record.lock:
            execution_record.status_name = terminal_outcome.status_name
            execution_record.output_document = terminal_outcome.output_document
            execution_record.error_name = terminal_outcome.error_name
            execution_record.cause_plaintext = terminal_outcome.cause_plaintext
            execution_record.stop_timestamp_seconds = self._time_fn()

          break

        current_document = state_outcome.output_document
        current_state_name = state_outcome.next_state_name
      else:
        with execution_record.lock:
          execution_record.status_name = "SUCCEEDED"
          execution_record.output_document = current_document
          execution_record.stop_timestamp_seconds = self._time_fn()
    except Exception as uncaught_exception:
      with execution_record.lock:
        execution_record.status_name = "FAILED"
        execution_record.output_document = current_document
        execution_record.error_name = "States.Runtime"
        execution_record.cause_plaintext = str(uncaught_exception)
        execution_record.stop_timestamp_seconds = self._time_fn()
    finally:
      execution_record.done_event.set()

  def _run_one_state_on_worker_thread(
    self,
    subgraph_engine: creatify_as.sfn.asl_subgraph_engine.AslSubgraphEngine,
    definition: dict[str, typing.Any],
    state_name: str,
    input_document: typing.Any,
  ) -> creatify_as.sfn.asl_subgraph_engine.AslSingleStateOutcome:
    """Execute one top-level state on a short-lived worker thread."""

    outcome_holder: dict[str, creatify_as.sfn.asl_subgraph_engine.AslSingleStateOutcome] = {}
    worker_done_event = threading.Event()
    worker_error_holder: dict[str, BaseException] = {}

    def run_state_worker() -> None:
      try:
        outcome_holder["outcome"] = subgraph_engine.execute_one_state_for_manager(
          definition,
          state_name,
          input_document,
        )
      except BaseException as worker_exception:
        worker_error_holder["error"] = worker_exception
      finally:
        worker_done_event.set()

    worker_thread = threading.Thread(
      target=run_state_worker,
      daemon=True,
      name="creatify-as-sfn-step-{state_name}".format(state_name=state_name),
    )
    worker_thread.start()
    worker_done_event.wait()

    if "error" in worker_error_holder:
      raise worker_error_holder["error"]

    return outcome_holder["outcome"]
