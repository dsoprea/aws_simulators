"""ASL subgraph execution for the Step Functions simulator."""

import concurrent.futures
import copy
import dataclasses
import typing

import creatify_as.sfn.asl_choice_utility
import creatify_as.sfn.asl_path_utility
import creatify_as.sfn.asl_task_utility


@dataclasses.dataclass
class AslTerminalOutcome:
  """Terminal status for one subgraph run."""

  status_name: str
  output_document: typing.Any
  error_name: str | None = None
  cause_plaintext: str | None = None


@dataclasses.dataclass
class AslSingleStateOutcome:
  """Result of executing one state within a subgraph."""

  next_state_name: str | None
  output_document: typing.Any
  terminal_outcome: AslTerminalOutcome | None = None


class AslSubgraphEngine:
  """Run one ASL States block synchronously (Map item processors and nested graphs)."""

  def __init__(
    self,
    *,
    task_handler_by_function_key: dict[str, typing.Callable[..., dict[str, typing.Any]]],
    sleep_fn: typing.Callable[[float], None],
    step_delay_seconds: float,
    execution_id: str,
    execution_name: str,
    state_machine_id: str,
  ) -> None:
    self._task_handler_by_function_key = task_handler_by_function_key
    self._sleep_fn = sleep_fn
    self._step_delay_seconds = step_delay_seconds
    self._execution_id = execution_id
    self._execution_name = execution_name
    self._state_machine_id = state_machine_id

  def run_subgraph_until_terminal(
    self,
    definition: dict[str, typing.Any],
    start_state_name: str,
    input_document: typing.Any,
    map_item_value: typing.Any | None = None,
    map_item_index: int | None = None,
  ) -> AslTerminalOutcome:
    """Execute states from start until End, Fail, or Succeed."""

    intrinsic_context = creatify_as.sfn.asl_path_utility.build_intrinsic_context(
      execution_id=self._execution_id,
      execution_name=self._execution_name,
      state_machine_id=self._state_machine_id,
      map_item_value=map_item_value,
      map_item_index=map_item_index,
    )

    states_by_name = definition["States"]
    state_name = start_state_name
    current_document = input_document

    while state_name is not None:
      try:
        state_outcome = self._execute_one_state(
          states_by_name[str(state_name)],
          current_document,
          intrinsic_context,
          states_by_name,
        )
      except creatify_as.sfn.asl_path_utility.StatesRuntimeError as runtime_error:
        return AslTerminalOutcome(
          status_name="FAILED",
          output_document=current_document,
          error_name=runtime_error.error_name,
          cause_plaintext=runtime_error.cause_plaintext,
        )

      if state_outcome.terminal_outcome is not None:
        return state_outcome.terminal_outcome

      current_document = state_outcome.output_document
      state_name = state_outcome.next_state_name

    return AslTerminalOutcome(
      status_name="SUCCEEDED",
      output_document=current_document,
    )

  def execute_one_state_for_manager(
    self,
    definition: dict[str, typing.Any],
    state_name: str,
    input_document: typing.Any,
  ) -> AslSingleStateOutcome:
    """Execute a single top-level state (manager schedules one state per call)."""

    intrinsic_context = creatify_as.sfn.asl_path_utility.build_intrinsic_context(
      execution_id=self._execution_id,
      execution_name=self._execution_name,
      state_machine_id=self._state_machine_id,
    )

    states_by_name = definition["States"]
    state_definition = states_by_name[state_name]

    try:
      return self._execute_one_state(
        state_definition,
        input_document,
        intrinsic_context,
        states_by_name,
      )
    except creatify_as.sfn.asl_path_utility.StatesRuntimeError as runtime_error:
      return AslSingleStateOutcome(
        next_state_name=None,
        output_document=input_document,
        terminal_outcome=AslTerminalOutcome(
          status_name="FAILED",
          output_document=input_document,
          error_name=runtime_error.error_name,
          cause_plaintext=runtime_error.cause_plaintext,
        ),
      )

  def _execute_one_state(
    self,
    state_definition: dict[str, typing.Any],
    input_document: typing.Any,
    intrinsic_context: dict[str, typing.Any],
    states_by_name: dict[str, typing.Any],
  ) -> AslSingleStateOutcome:
    """Dispatch one state by Type."""

    state_type = state_definition["Type"]

    if state_type == "Task":
      return self._execute_task_state(
        state_definition,
        input_document,
        intrinsic_context,
      )

    if state_type == "Choice":
      next_state_name = creatify_as.sfn.asl_choice_utility.resolve_choice_next_state(
        state_definition,
        input_document,
        intrinsic_context,
      )

      return AslSingleStateOutcome(
        next_state_name=next_state_name,
        output_document=input_document,
      )

    if state_type == "Wait":
      self._execute_wait_state(state_definition)

      return AslSingleStateOutcome(
        next_state_name=str(state_definition["Next"]),
        output_document=input_document,
      )

    if state_type == "Pass":
      output_document = self._execute_pass_state(
        state_definition,
        input_document,
        intrinsic_context,
      )

      return self._resolve_pass_or_task_success(
        state_definition,
        input_document,
        output_document,
        intrinsic_context,
      )

    if state_type == "Map":
      return self._execute_map_state(
        state_definition,
        input_document,
        intrinsic_context,
        states_by_name,
      )

    if state_type == "Fail":
      return AslSingleStateOutcome(
        next_state_name=None,
        output_document=input_document,
        terminal_outcome=AslTerminalOutcome(
          status_name="FAILED",
          output_document=input_document,
          error_name=str(state_definition.get("Error", "States.Failed")),
          cause_plaintext=str(state_definition.get("Cause", "")),
        ),
      )

    if state_type == "Succeed":
      return AslSingleStateOutcome(
        next_state_name=None,
        output_document=input_document,
        terminal_outcome=AslTerminalOutcome(
          status_name="SUCCEEDED",
          output_document=input_document,
        ),
      )

    raise ValueError(
      "unsupported ASL state Type={state_type}".format(
        state_type=state_type,
      ),
    )

  def _execute_wait_state(self, state_definition: dict[str, typing.Any]) -> None:
    """Sleep for one Wait state's Seconds field."""

    try:
      wait_seconds = float(state_definition["Seconds"])
    except KeyError as key_error:
      raise ValueError("Wait state requires numeric Seconds in v1 simulation") from key_error

    self._sleep_fn(wait_seconds)

  def _execute_pass_state(
    self,
    state_definition: dict[str, typing.Any],
    input_document: typing.Any,
    intrinsic_context: dict[str, typing.Any],
  ) -> typing.Any:
    """Build output for one Pass state."""

    if "Parameters" in state_definition:
      return creatify_as.sfn.asl_path_utility.build_object_from_template(
        state_definition["Parameters"],
        input_document,
        intrinsic_context,
      )

    if "Result" in state_definition:
      return copy.deepcopy(state_definition["Result"])

    return input_document

  def _resolve_pass_or_task_success(
    self,
    state_definition: dict[str, typing.Any],
    parent_document: typing.Any,
    raw_state_output: typing.Any,
    intrinsic_context: dict[str, typing.Any],
  ) -> AslSingleStateOutcome:
    """Apply OutputPath/ResultPath and resolve Next or End for non-Task successes."""

    filtered_output = creatify_as.sfn.asl_path_utility.apply_output_path(
      raw_state_output,
      state_definition.get("OutputPath"),
      raw_state_output,
      intrinsic_context,
    )

    merged_document = creatify_as.sfn.asl_path_utility.apply_result_path(
      parent_document,
      state_definition.get("ResultPath"),
      filtered_output,
    )

    if state_definition.get("End") is True:
      return AslSingleStateOutcome(
        next_state_name=None,
        output_document=merged_document,
        terminal_outcome=AslTerminalOutcome(
          status_name="SUCCEEDED",
          output_document=merged_document,
        ),
      )

    return AslSingleStateOutcome(
      next_state_name=str(state_definition["Next"]),
      output_document=merged_document,
    )

  def _execute_task_state(
    self,
    state_definition: dict[str, typing.Any],
    input_document: typing.Any,
    intrinsic_context: dict[str, typing.Any],
  ) -> AslSingleStateOutcome:
    """Run one Task state including Retry and Catch."""

    retry_rules = state_definition.get("Retry", [])
    catch_rules = state_definition.get("Catch", [])

    attempt_index = 0
    max_attempts = self._resolve_max_attempts(retry_rules)

    while attempt_index < max_attempts:
      attempt_index = attempt_index + 1

      try:
        if self._step_delay_seconds > 0.0:
          self._sleep_fn(self._step_delay_seconds)

        invoke_response = creatify_as.sfn.asl_task_utility.invoke_lambda_task(
          state_definition=state_definition,
          input_document=input_document,
          intrinsic_context=intrinsic_context,
          task_handler_by_function_key=self._task_handler_by_function_key,
        )

        selected_output = creatify_as.sfn.asl_task_utility.apply_result_selector(
          invoke_response,
          state_definition.get("ResultSelector"),
          intrinsic_context,
        )

        return self._resolve_pass_or_task_success(
          state_definition,
          input_document,
          selected_output,
          intrinsic_context,
        )
      except creatify_as.sfn.asl_task_utility.StatesTaskFailedError as task_failed_error:
        if attempt_index < max_attempts:
          retry_interval = self._resolve_retry_interval(retry_rules, attempt_index)

          if retry_interval > 0:
            self._sleep_fn(retry_interval)

          continue

        catch_outcome = self._apply_catch_rules(
          catch_rules,
          task_failed_error,
          input_document,
          intrinsic_context,
        )

        if catch_outcome is not None:
          return catch_outcome

        return AslSingleStateOutcome(
          next_state_name=None,
          output_document=input_document,
          terminal_outcome=AslTerminalOutcome(
            status_name="FAILED",
            output_document=input_document,
            error_name=task_failed_error.error_name,
            cause_plaintext=task_failed_error.cause_plaintext,
          ),
        )

    raise RuntimeError("unreachable task retry loop")

  def _resolve_max_attempts(self, retry_rules: list[dict[str, typing.Any]]) -> int:
    """Return total attempts including the first try."""

    if not retry_rules:
      return 1

    max_attempts_total = 1

    for retry_rule in retry_rules:
      rule_attempts = int(retry_rule.get("MaxAttempts", 1))
      max_attempts_total = max(max_attempts_total, rule_attempts + 1)

    return max_attempts_total

  def _resolve_retry_interval(
    self,
    retry_rules: list[dict[str, typing.Any]],
    attempt_index: int,
  ) -> float:
    """Return sleep seconds before the next retry attempt."""

    if not retry_rules:
      return 0.0

    retry_rule = retry_rules[0]
    base_interval = float(retry_rule.get("IntervalSeconds", 1))
    backoff_rate = float(retry_rule.get("BackoffRate", 1.0))

    if attempt_index <= 1:
      return base_interval

    multiplier = backoff_rate ** (attempt_index - 1)

    return base_interval * multiplier

  def _apply_catch_rules(
    self,
    catch_rules: list[dict[str, typing.Any]],
    task_failed_error: creatify_as.sfn.asl_task_utility.StatesTaskFailedError,
    input_document: typing.Any,
    intrinsic_context: dict[str, typing.Any],
  ) -> AslSingleStateOutcome | None:
    """Apply the first matching Catch rule when present."""

    for catch_rule in catch_rules:
      if not self._catch_rule_matches(catch_rule, task_failed_error.error_name):
        continue

      error_object = {
        "Error": task_failed_error.error_name,
        "Cause": task_failed_error.cause_plaintext,
      }

      merged_document = creatify_as.sfn.asl_path_utility.apply_result_path(
        input_document,
        catch_rule.get("ResultPath"),
        error_object,
      )

      return AslSingleStateOutcome(
        next_state_name=str(catch_rule["Next"]),
        output_document=merged_document,
      )

    return None

  def _catch_rule_matches(
    self,
    catch_rule: dict[str, typing.Any],
    error_name: str,
  ) -> bool:
    """Return whether one Catch rule includes the raised error name."""

    error_equals_list = catch_rule.get("ErrorEquals", [])

    for error_equals_name in error_equals_list:
      if error_equals_name == "States.ALL":
        return True

      if error_equals_name == error_name:
        return True

    return False

  def _execute_map_state(
    self,
    state_definition: dict[str, typing.Any],
    input_document: typing.Any,
    intrinsic_context: dict[str, typing.Any],
    states_by_name: dict[str, typing.Any],
  ) -> AslSingleStateOutcome:
    """Run one Map state with a bounded thread pool."""

    items_path = state_definition.get("ItemsPath", "$")

    items_list = creatify_as.sfn.asl_path_utility.resolve_json_path(
      items_path,
      input_document,
      intrinsic_context,
    )

    if not isinstance(items_list, list):
      raise creatify_as.sfn.asl_path_utility.StatesRuntimeError("JSONPath not found")

    item_processor = state_definition.get("ItemProcessor", {})

    processor_config = item_processor.get("ProcessorConfig", {})

    if processor_config.get("Mode") != "INLINE":
      raise ValueError(
        "only INLINE ItemProcessor Mode is supported (got {mode})".format(
          mode=processor_config.get("Mode"),
        ),
      )

    processor_start = str(item_processor["StartAt"])
    inline_definition = {
      "StartAt": processor_start,
      "States": item_processor["States"],
    }
    item_selector_template = state_definition.get("ItemSelector", {})

    max_concurrency_raw = state_definition.get("MaxConcurrency", 1)

    max_workers = int(max_concurrency_raw)

    if max_workers < 1:
      max_workers = 1

    map_results: list[typing.Any] = [None] * len(items_list)

    def run_one_map_item(item_index: int, item_value: typing.Any) -> typing.Any:
      item_input_document = creatify_as.sfn.asl_path_utility.build_object_from_template(
        item_selector_template,
        input_document,
        creatify_as.sfn.asl_path_utility.build_intrinsic_context(
          execution_id=self._execution_id,
          execution_name=self._execution_name,
          state_machine_id=self._state_machine_id,
          map_item_value=item_value,
          map_item_index=item_index,
        ),
      )

      item_engine = AslSubgraphEngine(
        task_handler_by_function_key=self._task_handler_by_function_key,
        sleep_fn=self._sleep_fn,
        step_delay_seconds=self._step_delay_seconds,
        execution_id=self._execution_id,
        execution_name=self._execution_name,
        state_machine_id=self._state_machine_id,
      )

      item_terminal = item_engine.run_subgraph_until_terminal(
        definition=inline_definition,
        start_state_name=processor_start,
        input_document=item_input_document,
        map_item_value=item_value,
        map_item_index=item_index,
      )

      if item_terminal.status_name != "SUCCEEDED":
        raise creatify_as.sfn.asl_task_utility.StatesTaskFailedError(
          cause_plaintext=item_terminal.cause_plaintext or item_terminal.error_name or "Map item failed",
        )

      return item_terminal.output_document

    try:
      with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_by_index: dict[int, concurrent.futures.Future[typing.Any]] = {}

        for item_index, item_value in enumerate(items_list):
          future_by_index[item_index] = executor.submit(
            run_one_map_item,
            item_index,
            item_value,
          )

        for item_index in range(len(items_list)):
          map_results[item_index] = future_by_index[item_index].result()
    except creatify_as.sfn.asl_task_utility.StatesTaskFailedError as map_item_failed:
      catch_outcome = self._apply_catch_rules(
        state_definition.get("Catch", []),
        map_item_failed,
        input_document,
        intrinsic_context,
      )

      if catch_outcome is not None:
        return catch_outcome

      return AslSingleStateOutcome(
        next_state_name=None,
        output_document=input_document,
        terminal_outcome=AslTerminalOutcome(
          status_name="FAILED",
          output_document=input_document,
          error_name=map_item_failed.error_name,
          cause_plaintext=map_item_failed.cause_plaintext,
        ),
      )

    merged_document = creatify_as.sfn.asl_path_utility.apply_result_path(
      input_document,
      state_definition.get("ResultPath"),
      map_results,
    )

    if state_definition.get("End") is True:
      return AslSingleStateOutcome(
        next_state_name=None,
        output_document=merged_document,
        terminal_outcome=AslTerminalOutcome(
          status_name="SUCCEEDED",
          output_document=merged_document,
        ),
      )

    return AslSingleStateOutcome(
      next_state_name=str(state_definition["Next"]),
      output_document=merged_document,
    )
