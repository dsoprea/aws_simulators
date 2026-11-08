"""Lambda invoke Task helpers for ASL simulation."""

import typing

import creatify_as.sfn.asl_path_utility

LAMBDA_INVOKE_RESOURCE_ARN = "arn:aws:states:::lambda:invoke"


class StatesTaskFailedError(Exception):
  """Raised when a simulated Lambda task fails (States.TaskFailed)."""

  def __init__(self, *, cause_plaintext: str, error_name: str = "States.TaskFailed") -> None:
    super().__init__(cause_plaintext)
    self.error_name = error_name
    self.cause_plaintext = cause_plaintext


def normalize_function_lookup_key(function_name_plaintext: str) -> str:
  """Return the handler map key for one Task FunctionName or ARN."""

  if ":function:" in function_name_plaintext:
    function_segment = function_name_plaintext.split(":function:")[-1]

    if function_segment.count(":") >= 1:
      return function_segment.split(":")[0]

    return function_segment

  return function_name_plaintext


def resolve_task_handler(
  function_name_plaintext: str,
  task_handler_by_function_key: dict[str, typing.Callable[..., dict[str, typing.Any]]],
) -> typing.Callable[..., dict[str, typing.Any]]:
  """Look up one handler by full name or normalized Lambda name segment."""

  if function_name_plaintext in task_handler_by_function_key:
    return task_handler_by_function_key[function_name_plaintext]

  normalized_key = normalize_function_lookup_key(function_name_plaintext)

  if normalized_key in task_handler_by_function_key:
    return task_handler_by_function_key[normalized_key]

  raise ValueError(
    "no task handler registered for FunctionName={function_name}".format(
      function_name=function_name_plaintext,
    ),
  )


def build_lambda_invoke_response(handler_payload: dict[str, typing.Any]) -> dict[str, typing.Any]:
  """Wrap one handler return value like boto3 lambda:invoke Payload."""

  return {
    "StatusCode": 200,
    "ExecutedVersion": "$LATEST",
    "Payload": handler_payload,
  }


def invoke_lambda_task(
  *,
  state_definition: dict[str, typing.Any],
  input_document: typing.Any,
  intrinsic_context: dict[str, typing.Any],
  task_handler_by_function_key: dict[str, typing.Callable[..., dict[str, typing.Any]]],
) -> dict[str, typing.Any]:
  """Run one lambda:invoke Task and return the raw invoke response dict."""

  resource_plaintext = state_definition.get("Resource", "")

  if resource_plaintext != LAMBDA_INVOKE_RESOURCE_ARN:
    raise ValueError(
      "unsupported Task Resource={resource}; only {supported} is implemented".format(
        resource=resource_plaintext,
        supported=LAMBDA_INVOKE_RESOURCE_ARN,
      ),
    )

  parameters_template = state_definition.get("Parameters")

  if parameters_template is None:
    raise ValueError("Task Parameters are required for lambda:invoke simulation")

  resolved_parameters = creatify_as.sfn.asl_path_utility.build_object_from_template(
    parameters_template,
    input_document,
    intrinsic_context,
  )

  try:
    function_name_plaintext = resolved_parameters["FunctionName"]
  except KeyError as key_error:
    raise ValueError("lambda:invoke Parameters must include FunctionName") from key_error

  task_handler = resolve_task_handler(
    str(function_name_plaintext),
    task_handler_by_function_key,
  )

  if "Payload.$" in resolved_parameters:
    payload_document = resolved_parameters["Payload.$"]
  else:
    try:
      payload_document = resolved_parameters["Payload"]
    except KeyError as key_error:
      raise ValueError("lambda:invoke Parameters must include Payload") from key_error

  if not isinstance(payload_document, dict):
    raise StatesTaskFailedError(cause_plaintext="Task payload must be a JSON object")

  try:
    handler_result = task_handler(payload_document)
  except StatesTaskFailedError:
    raise
  except Exception as task_exception:
    raise StatesTaskFailedError(cause_plaintext=str(task_exception)) from task_exception

  if not isinstance(handler_result, dict):
    raise StatesTaskFailedError(cause_plaintext="Task handler must return a JSON object")

  return build_lambda_invoke_response(handler_result)


def apply_result_selector(
  invoke_response: dict[str, typing.Any],
  result_selector_template: dict[str, typing.Any] | None,
  intrinsic_context: dict[str, typing.Any],
) -> typing.Any:
  """Build task output after ResultSelector when configured."""

  if result_selector_template is None:
    return invoke_response

  return creatify_as.sfn.asl_path_utility.build_object_from_template(
    result_selector_template,
    invoke_response,
    intrinsic_context,
  )
