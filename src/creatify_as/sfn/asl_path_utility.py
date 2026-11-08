"""Minimal JSONPath resolution for Step Functions ASL simulation."""

import copy
import typing


class StatesRuntimeError(Exception):
  """Raised when JSONPath resolution fails (States.Runtime)."""

  def __init__(self, cause_plaintext: str) -> None:
    super().__init__(cause_plaintext)
    self.error_name = "States.Runtime"
    self.cause_plaintext = cause_plaintext


def build_intrinsic_context(
  *,
  execution_id: str,
  execution_name: str,
  state_machine_id: str,
  map_item_value: typing.Any | None = None,
  map_item_index: int | None = None,
) -> dict[str, typing.Any]:
  """Return the $$. context object for path resolution."""

  intrinsic_context: dict[str, typing.Any] = {
    "Execution": {
      "Id": execution_id,
      "Name": execution_name,
    },
    "StateMachine": {
      "Id": state_machine_id,
    },
  }

  if map_item_value is not None or map_item_index is not None:
    intrinsic_context["Map"] = {
      "Item": {
        "Value": map_item_value,
        "Index": map_item_index,
      },
    }

  return intrinsic_context


def resolve_json_path(
  path_plaintext: str,
  document: typing.Any,
  intrinsic_context: dict[str, typing.Any],
) -> typing.Any:
  """Resolve one ASL path against $ or $$. roots."""

  if not path_plaintext.startswith("$"):
    raise StatesRuntimeError(
      "JSONPath not found: path must start with $ (got {path})".format(
        path=path_plaintext,
      ),
    )

  if path_plaintext.startswith("$$."):
    return _walk_path_segments(
      intrinsic_context,
      path_plaintext[3:],
    )

  if path_plaintext == "$":
    return document

  if path_plaintext.startswith("$."):
    return _walk_path_segments(
      document,
      path_plaintext[2:],
    )

  if path_plaintext.startswith("$["):
    return _walk_path_with_brackets(
      document,
      path_plaintext[1:],
    )

  raise StatesRuntimeError(
    "JSONPath not found: unsupported path {path}".format(
      path=path_plaintext,
    ),
  )


def _walk_path_segments(root_value: typing.Any, dotted_suffix: str) -> typing.Any:
  """Follow dot-separated segments from one mapping root."""

  if dotted_suffix == "":
    return root_value

  current_value = root_value
  segment_tokens = _split_dotted_path(dotted_suffix)

  for segment_token in segment_tokens:
    bracket_suffix = ""
    base_segment = segment_token

    if "[" in segment_token:
      bracket_index = segment_token.index("[")
      base_segment = segment_token[:bracket_index]
      bracket_suffix = segment_token[bracket_index:]

    if base_segment != "":
      if not isinstance(current_value, dict):
        raise StatesRuntimeError("JSONPath not found")

      try:
        current_value = current_value[base_segment]
      except KeyError as key_error:
        raise StatesRuntimeError("JSONPath not found") from key_error

    if bracket_suffix != "":
      current_value = _walk_path_with_brackets(
        current_value,
        bracket_suffix,
      )

  return current_value


def _walk_path_with_brackets(root_value: typing.Any, bracket_suffix: str) -> typing.Any:
  """Follow [n] segments against a list or dict root."""

  if bracket_suffix == "":
    return root_value

  if not bracket_suffix.startswith("["):
    raise StatesRuntimeError("JSONPath not found")

  current_value = root_value
  position = 0

  while position < len(bracket_suffix):
    if bracket_suffix[position] != "[":
      raise StatesRuntimeError("JSONPath not found")

    close_bracket = bracket_suffix.index("]", position)
    index_token = bracket_suffix[position + 1:close_bracket]
    position = close_bracket + 1

    if not index_token.isdigit():
      raise StatesRuntimeError("JSONPath not found")

    index_number = int(index_token)

    if not isinstance(current_value, list):
      raise StatesRuntimeError("JSONPath not found")

    if index_number < 0 or index_number >= len(current_value):
      raise StatesRuntimeError("JSONPath not found")

    current_value = current_value[index_number]

  return current_value


def _split_dotted_path(dotted_suffix: str) -> list[str]:
  """Split one dotted path without breaking bracketed segments."""

  segments: list[str] = []
  buffer_characters: list[str] = []

  for character in dotted_suffix:
    if character == "." and "[" not in "".join(buffer_characters):
      if buffer_characters:
        segments.append("".join(buffer_characters))
        buffer_characters = []

      continue

    buffer_characters.append(character)

  if buffer_characters:
    segments.append("".join(buffer_characters))

  return segments


def build_object_from_template(
  template_value: typing.Any,
  document: typing.Any,
  intrinsic_context: dict[str, typing.Any],
) -> typing.Any:
  """Resolve Parameters / ItemSelector templates including keys ending in .$ ."""

  if isinstance(template_value, dict):
    resolved_mapping: dict[str, typing.Any] = {}

    for key_plaintext, nested_value in template_value.items():
      if key_plaintext.endswith(".$"):
        output_key = key_plaintext[:-2]

        if not isinstance(nested_value, str):
          raise StatesRuntimeError(
            "JSONPath reference value must be a string for key {key}".format(
              key=key_plaintext,
            ),
          )

        resolved_mapping[output_key] = resolve_json_path(
          nested_value,
          document,
          intrinsic_context,
        )

        continue

      resolved_mapping[key_plaintext] = build_object_from_template(
        nested_value,
        document,
        intrinsic_context,
      )

    return resolved_mapping

  if isinstance(template_value, list):
    return [
      build_object_from_template(element, document, intrinsic_context)
      for element in template_value
    ]

  return copy.deepcopy(template_value)


def apply_output_path(
  state_output: typing.Any,
  output_path_plaintext: str | None,
  document: typing.Any,
  intrinsic_context: dict[str, typing.Any],
) -> typing.Any:
  """Apply OutputPath when present; otherwise return state output unchanged."""

  if output_path_plaintext is None:
    return state_output

  combined_document = state_output

  if output_path_plaintext == "$":
    return combined_document

  return resolve_json_path(
    output_path_plaintext,
    combined_document,
    intrinsic_context,
  )


def apply_result_path(
  parent_document: typing.Any,
  result_path_plaintext: str | None,
  state_output: typing.Any,
) -> typing.Any:
  """Merge state output into the parent document at ResultPath."""

  if result_path_plaintext is None:
    return state_output

  if result_path_plaintext == "$":
    return state_output

  if not result_path_plaintext.startswith("$.") and result_path_plaintext != "$":
    raise StatesRuntimeError("JSONPath not found")

  merged_document = copy.deepcopy(parent_document)

  if not isinstance(merged_document, dict):
    raise StatesRuntimeError("JSONPath not found")

  path_segments = result_path_plaintext[2:].split(".")
  walk_mapping = merged_document

  for segment_index, segment_plaintext in enumerate(path_segments):
    is_last = segment_index == len(path_segments) - 1

    if is_last:
      walk_mapping[segment_plaintext] = copy.deepcopy(state_output)

      break

    try:
      next_value = walk_mapping[segment_plaintext]
    except KeyError as key_error:
      raise StatesRuntimeError("JSONPath not found") from key_error

    if not isinstance(next_value, dict):
      raise StatesRuntimeError("JSONPath not found")

    walk_mapping = next_value

  return merged_document
