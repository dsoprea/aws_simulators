"""Choice state evaluation for ASL simulation."""

import typing

import creatify_as.sfn.asl_path_utility


def resolve_choice_next_state(
  state_definition: dict[str, typing.Any],
  input_document: typing.Any,
  intrinsic_context: dict[str, typing.Any],
) -> str:
  """Return the next state name for one Choice state."""

  choices_list = state_definition.get("Choices", [])

  for choice_rule in choices_list:
    if _choice_rule_matches(choice_rule, input_document, intrinsic_context):
      return str(choice_rule["Next"])

  if "Default" not in state_definition:
    raise ValueError("Choice state missing Default branch")

  return str(state_definition["Default"])


def _choice_rule_matches(
  choice_rule: dict[str, typing.Any],
  input_document: typing.Any,
  intrinsic_context: dict[str, typing.Any],
) -> bool:
  """Return whether one Choice rule matches the input document."""

  if "BooleanEquals" in choice_rule:
    variable_path = choice_rule["Variable"]

    field_value = creatify_as.sfn.asl_path_utility.resolve_json_path(
      variable_path,
      input_document,
      intrinsic_context,
    )

    return field_value == choice_rule["BooleanEquals"]

  if "StringEquals" in choice_rule:
    variable_path = choice_rule["Variable"]

    field_value = creatify_as.sfn.asl_path_utility.resolve_json_path(
      variable_path,
      input_document,
      intrinsic_context,
    )

    return str(field_value) == str(choice_rule["StringEquals"])

  if "IsPresent" in choice_rule:
    variable_path = choice_rule["Variable"]
    is_present_required = bool(choice_rule["IsPresent"])

    try:
      creatify_as.sfn.asl_path_utility.resolve_json_path(
        variable_path,
        input_document,
        intrinsic_context,
      )
    except creatify_as.sfn.asl_path_utility.StatesRuntimeError:
      return not is_present_required

    return is_present_required

  raise ValueError(
    "unsupported Choice rule keys={keys}".format(
      keys=sorted(choice_rule.keys()),
    ),
  )
