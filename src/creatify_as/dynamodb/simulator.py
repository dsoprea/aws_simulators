"""Generic in-memory DynamoDB simulator for tests without live AWS calls."""

import copy
import dataclasses
import decimal
import re
import typing

import boto3.dynamodb.types
import botocore.exceptions


_ITEM_TYPE_DESERIALIZER = boto3.dynamodb.types.TypeDeserializer()


@dataclasses.dataclass(frozen=True)
class DynamoDbSimulatorConfig:
  """Table and index metadata that parameterizes one in-memory DynamoDB store."""

  allowed_table_names: frozenset[str]
  table_primary_key_attribute_names_by_table_name: dict[str, tuple[str, ...]]
  gsi_key_attribute_names_by_index_name: dict[str, tuple[str, str | None]]


def _deserialize_attribute_value(attribute_map: dict[str, typing.Any]) -> typing.Any:
  """Return one Python value from a low-level DynamoDB attribute map."""

  return _ITEM_TYPE_DESERIALIZER.deserialize(attribute_map)


def _deserialize_item(item_map: dict[str, dict[str, typing.Any]]) -> dict[str, typing.Any]:
  """Return one Python item dict from a low-level DynamoDB item map."""

  return {
    attribute_name: _deserialize_attribute_value(attribute_value)
    for attribute_name, attribute_value in item_map.items()
  }


def _deserialize_key(key_map: dict[str, dict[str, typing.Any]]) -> dict[str, typing.Any]:
  """Return one primary-key dict from a low-level DynamoDB key map."""

  return _deserialize_item(key_map)


def _coerce_numeric(value: typing.Any) -> float:
  """Coerce *value* to float for Dynamo-style numeric comparisons and arithmetic."""

  if isinstance(value, decimal.Decimal):
    return float(value)

  return float(value)


def _normalize_expression_whitespace(expression: str) -> str:
  """Collapse repeated whitespace in one DynamoDB expression string."""

  return " ".join(expression.split())


def _split_and_clauses(expression: str) -> list[str]:
  """Split one condition expression on ``AND`` while preserving clause text."""

  normalized_expression = _normalize_expression_whitespace(expression)
  clause_parts = re.split(r"\s+AND\s+", normalized_expression)

  return [clause_part.strip() for clause_part in clause_parts if clause_part.strip() != ""]


def _resolve_attribute_name(
  attribute_token: str,
  expression_attribute_names: dict[str, str] | None,
) -> str:
  """Resolve ``#placeholder`` tokens to real attribute names."""

  if attribute_token.startswith("#"):
    if expression_attribute_names is None:
      raise ValueError(
        "missing ExpressionAttributeNames for placeholder {placeholder}".format(
          placeholder=attribute_token))

    return expression_attribute_names[attribute_token]

  return attribute_token


def _resolve_attribute_value(
  value_token: str,
  expression_attribute_values: dict[str, typing.Any] | None,
) -> typing.Any:
  """Resolve ``:placeholder`` tokens to bound values."""

  if value_token.startswith(":"):
    if expression_attribute_values is None:
      raise ValueError(
        "missing ExpressionAttributeValues for placeholder {placeholder}".format(
          placeholder=value_token))

    return expression_attribute_values[value_token]

  return value_token


def _read_item_attribute(
  item: dict[str, typing.Any],
  attribute_name: str,
) -> typing.Any:
  """Return one attribute from *item*, treating a missing key as absent."""

  try:
    return item[attribute_name]
  except KeyError:
    return None


def _attribute_exists(item: dict[str, typing.Any], attribute_name: str) -> bool:
  """Return whether *attribute_name* is present on *item*."""

  try:
    item[attribute_name]
  except KeyError:
    return False

  return True


def _evaluate_comparison_clause(
  clause: str,
  item: dict[str, typing.Any],
  expression_attribute_names: dict[str, str] | None,
  expression_attribute_values: dict[str, typing.Any] | None,
) -> bool:
  """Evaluate one ``attribute op value`` condition clause against *item*."""

  comparison_match = re.fullmatch(
    r"(?P<left>#[A-Za-z0-9_]+|[A-Za-z0-9_]+)\s*(?P<operator>=|<>|<=|>=|<|>)\s*(?P<right>:[A-Za-z0-9_]+)",
    clause,
  )

  if comparison_match is None:
    raise ValueError("unsupported condition clause: {clause}".format(clause=clause))

  attribute_name = _resolve_attribute_name(
    comparison_match.group("left"),
    expression_attribute_names)
  comparison_operator = comparison_match.group("operator")
  comparison_value = _resolve_attribute_value(
    comparison_match.group("right"),
    expression_attribute_values)
  item_value = _read_item_attribute(item, attribute_name)

  if comparison_operator == "=":
    return item_value == comparison_value

  if item_value is None:
    return False

  item_number = _coerce_numeric(item_value)
  comparison_number = _coerce_numeric(comparison_value)

  if comparison_operator == ">":
    return item_number > comparison_number

  if comparison_operator == ">=":
    return item_number >= comparison_number

  if comparison_operator == "<":
    return item_number < comparison_number

  if comparison_operator == "<=":
    return item_number <= comparison_number

  if comparison_operator == "<>":
    return item_value != comparison_value

  raise ValueError(
    "unsupported comparison operator {operator}".format(operator=comparison_operator))


def _evaluate_condition_expression(
  condition_expression: str,
  item: dict[str, typing.Any],
  expression_attribute_names: dict[str, str] | None,
  expression_attribute_values: dict[str, typing.Any] | None,
) -> bool:
  """Return whether *item* satisfies one DynamoDB condition expression."""

  for clause in _split_and_clauses(condition_expression):
    function_match = re.fullmatch(
      r"(?P<function_name>attribute_exists|attribute_not_exists)\((?P<attribute_token>#[A-Za-z0-9_]+|[A-Za-z0-9_]+)\)",
      clause,
    )

    if function_match is not None:
      attribute_name = _resolve_attribute_name(
        function_match.group("attribute_token"),
        expression_attribute_names)
      attribute_is_present = _attribute_exists(item, attribute_name)

      if function_match.group("function_name") == "attribute_exists":
        if not attribute_is_present:
          return False

        continue

      if attribute_is_present:
        return False

      continue

    if not _evaluate_comparison_clause(
      clause,
      item,
      expression_attribute_names,
      expression_attribute_values):
      return False

  return True


def _apply_set_action(
  item: dict[str, typing.Any],
  action_text: str,
  expression_attribute_names: dict[str, str] | None,
  expression_attribute_values: dict[str, typing.Any],
) -> None:
  """Apply one ``SET`` assignment from an update expression to *item* in place."""

  assignment_match = re.fullmatch(
    r"(?P<left>#[A-Za-z0-9_]+|[A-Za-z0-9_]+)\s*=\s*(?P<right>.+)",
    action_text,
  )

  if assignment_match is None:
    raise ValueError("unsupported SET action: {action_text}".format(action_text=action_text))

  attribute_name = _resolve_attribute_name(
    assignment_match.group("left"),
    expression_attribute_names)
  right_hand_side = assignment_match.group("right").strip()

  arithmetic_match = re.fullmatch(
    r"(?P<source>#[A-Za-z0-9_]+|[A-Za-z0-9_]+)\s*-\s*(?P<delta>:[A-Za-z0-9_]+)",
    right_hand_side,
  )

  if arithmetic_match is not None:
    source_attribute_name = _resolve_attribute_name(
      arithmetic_match.group("source"),
      expression_attribute_names)
    delta_value = _resolve_attribute_value(
      arithmetic_match.group("delta"),
      expression_attribute_values)
    source_value = _read_item_attribute(item, source_attribute_name)

    if source_value is None:
      raise ValueError(
        "missing source attribute {attribute_name} for arithmetic update".format(
          attribute_name=source_attribute_name))

    item[attribute_name] = _coerce_numeric(source_value) - _coerce_numeric(delta_value)

    return

  increment_match = re.fullmatch(
    r"(?P<source>#[A-Za-z0-9_]+|[A-Za-z0-9_]+)\s*\+\s*(?P<delta>:[A-Za-z0-9_]+)",
    right_hand_side,
  )

  if increment_match is not None:
    source_attribute_name = _resolve_attribute_name(
      increment_match.group("source"),
      expression_attribute_names)
    delta_value = _resolve_attribute_value(
      increment_match.group("delta"),
      expression_attribute_values)
    source_value = _read_item_attribute(item, source_attribute_name)

    if source_value is None:
      raise ValueError(
        "missing source attribute {attribute_name} for arithmetic update".format(
          attribute_name=source_attribute_name))

    item[attribute_name] = _coerce_numeric(source_value) + _coerce_numeric(delta_value)

    return

  if right_hand_side.startswith(":"):
    item[attribute_name] = _resolve_attribute_value(
      right_hand_side,
      expression_attribute_values)

    return

  raise ValueError("unsupported SET right-hand side: {right_hand_side}".format(
    right_hand_side=right_hand_side))


def _apply_update_expression(
  item: dict[str, typing.Any],
  update_expression: str,
  expression_attribute_names: dict[str, str] | None,
  expression_attribute_values: dict[str, typing.Any],
) -> dict[str, typing.Any]:
  """Return *item* after applying one DynamoDB ``UpdateExpression``."""

  updated_item = copy.deepcopy(item)
  normalized_expression = _normalize_expression_whitespace(update_expression)

  if normalized_expression.startswith("SET "):
    # Split SET and optional REMOVE sections.
    remove_index = normalized_expression.find(" REMOVE ")

    if remove_index >= 0:
      set_section = normalized_expression[len("SET "):remove_index]
      remove_section = normalized_expression[remove_index + len(" REMOVE "):]
    else:
      set_section = normalized_expression[len("SET "):]
      remove_section = ""

    for action_text in set_section.split(","):
      trimmed_action_text = action_text.strip()

      if trimmed_action_text == "":
        continue

      _apply_set_action(
        updated_item,
        trimmed_action_text,
        expression_attribute_names,
        expression_attribute_values)

    if remove_section != "":
      for attribute_token in remove_section.split(","):
        attribute_name = _resolve_attribute_name(
          attribute_token.strip(),
          expression_attribute_names)

        try:
          del updated_item[attribute_name]
        except KeyError:
          pass

    return updated_item

  raise ValueError(
    "unsupported update expression: {update_expression}".format(
      update_expression=update_expression))


def _evaluate_boto3_key_condition(
  key_condition_expression: typing.Any,
  item: dict[str, typing.Any],
) -> bool:
  """Return whether *item* matches one boto3 ``Key`` condition expression."""

  expression_operator = key_condition_expression.expression_operator
  condition_values = key_condition_expression._values

  if expression_operator == "AND":
    return all(
      _evaluate_boto3_key_condition(nested_condition, item)
      for nested_condition in condition_values)

  attribute_name = condition_values[0].name
  comparison_value = condition_values[1]
  item_value = _read_item_attribute(item, attribute_name)

  if expression_operator == "=":
    return item_value == comparison_value

  if item_value is None:
    return False

  try:
    item_number = _coerce_numeric(item_value)
    comparison_number = _coerce_numeric(comparison_value)
  except (TypeError, ValueError):
    item_text = str(item_value)
    comparison_text = str(comparison_value)

    if expression_operator == "<":
      return item_text < comparison_text

    if expression_operator == "<=":
      return item_text <= comparison_text

    if expression_operator == ">":
      return item_text > comparison_text

    if expression_operator == ">=":
      return item_text >= comparison_text

    raise ValueError(
      "unsupported boto3 key condition operator {operator}".format(operator=expression_operator))

  if expression_operator == "<":
    return item_number < comparison_number

  if expression_operator == "<=":
    return item_number <= comparison_number

  if expression_operator == ">":
    return item_number > comparison_number

  if expression_operator == ">=":
    return item_number >= comparison_number

  raise ValueError(
    "unsupported boto3 key condition operator {operator}".format(operator=expression_operator))


def _build_primary_key_tuple(
  table_name: str,
  key: dict[str, typing.Any],
  table_primary_key_attribute_names_by_table_name: dict[str, tuple[str, ...]],
) -> tuple[typing.Any, ...]:
  """Return the storage key tuple for one DynamoDB primary key."""

  primary_key_attribute_names = table_primary_key_attribute_names_by_table_name[table_name]

  return tuple(key[attribute_name] for attribute_name in primary_key_attribute_names)


def _build_transaction_canceled_error() -> botocore.exceptions.ClientError:
  """Return a ``TransactionCanceledException`` shaped like DynamoDB transact failures."""

  response = {
    "Error": {
      "Code": "TransactionCanceledException",
      "Message": "Transaction cancelled, please refer cancellation reasons for specific reasons",
    },
    "CancellationReasons": [
      {
        "Code": "ConditionalCheckFailed",
        "Message": "The conditional request failed",
      },
    ],
  }

  return botocore.exceptions.ClientError(response, "TransactWriteItems")


class DynamoDbSimulator:
  """In-memory DynamoDB store parameterized by table and GSI metadata."""

  def __init__(self, config: DynamoDbSimulatorConfig) -> None:
    self._config = config
    self._items_by_table_name_and_primary_key: dict[str, dict[tuple[typing.Any, ...], dict[str, typing.Any]]] = {}

  def clear(self) -> None:
    """Remove every stored table row."""

    self._items_by_table_name_and_primary_key = {}

  def _build_row_primary_key_tuple(self, table_name: str, key: dict[str, typing.Any]) -> tuple[typing.Any, ...]:
    """Return the storage key tuple for one row primary key."""

    return _build_primary_key_tuple(
      table_name,
      key,
      self._config.table_primary_key_attribute_names_by_table_name)

  def seed_item(self, table_name: str, item: dict[str, typing.Any]) -> None:
    """Insert or replace one row for test setup."""

    self._ensure_table_name(table_name)
    primary_key_tuple = self._build_row_primary_key_tuple(table_name, item)
    table_items = self._items_by_table_name_and_primary_key.setdefault(table_name, {})
    table_items[primary_key_tuple] = copy.deepcopy(item)

  def get_item_or_none(
    self,
    table_name: str,
    key: dict[str, typing.Any],
  ) -> dict[str, typing.Any] | None:
    """Return one row for *key*, or ``None`` when absent."""

    self._ensure_table_name(table_name)
    primary_key_tuple = self._build_row_primary_key_tuple(table_name, key)
    table_items = self._items_by_table_name_and_primary_key.get(table_name, {})
    stored_item = table_items.get(primary_key_tuple)

    if stored_item is None:
      return None

    return copy.deepcopy(stored_item)

  def put_item(
    self,
    table_name: str,
    item: dict[str, typing.Any],
    *,
    condition_expression: str | None = None,
    expression_attribute_names: dict[str, str] | None = None,
    expression_attribute_values: dict[str, typing.Any] | None = None,
  ) -> None:
    """Upsert one row, optionally guarded by a condition expression."""

    self._ensure_table_name(table_name)
    primary_key_tuple = self._build_row_primary_key_tuple(table_name, item)
    table_items = self._items_by_table_name_and_primary_key.setdefault(table_name, {})
    existing_item = table_items.get(primary_key_tuple, {})

    if condition_expression is not None:
      if not _evaluate_condition_expression(
        condition_expression,
        existing_item,
        expression_attribute_names,
        expression_attribute_values):
        raise self._build_conditional_check_failed_error("PutItem")

    table_items[primary_key_tuple] = copy.deepcopy(item)

  def update_item(
    self,
    table_name: str,
    key: dict[str, typing.Any],
    *,
    update_expression: str,
    condition_expression: str | None = None,
    expression_attribute_names: dict[str, str] | None = None,
    expression_attribute_values: dict[str, typing.Any],
  ) -> dict[str, typing.Any]:
    """Apply one update expression to one row and return a minimal response."""

    self._ensure_table_name(table_name)
    primary_key_tuple = self._build_row_primary_key_tuple(table_name, key)
    table_items = self._items_by_table_name_and_primary_key.setdefault(table_name, {})
    existing_item = table_items.get(primary_key_tuple)

    if existing_item is None:
      if condition_expression is not None:
        raise self._build_conditional_check_failed_error("UpdateItem")

      existing_item = {}

    if condition_expression is not None:
      if not _evaluate_condition_expression(
        condition_expression,
        existing_item,
        expression_attribute_names,
        expression_attribute_values):
        raise self._build_conditional_check_failed_error("UpdateItem")

    updated_item = _apply_update_expression(
      existing_item,
      update_expression,
      expression_attribute_names,
      expression_attribute_values)
    table_items[primary_key_tuple] = updated_item

    return {
      "ResponseMetadata": {
        "HTTPStatusCode": 200,
      },
    }

  def delete_item(
    self,
    table_name: str,
    key: dict[str, typing.Any],
    *,
    condition_expression: str | None = None,
    expression_attribute_names: dict[str, str] | None = None,
    expression_attribute_values: dict[str, typing.Any] | None = None,
  ) -> dict[str, typing.Any]:
    """Delete one row when its condition expression passes."""

    self._ensure_table_name(table_name)
    primary_key_tuple = self._build_row_primary_key_tuple(table_name, key)
    table_items = self._items_by_table_name_and_primary_key.get(table_name, {})
    existing_item = table_items.get(primary_key_tuple, {})

    if condition_expression is not None:
      if not _evaluate_condition_expression(
        condition_expression,
        existing_item,
        expression_attribute_names,
        expression_attribute_values):
        raise self._build_conditional_check_failed_error("DeleteItem")

    if primary_key_tuple in table_items:
      del table_items[primary_key_tuple]

    return {
      "ResponseMetadata": {
        "HTTPStatusCode": 200,
      },
    }

  def query_collect_items(
    self,
    table_name: str,
    **query_keyword_arguments: typing.Any,
  ) -> list[dict[str, typing.Any]]:
    """Return rows matching one key-condition expression."""

    self._ensure_table_name(table_name)
    index_name = query_keyword_arguments.get("IndexName")
    key_condition_expression = query_keyword_arguments.get("KeyConditionExpression")
    table_items = self._items_by_table_name_and_primary_key.get(table_name, {})
    matching_items: list[dict[str, typing.Any]] = []

    for stored_item in table_items.values():
      if index_name is not None:
        gsi_key_attribute_names = self._config.gsi_key_attribute_names_by_index_name[index_name]
        hash_key_attribute_name = gsi_key_attribute_names[0]
        sort_key_attribute_name = gsi_key_attribute_names[1]

        if not _attribute_exists(stored_item, hash_key_attribute_name):
          continue

        if sort_key_attribute_name is not None:
          if not _attribute_exists(stored_item, sort_key_attribute_name):
            continue

      if hasattr(key_condition_expression, "expression_operator"):
        if not _evaluate_boto3_key_condition(key_condition_expression, stored_item):
          continue
      else:
        resolved_expression_attribute_names = query_keyword_arguments.get("ExpressionAttributeNames")
        resolved_expression_attribute_values = query_keyword_arguments.get("ExpressionAttributeValues")
        condition_expression_string = key_condition_expression

        if condition_expression_string is None:
          raise ValueError("KeyConditionExpression is required")

        if not _evaluate_condition_expression(
          condition_expression_string,
          stored_item,
          resolved_expression_attribute_names,
          resolved_expression_attribute_values):
          continue

      matching_items.append(copy.deepcopy(stored_item))

    if index_name is not None:
      sort_key_attribute_name = self._config.gsi_key_attribute_names_by_index_name[index_name][1]

      if sort_key_attribute_name is not None:
        matching_items.sort(key=lambda item: item[sort_key_attribute_name])

    return matching_items

  def query_first_item_or_none(
    self,
    table_name: str,
    **query_keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any] | None:
    """Return the first query row, or ``None`` when no row matches."""

    matching_items = self.query_collect_items(table_name, **query_keyword_arguments)

    if len(matching_items) < 1:
      return None

    return matching_items[0]

  def scan_collect_items(
    self,
    table_name: str,
    **scan_keyword_arguments: typing.Any,
  ) -> list[dict[str, typing.Any]]:
    """Return every row in *table_name*, optionally filtered by ``FilterExpression``."""

    self._ensure_table_name(table_name)
    filter_expression = scan_keyword_arguments.get("FilterExpression")
    table_items = self._items_by_table_name_and_primary_key.get(table_name, {})
    matching_items: list[dict[str, typing.Any]] = []

    for stored_item in table_items.values():
      if filter_expression is not None:
        if hasattr(filter_expression, "expression_operator"):
          if not _evaluate_boto3_key_condition(filter_expression, stored_item):
            continue
        elif not _evaluate_condition_expression(
          filter_expression,
          stored_item,
          scan_keyword_arguments.get("ExpressionAttributeNames"),
          scan_keyword_arguments.get("ExpressionAttributeValues")):
          continue

      matching_items.append(copy.deepcopy(stored_item))

    return matching_items

  def scan_all_items(self, table_name: str) -> list[dict[str, typing.Any]]:
    """Return every row stored for *table_name*."""

    return self.scan_collect_items(table_name)

  def transact_write_items(self, transact_items: list[dict[str, typing.Any]]) -> None:
    """Execute one ``TransactWriteItems`` request against the in-memory tables."""

    staged_writes: list[tuple[str, tuple[typing.Any, ...], dict[str, typing.Any] | None]] = []

    for transact_item in transact_items:
      if "Put" in transact_item:
        put_request = transact_item["Put"]
        table_name = put_request["TableName"]
        self._ensure_table_name(table_name)
        item = _deserialize_item(put_request["Item"])
        primary_key_tuple = self._build_row_primary_key_tuple(table_name, item)
        table_items = self._items_by_table_name_and_primary_key.setdefault(table_name, {})
        existing_item = table_items.get(primary_key_tuple, {})
        condition_expression = put_request.get("ConditionExpression")
        expression_attribute_names = put_request.get("ExpressionAttributeNames")
        expression_attribute_values = None

        if "ExpressionAttributeValues" in put_request:
          expression_attribute_values = {
            placeholder: _deserialize_attribute_value(attribute_value)
            for placeholder, attribute_value in put_request["ExpressionAttributeValues"].items()
          }

        if condition_expression is not None:
          if not _evaluate_condition_expression(
            condition_expression,
            existing_item,
            expression_attribute_names,
            expression_attribute_values):
            raise _build_transaction_canceled_error()

        staged_writes.append((table_name, primary_key_tuple, copy.deepcopy(item)))
        continue

      if "Update" in transact_item:
        update_request = transact_item["Update"]
        table_name = update_request["TableName"]
        self._ensure_table_name(table_name)
        key = _deserialize_key(update_request["Key"])
        primary_key_tuple = self._build_row_primary_key_tuple(table_name, key)
        table_items = self._items_by_table_name_and_primary_key.setdefault(table_name, {})
        existing_item = table_items.get(primary_key_tuple)

        if existing_item is None:
          raise _build_transaction_canceled_error()

        expression_attribute_values = {
          placeholder: _deserialize_attribute_value(attribute_value)
          for placeholder, attribute_value in update_request["ExpressionAttributeValues"].items()
        }
        condition_expression = update_request.get("ConditionExpression")

        if condition_expression is not None:
          if not _evaluate_condition_expression(
            condition_expression,
            existing_item,
            update_request.get("ExpressionAttributeNames"),
            expression_attribute_values):
            raise _build_transaction_canceled_error()

        updated_item = _apply_update_expression(
          existing_item,
          update_request["UpdateExpression"],
          update_request.get("ExpressionAttributeNames"),
          expression_attribute_values)
        staged_writes.append((table_name, primary_key_tuple, updated_item))
        continue

      if "Delete" in transact_item:
        delete_request = transact_item["Delete"]
        table_name = delete_request["TableName"]
        self._ensure_table_name(table_name)
        key = _deserialize_key(delete_request["Key"])
        primary_key_tuple = self._build_row_primary_key_tuple(table_name, key)
        table_items = self._items_by_table_name_and_primary_key.setdefault(table_name, {})
        existing_item = table_items.get(primary_key_tuple, {})
        expression_attribute_values = None

        if "ExpressionAttributeValues" in delete_request:
          expression_attribute_values = {
            placeholder: _deserialize_attribute_value(attribute_value)
            for placeholder, attribute_value in delete_request["ExpressionAttributeValues"].items()
          }
        condition_expression = delete_request.get("ConditionExpression")

        if condition_expression is not None:
          if not _evaluate_condition_expression(
            condition_expression,
            existing_item,
            delete_request.get("ExpressionAttributeNames"),
            expression_attribute_values):
            raise _build_transaction_canceled_error()

        staged_writes.append((table_name, primary_key_tuple, None))
        continue

      raise ValueError("unsupported transact item shape")

    for table_name, primary_key_tuple, item in staged_writes:
      table_items = self._items_by_table_name_and_primary_key.setdefault(table_name, {})

      if item is None:
        if primary_key_tuple in table_items:
          del table_items[primary_key_tuple]

        continue

      table_items[primary_key_tuple] = item

  def _ensure_table_name(self, table_name: str) -> None:
    """Raise when *table_name* is outside the configured fake scope."""

    if table_name not in self._config.allowed_table_names:
      raise ValueError(
        "DynamoDbSimulator only supports configured tables; got table_name={table_name}".format(
          table_name=table_name))

  def _build_conditional_check_failed_error(self, operation_name: str) -> botocore.exceptions.ClientError:
    """Return a ``ConditionalCheckFailedException`` for non-transact writes."""

    response = {
      "Error": {
        "Code": "ConditionalCheckFailedException",
        "Message": "The conditional request failed",
      },
    }

    return botocore.exceptions.ClientError(response, operation_name)
