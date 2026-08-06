"""Unit tests for ``DynamoDbSimulator`` semantics."""

import typing

import boto3.dynamodb.conditions
import boto3.dynamodb.types
import botocore.exceptions
import pytest

import creatify_as.dynamodb.simulator

_AWA_FLEETS_TABLE_NAME = "awa_fleets"
_AWA_STACKS_TABLE_NAME = "awa_stacks"
_AWA_ASSOCIATIONS_STUDENT_TABLE_NAME = "awa_associations_student"
_AWA_ASSOCIATIONS_STUDENT_QUEUE_TABLE_NAME = "awa_associations_student_queue"
_AWA_STACKS_QUEUE_TABLE_NAME = "awa_stacks_queue"

_FLEET_STATUS_AVAILABLE = "available"
_FLEET_STATUS_ALLOCATED = "allocated"
_STACK_STATUS_AVAILABLE = "available"
_STACK_STATUS_PREALLOCATED = "preallocated"
_STACK_STATUS_ALLOCATED = "allocated"

_GSI_NAME = "gsi_name"

_TEST_DYNAMODB_SIMULATOR_CONFIG = creatify_as.dynamodb.simulator.DynamoDbSimulatorConfig(
  allowed_table_names=frozenset({
    _AWA_FLEETS_TABLE_NAME,
    _AWA_STACKS_TABLE_NAME,
    _AWA_ASSOCIATIONS_STUDENT_TABLE_NAME,
    _AWA_ASSOCIATIONS_STUDENT_QUEUE_TABLE_NAME,
    _AWA_STACKS_QUEUE_TABLE_NAME,
  }),
  table_primary_key_attribute_names_by_table_name={
    _AWA_FLEETS_TABLE_NAME: ("id",),
    _AWA_STACKS_TABLE_NAME: ("id",),
    _AWA_ASSOCIATIONS_STUDENT_TABLE_NAME: ("id",),
    _AWA_ASSOCIATIONS_STUDENT_QUEUE_TABLE_NAME: ("student_association_id",),
    _AWA_STACKS_QUEUE_TABLE_NAME: ("stack_id",),
  },
  gsi_key_attribute_names_by_index_name={
    _GSI_NAME: ("name", None),
  },
)

_ITEM_TYPE_SERIALIZER = boto3.dynamodb.types.TypeSerializer()


def _serialize_attribute_value(value: typing.Any) -> dict[str, typing.Any]:
  """Return one low-level DynamoDB attribute map for *value*."""

  return _ITEM_TYPE_SERIALIZER.serialize(value)


def _serialize_item(item: dict[str, typing.Any]) -> dict[str, dict[str, typing.Any]]:
  """Return one low-level DynamoDB item map."""

  return {
    attribute_name: _serialize_attribute_value(attribute_value)
    for attribute_name, attribute_value in item.items()
  }


def _build_transact_put_item(
  table_name: str,
  item: dict[str, typing.Any],
  *,
  condition_expression: str | None = None,
  expression_attribute_names: dict[str, str] | None = None,
  expression_attribute_values: dict[str, typing.Any] | None = None,
) -> dict[str, typing.Any]:
  """Build one ``Put`` entry for ``TransactWriteItems``."""

  put_item_request: dict[str, typing.Any] = {
    "TableName": table_name,
    "Item": _serialize_item(item),
  }

  if condition_expression is not None:
    put_item_request["ConditionExpression"] = condition_expression

  if expression_attribute_names is not None:
    put_item_request["ExpressionAttributeNames"] = expression_attribute_names

  if expression_attribute_values is not None:
    put_item_request["ExpressionAttributeValues"] = {
      placeholder: _serialize_attribute_value(attribute_value)
      for placeholder, attribute_value in expression_attribute_values.items()
    }

  return {"Put": put_item_request}


def _build_transact_update_item(
  table_name: str,
  key: dict[str, typing.Any],
  *,
  update_expression: str,
  condition_expression: str | None = None,
  expression_attribute_names: dict[str, str] | None = None,
  expression_attribute_values: dict[str, typing.Any],
) -> dict[str, typing.Any]:
  """Build one ``Update`` entry for ``TransactWriteItems``."""

  update_item_request: dict[str, typing.Any] = {
    "TableName": table_name,
    "Key": _serialize_item(key),
    "UpdateExpression": update_expression,
    "ExpressionAttributeValues": {
      placeholder: _serialize_attribute_value(attribute_value)
      for placeholder, attribute_value in expression_attribute_values.items()
    },
  }

  if condition_expression is not None:
    update_item_request["ConditionExpression"] = condition_expression

  if expression_attribute_names is not None:
    update_item_request["ExpressionAttributeNames"] = expression_attribute_names

  return {"Update": update_item_request}


def _build_transact_delete_item(
  table_name: str,
  key: dict[str, typing.Any],
  *,
  condition_expression: str | None = None,
  expression_attribute_names: dict[str, str] | None = None,
  expression_attribute_values: dict[str, typing.Any] | None = None,
) -> dict[str, typing.Any]:
  """Build one ``Delete`` entry for ``TransactWriteItems``."""

  delete_item_request: dict[str, typing.Any] = {
    "TableName": table_name,
    "Key": _serialize_item(key),
  }

  if condition_expression is not None:
    delete_item_request["ConditionExpression"] = condition_expression

  if expression_attribute_names is not None:
    delete_item_request["ExpressionAttributeNames"] = expression_attribute_names

  if expression_attribute_values is not None:
    delete_item_request["ExpressionAttributeValues"] = {
      placeholder: _serialize_attribute_value(attribute_value)
      for placeholder, attribute_value in expression_attribute_values.items()
    }

  return {"Delete": delete_item_request}


@pytest.fixture
def dynamodb_simulator() -> creatify_as.dynamodb.simulator.DynamoDbSimulator:
  """Return a fresh in-memory DynamoDB store for AWA-shaped tests."""

  return creatify_as.dynamodb.simulator.DynamoDbSimulator(_TEST_DYNAMODB_SIMULATOR_CONFIG)


def test_get_item_or_none_returns_seeded_fleet_row(
  dynamodb_simulator: creatify_as.dynamodb.simulator.DynamoDbSimulator,
) -> None:
  fleet_item = {
    "id": "fleet-1",
    "name": "staging.fleet-1",
    "status": _FLEET_STATUS_AVAILABLE,
    "instances_remaining_count": 3,
    "service_region": "us-east-1",
    "last_updated_at_timestamp": "2026-08-05T10:00:00+00:00",
  }

  dynamodb_simulator.seed_item(_AWA_FLEETS_TABLE_NAME, fleet_item)

  loaded_fleet_item = dynamodb_simulator.get_item_or_none(
    _AWA_FLEETS_TABLE_NAME,
    {"id": "fleet-1"})

  assert loaded_fleet_item == fleet_item


def test_query_collect_items_filters_by_gsi_name(
  dynamodb_simulator: creatify_as.dynamodb.simulator.DynamoDbSimulator,
) -> None:
  dynamodb_simulator.seed_item(
    _AWA_FLEETS_TABLE_NAME,
    {
      "id": "fleet-1",
      "name": "staging.fleet-1",
      "status": _FLEET_STATUS_AVAILABLE,
      "instances_remaining_count": 3,
      "service_region": "us-east-1",
      "last_updated_at_timestamp": "2026-08-05T10:00:00+00:00",
    })
  dynamodb_simulator.seed_item(
    _AWA_FLEETS_TABLE_NAME,
    {
      "id": "fleet-2",
      "name": "staging.fleet-2",
      "status": _FLEET_STATUS_AVAILABLE,
      "instances_remaining_count": 1,
      "service_region": "us-east-1",
      "last_updated_at_timestamp": "2026-08-05T10:01:00+00:00",
    })

  matching_fleet_records = dynamodb_simulator.query_collect_items(
    _AWA_FLEETS_TABLE_NAME,
    IndexName=_GSI_NAME,
    KeyConditionExpression=boto3.dynamodb.conditions.Key("name").eq("staging.fleet-1"),
  )

  assert len(matching_fleet_records) == 1
  assert matching_fleet_records[0]["id"] == "fleet-1"


def test_transact_put_association_and_decrement_fleet_remaining_count(
  dynamodb_simulator: creatify_as.dynamodb.simulator.DynamoDbSimulator,
) -> None:
  dynamodb_simulator.seed_item(
    _AWA_FLEETS_TABLE_NAME,
    {
      "id": "fleet-1",
      "name": "staging.fleet-1",
      "status": _FLEET_STATUS_AVAILABLE,
      "instances_remaining_count": 2,
      "service_region": "us-east-1",
      "last_updated_at_timestamp": "2026-08-05T10:00:00+00:00",
    })

  association_item = {
    "id": "association-1",
    "fleet_id": "fleet-1",
    "stack_id": "stack-1",
    "student_scti_mapping_id": "mapping-1",
    "user_id": "user-1",
    "stack_name": "staging.stack-1",
    "created_at_timestamp": "2026-08-05T10:05:00+00:00",
  }
  association_queue_item = {
    "student_association_id": "association-1",
    "fleet_id": "fleet-1",
    "stack_id": "stack-1",
    "student_scti_mapping_id": "mapping-1",
    "created_at_timestamp": "2026-08-05T10:05:00+00:00",
    "expires_at_timestamp": "2026-08-05T11:05:00+00:00",
  }
  transact_items = [
    _build_transact_put_item(
      _AWA_ASSOCIATIONS_STUDENT_TABLE_NAME,
      association_item,
      condition_expression="attribute_not_exists(id)",
    ),
    _build_transact_put_item(
      _AWA_ASSOCIATIONS_STUDENT_QUEUE_TABLE_NAME,
      association_queue_item,
      condition_expression="attribute_not_exists(student_association_id)",
    ),
    _build_transact_update_item(
      _AWA_FLEETS_TABLE_NAME,
      {"id": "fleet-1"},
      update_expression=(
        "SET instances_remaining_count = instances_remaining_count - :decrement, "
        "#status = :allocated_status, "
        "last_updated_at_timestamp = :last_updated_at_timestamp, "
        "cleanup_after_timestamp = :cleanup_after_timestamp"
      ),
      condition_expression=(
        "instances_remaining_count > :zero AND #status = :available_status"
      ),
      expression_attribute_names={
        "#status": "status",
      },
      expression_attribute_values={
        ":decrement": 1,
        ":zero": 0,
        ":allocated_status": _FLEET_STATUS_ALLOCATED,
        ":available_status": _FLEET_STATUS_AVAILABLE,
        ":last_updated_at_timestamp": "2026-08-05T10:05:00+00:00",
        ":cleanup_after_timestamp": "2026-08-05T11:05:00+00:00",
      },
    ),
  ]

  dynamodb_simulator.transact_write_items(transact_items)

  updated_fleet_item = dynamodb_simulator.get_item_or_none(
    _AWA_FLEETS_TABLE_NAME,
    {"id": "fleet-1"})
  stored_association_item = dynamodb_simulator.get_item_or_none(
    _AWA_ASSOCIATIONS_STUDENT_TABLE_NAME,
    {"id": "association-1"})

  assert updated_fleet_item is not None
  assert updated_fleet_item["instances_remaining_count"] == 1.0
  assert updated_fleet_item["status"] == _FLEET_STATUS_ALLOCATED
  assert stored_association_item == association_item


def test_transact_put_association_rejects_duplicate_association(
  dynamodb_simulator: creatify_as.dynamodb.simulator.DynamoDbSimulator,
) -> None:
  dynamodb_simulator.seed_item(
    _AWA_FLEETS_TABLE_NAME,
    {
      "id": "fleet-1",
      "name": "staging.fleet-1",
      "status": _FLEET_STATUS_AVAILABLE,
      "instances_remaining_count": 2,
      "service_region": "us-east-1",
      "last_updated_at_timestamp": "2026-08-05T10:00:00+00:00",
    })
  dynamodb_simulator.seed_item(
    _AWA_ASSOCIATIONS_STUDENT_TABLE_NAME,
    {
      "id": "association-1",
      "fleet_id": "fleet-1",
      "stack_id": "stack-1",
      "student_scti_mapping_id": "mapping-1",
      "user_id": "user-1",
      "stack_name": "staging.stack-1",
      "created_at_timestamp": "2026-08-05T10:05:00+00:00",
    })

  transact_items = [
    _build_transact_put_item(
      _AWA_ASSOCIATIONS_STUDENT_TABLE_NAME,
      {
        "id": "association-1",
        "fleet_id": "fleet-1",
        "stack_id": "stack-1",
        "student_scti_mapping_id": "mapping-1",
        "user_id": "user-1",
        "stack_name": "staging.stack-1",
        "created_at_timestamp": "2026-08-05T10:06:00+00:00",
      },
      condition_expression="attribute_not_exists(id)",
    ),
  ]

  with pytest.raises(botocore.exceptions.ClientError) as error_info:
    dynamodb_simulator.transact_write_items(transact_items)

  assert error_info.value.response["Error"]["Code"] == "TransactionCanceledException"


def test_transact_preallocate_stack_removes_queue_row_and_updates_stack(
  dynamodb_simulator: creatify_as.dynamodb.simulator.DynamoDbSimulator,
) -> None:
  dynamodb_simulator.seed_item(
    _AWA_STACKS_TABLE_NAME,
    {
      "id": "stack-1",
      "name": "staging.stack-1",
      "status": _STACK_STATUS_AVAILABLE,
      "school_id": "school-1",
      "service_region": "us-east-1",
      "last_updated_at_timestamp": "2026-08-05T10:00:00+00:00",
    })
  dynamodb_simulator.seed_item(
    _AWA_STACKS_QUEUE_TABLE_NAME,
    {
      "stack_id": "stack-1",
      "school_id": "school-1",
      "service_region": "us-east-1",
      "qualifier_key": "school-1,us-east-1",
      "created_at_timestamp": "2026-08-05T09:00:00+00:00",
    })

  transact_items = [
    _build_transact_delete_item(
      _AWA_STACKS_QUEUE_TABLE_NAME,
      {"stack_id": "stack-1"},
      condition_expression="attribute_exists(stack_id)",
    ),
    _build_transact_update_item(
      _AWA_STACKS_TABLE_NAME,
      {"id": "stack-1"},
      update_expression=(
        "SET #status = :preallocated_status, last_updated_at_timestamp = :last_updated_at_timestamp, "
        "preallocated_at_timestamp = :preallocated_at_timestamp, "
        "last_launched_at_timestamp = :last_launched_at_timestamp"
      ),
      condition_expression="#status = :available_status",
      expression_attribute_names={
        "#status": "status",
      },
      expression_attribute_values={
        ":preallocated_status": _STACK_STATUS_PREALLOCATED,
        ":available_status": _STACK_STATUS_AVAILABLE,
        ":last_updated_at_timestamp": "2026-08-05T10:05:00+00:00",
        ":preallocated_at_timestamp": "2026-08-05T10:05:00+00:00",
        ":last_launched_at_timestamp": "2026-08-05T10:05:00+00:00",
      },
    ),
  ]

  dynamodb_simulator.transact_write_items(transact_items)

  updated_stack_item = dynamodb_simulator.get_item_or_none(
    _AWA_STACKS_TABLE_NAME,
    {"id": "stack-1"})
  removed_queue_item = dynamodb_simulator.get_item_or_none(
    _AWA_STACKS_QUEUE_TABLE_NAME,
    {"stack_id": "stack-1"})

  assert updated_stack_item is not None
  assert updated_stack_item["status"] == _STACK_STATUS_PREALLOCATED
  assert removed_queue_item is None


def test_transact_reset_stack_to_available_removes_allocated_attributes(
  dynamodb_simulator: creatify_as.dynamodb.simulator.DynamoDbSimulator,
) -> None:
  dynamodb_simulator.seed_item(
    _AWA_STACKS_TABLE_NAME,
    {
      "id": "stack-1",
      "name": "staging.stack-1",
      "status": _STACK_STATUS_ALLOCATED,
      "school_id": "school-1",
      "service_region": "us-east-1",
      "last_updated_at_timestamp": "2026-08-05T10:00:00+00:00",
      "associated_school_id": "school-1",
      "associated_student_scti_mapping_id": "mapping-1",
      "association_expires_at_timestamp": "2026-08-05T11:00:00+00:00",
      "preallocated_at_timestamp": "2026-08-05T09:00:00+00:00",
      "cleanup_after_timestamp": "2026-08-05T11:00:00+00:00",
      "school_class_term_instance_id": "scti-1",
    })

  queue_item = {
    "stack_id": "stack-1",
    "school_id": "school-1",
    "service_region": "us-east-1",
    "qualifier_key": "school-1,us-east-1",
    "created_at_timestamp": "2026-08-05T10:05:00+00:00",
  }
  transact_items = [
    _build_transact_update_item(
      _AWA_STACKS_TABLE_NAME,
      {"id": "stack-1"},
      update_expression=(
        "SET #status = :available_status, last_updated_at_timestamp = :last_updated_at_timestamp "
        "REMOVE associated_school_id, associated_student_scti_mapping_id, "
        "association_expires_at_timestamp, preallocated_at_timestamp, cleanup_after_timestamp, "
        "school_class_term_instance_id"
      ),
      condition_expression="#status = :expected_status",
      expression_attribute_names={
        "#status": "status",
      },
      expression_attribute_values={
        ":available_status": _STACK_STATUS_AVAILABLE,
        ":expected_status": _STACK_STATUS_ALLOCATED,
        ":last_updated_at_timestamp": "2026-08-05T10:10:00+00:00",
      },
    ),
    _build_transact_put_item(
      _AWA_STACKS_QUEUE_TABLE_NAME,
      queue_item,
    ),
  ]

  dynamodb_simulator.transact_write_items(transact_items)

  updated_stack_item = dynamodb_simulator.get_item_or_none(
    _AWA_STACKS_TABLE_NAME,
    {"id": "stack-1"})
  stored_queue_item = dynamodb_simulator.get_item_or_none(
    _AWA_STACKS_QUEUE_TABLE_NAME,
    {"stack_id": "stack-1"})

  assert updated_stack_item is not None
  assert updated_stack_item["status"] == _STACK_STATUS_AVAILABLE
  assert "associated_school_id" not in updated_stack_item
  assert "cleanup_after_timestamp" not in updated_stack_item
  assert stored_queue_item == queue_item


def test_query_key_condition_compares_iso_timestamp_sort_keys_lexicographically() -> None:
  """GSI sort keys stored as ISO timestamps compare as strings when not numeric."""

  dynamodb_simulator = creatify_as.dynamodb.simulator.DynamoDbSimulator(
    creatify_as.dynamodb.simulator.DynamoDbSimulatorConfig(
      allowed_table_names=frozenset({_AWA_FLEETS_TABLE_NAME}),
      table_primary_key_attribute_names_by_table_name={
        _AWA_FLEETS_TABLE_NAME: ("id",),
      },
      gsi_key_attribute_names_by_index_name={
        "gsi_status_and_cleanup_after_timestamp": (
          "status",
          "cleanup_after_timestamp",
        ),
      },
    ))

  dynamodb_simulator.seed_item(
    _AWA_FLEETS_TABLE_NAME,
    {
      "id": "fleet-eligible",
      "status": _FLEET_STATUS_ALLOCATED,
      "cleanup_after_timestamp": "2020-01-01T00:00:00+00:00",
    })
  dynamodb_simulator.seed_item(
    _AWA_FLEETS_TABLE_NAME,
    {
      "id": "fleet-future",
      "status": _FLEET_STATUS_ALLOCATED,
      "cleanup_after_timestamp": "2099-01-01T00:00:00+00:00",
    })

  matching_items = dynamodb_simulator.query_collect_items(
    _AWA_FLEETS_TABLE_NAME,
    IndexName="gsi_status_and_cleanup_after_timestamp",
    KeyConditionExpression=(
      boto3.dynamodb.conditions.Key("status").eq(_FLEET_STATUS_ALLOCATED)
      & boto3.dynamodb.conditions.Key("cleanup_after_timestamp").lt(
        "2026-08-05T12:00:00+00:00")),
  )

  matching_fleet_ids = sorted(item["id"] for item in matching_items)

  assert matching_fleet_ids == ["fleet-eligible"]
