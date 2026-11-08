"""Boto-shaped DynamoDB client surface backed by the DynamoDB simulator."""

import typing

import creatify_as.dynamodb.simulator


class FakeDynamoDbClientMeta:
  """Minimal client.meta shim for region-aware wrappers."""

  def __init__(self, region_name: str) -> None:
    self.region_name = region_name


class FakeDynamoDbClient:
  """Delegates high-level DynamoDB operations to one regional simulator partition."""

  def __init__(
    self,
    simulator: creatify_as.dynamodb.simulator.DynamoDbSimulator,
    *,
    region_name: str = "us-east-1",
  ) -> None:
    self._simulator = simulator
    self._region_name = region_name
    self.meta = FakeDynamoDbClientMeta(region_name)

  def get_item_or_none(
    self,
    table_name: str,
    key: dict[str, typing.Any],
  ) -> dict[str, typing.Any] | None:
    return self._simulator.get_item_or_none(
      table_name,
      key,
      region_name=self._region_name,
    )

  def put_item(
    self,
    table_name: str,
    item: dict[str, typing.Any],
    **put_item_keyword_arguments: typing.Any,
  ) -> None:
    self._simulator.put_item(
      table_name,
      item,
      region_name=self._region_name,
      condition_expression=put_item_keyword_arguments.get("ConditionExpression"),
      expression_attribute_names=put_item_keyword_arguments.get("ExpressionAttributeNames"),
      expression_attribute_values=put_item_keyword_arguments.get("ExpressionAttributeValues"),
    )

  def update_item(
    self,
    table_name: str,
    key: dict[str, typing.Any],
    **update_item_keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any]:
    return self._simulator.update_item(
      table_name,
      key,
      region_name=self._region_name,
      update_expression=update_item_keyword_arguments["UpdateExpression"],
      condition_expression=update_item_keyword_arguments.get("ConditionExpression"),
      expression_attribute_names=update_item_keyword_arguments.get("ExpressionAttributeNames"),
      expression_attribute_values=update_item_keyword_arguments["ExpressionAttributeValues"],
    )

  def delete_item(
    self,
    table_name: str,
    key: dict[str, typing.Any],
    **delete_item_keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any]:
    return self._simulator.delete_item(
      table_name,
      key,
      region_name=self._region_name,
      condition_expression=delete_item_keyword_arguments.get("ConditionExpression"),
      expression_attribute_names=delete_item_keyword_arguments.get("ExpressionAttributeNames"),
      expression_attribute_values=delete_item_keyword_arguments.get("ExpressionAttributeValues"),
    )

  def query_collect_items(
    self,
    table_name: str,
    **query_keyword_arguments: typing.Any,
  ) -> list[dict[str, typing.Any]]:
    return self._simulator.query_collect_items(
      table_name,
      region_name=self._region_name,
      **query_keyword_arguments,
    )

  def query_first_item_or_none(
    self,
    table_name: str,
    **query_keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any] | None:
    return self._simulator.query_first_item_or_none(
      table_name,
      region_name=self._region_name,
      **query_keyword_arguments,
    )

  def scan_collect_items(
    self,
    table_name: str,
    **scan_keyword_arguments: typing.Any,
  ) -> list[dict[str, typing.Any]]:
    return self._simulator.scan_collect_items(
      table_name,
      region_name=self._region_name,
      **scan_keyword_arguments,
    )

  def scan_all_items(self, table_name: str) -> list[dict[str, typing.Any]]:
    return self._simulator.scan_all_items(table_name, region_name=self._region_name)

  def transact_write_items(self, transact_items: list[dict[str, typing.Any]]) -> None:
    self._simulator.transact_write_items(
      transact_items,
      region_name=self._region_name,
    )
