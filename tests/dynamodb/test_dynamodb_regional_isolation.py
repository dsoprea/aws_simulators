"""Regional partition behavior for DynamoDB simulator and fake client."""

import creatify_as.dynamodb.fake_client
import creatify_as.dynamodb.simulator


_TABLE_NAME = "awa_fleets"
_PRIMARY_KEY_NAMES = ("id",)


def test_get_item_or_none_scoped_to_client_region() -> None:
  config = creatify_as.dynamodb.simulator.DynamoDbSimulatorConfig(
    allowed_table_names=frozenset({_TABLE_NAME}),
    table_primary_key_attribute_names_by_table_name={_TABLE_NAME: _PRIMARY_KEY_NAMES},
    gsi_key_attribute_names_by_index_name={},
  )
  store = creatify_as.dynamodb.simulator.DynamoDbSimulator(config)
  row = {"id": 1, "name": "fleet-a"}

  store.seed_item(_TABLE_NAME, row, region_name="us-east-1")

  east_client = creatify_as.dynamodb.fake_client.FakeDynamoDbClient(store, region_name="us-east-1")
  west_client = creatify_as.dynamodb.fake_client.FakeDynamoDbClient(store, region_name="us-west-2")

  assert east_client.get_item_or_none(_TABLE_NAME, {"id": 1}) == row
  assert west_client.get_item_or_none(_TABLE_NAME, {"id": 1}) is None
