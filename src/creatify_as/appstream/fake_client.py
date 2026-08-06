"""Boto-shaped AppStream client backed by the AppStream simulator."""

import typing

import creatify_as.appstream.simulator


class FakeAppStreamClientMeta:
  """Minimal client.meta shim for region-aware wrappers."""

  def __init__(self, region_name: str) -> None:
    self.region_name = region_name


class FakeAppStreamClient:
  """Delegates boto AppStream calls to one AppStreamSimulator instance."""

  def __init__(
    self,
    simulator: creatify_as.appstream.simulator.AppStreamSimulator,
    *,
    region_name: str | None = None,
  ) -> None:
    self._simulator = simulator
    self.meta = FakeAppStreamClientMeta(
      region_name or simulator._config.region_name,
    )
    self.associate_fleet_calls: list[dict[str, typing.Any]] = []
    self.disassociate_fleet_calls: list[dict[str, typing.Any]] = []
    self.create_streaming_url_calls: list[dict[str, typing.Any]] = []
    self.describe_fleets_calls: list[dict[str, typing.Any]] = []
    self.describe_stacks_calls: list[dict[str, typing.Any]] = []
    self.describe_images_calls: list[dict[str, typing.Any]] = []
    self.describe_sessions_calls: list[dict[str, typing.Any]] = []
    self.create_fleet_calls: list[dict[str, typing.Any]] = []
    self.start_fleet_calls: list[dict[str, typing.Any]] = []
    self.stop_fleet_calls: list[dict[str, typing.Any]] = []
    self.tag_resource_calls: list[dict[str, typing.Any]] = []
    self.untag_resource_calls: list[dict[str, typing.Any]] = []
    self.update_fleet_calls: list[dict[str, typing.Any]] = []

  def associate_fleet(self, **keyword_arguments: typing.Any) -> None:
    self.associate_fleet_calls.append(keyword_arguments)
    self._simulator.associate_fleet(
      fleet_name=keyword_arguments["FleetName"],
      stack_name=keyword_arguments["StackName"],
    )

  def disassociate_fleet(self, **keyword_arguments: typing.Any) -> None:
    self.disassociate_fleet_calls.append(keyword_arguments)
    self._simulator.disassociate_fleet(
      fleet_name=keyword_arguments["FleetName"],
      stack_name=keyword_arguments["StackName"],
    )

  def list_associated_stacks(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    return self._simulator.list_associated_stacks(
      fleet_name=keyword_arguments["FleetName"],
      next_token=keyword_arguments.get("NextToken"),
    )

  def list_associated_fleets(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    return self._simulator.list_associated_fleets(
      stack_name=keyword_arguments["StackName"],
      next_token=keyword_arguments.get("NextToken"),
    )

  def create_streaming_url(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.create_streaming_url_calls.append(keyword_arguments)

    return self._simulator.create_streaming_url(
      stack_name=keyword_arguments["StackName"],
      fleet_name=keyword_arguments["FleetName"],
      user_id=keyword_arguments["UserId"],
      application_id=keyword_arguments["ApplicationId"],
      validity=keyword_arguments["Validity"],
      session_context=keyword_arguments.get("SessionContext", ""),
    )

  def describe_fleets(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.describe_fleets_calls.append(keyword_arguments)

    return self._simulator.describe_fleets(
      names=keyword_arguments.get("Names"),
      next_token=keyword_arguments.get("NextToken"),
    )

  def describe_stacks(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.describe_stacks_calls.append(keyword_arguments)

    return self._simulator.describe_stacks(
      names=keyword_arguments.get("Names"),
      next_token=keyword_arguments.get("NextToken"),
    )

  def create_fleet(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.create_fleet_calls.append(keyword_arguments)

    return self._simulator.create_fleet(**keyword_arguments)

  def start_fleet(self, **keyword_arguments: typing.Any) -> None:
    self.start_fleet_calls.append(keyword_arguments)
    self._simulator.start_fleet(name=keyword_arguments["Name"])

  def stop_fleet(self, **keyword_arguments: typing.Any) -> None:
    self.stop_fleet_calls.append(keyword_arguments)
    self._simulator.stop_fleet(name=keyword_arguments["Name"])

  def delete_fleet(self, **keyword_arguments: typing.Any) -> None:
    self._simulator.delete_fleet(name=keyword_arguments["Name"])

  def create_stack(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    return self._simulator.create_stack(**keyword_arguments)

  def delete_stack(self, **keyword_arguments: typing.Any) -> None:
    self._simulator.delete_stack(name=keyword_arguments["Name"])

  def describe_sessions(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.describe_sessions_calls.append(keyword_arguments)

    return self._simulator.describe_sessions(
      stack_name=keyword_arguments["StackName"],
      fleet_name=keyword_arguments["FleetName"],
      user_id=keyword_arguments.get("UserId"),
      authentication_type=keyword_arguments.get("AuthenticationType"),
      next_token=keyword_arguments.get("NextToken"),
    )

  def expire_session(self, **keyword_arguments: typing.Any) -> None:
    self._simulator.expire_session(session_id=keyword_arguments["SessionId"])

  def tag_resource(self, **keyword_arguments: typing.Any) -> None:
    self.tag_resource_calls.append(keyword_arguments)
    self._simulator.tag_resource(
      resource_arn=keyword_arguments["ResourceArn"],
      tags=keyword_arguments["Tags"],
    )

  def untag_resource(self, **keyword_arguments: typing.Any) -> None:
    self.untag_resource_calls.append(keyword_arguments)
    self._simulator.untag_resource(
      resource_arn=keyword_arguments["ResourceArn"],
      tag_keys=keyword_arguments["TagKeys"],
    )

  def list_tags_for_resource(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    return self._simulator.list_tags_for_resource(
      resource_arn=keyword_arguments["ResourceArn"],
    )

  def describe_images(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.describe_images_calls.append(keyword_arguments)

    return self._simulator.describe_images(
      names=keyword_arguments.get("Names"),
      arns=keyword_arguments.get("Arns"),
      next_token=keyword_arguments.get("NextToken"),
      max_results=keyword_arguments.get("MaxResults"),
    )

  def update_fleet(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.update_fleet_calls.append(keyword_arguments)

    return self._simulator.update_fleet(**keyword_arguments)
