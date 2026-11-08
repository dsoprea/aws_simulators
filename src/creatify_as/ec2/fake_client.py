"""Boto-shaped EC2 client backed by the EC2 simulator."""

import typing

import creatify_as.ec2.simulator


class FakeEc2ClientMeta:
  """Minimal client.meta shim for region-aware wrappers."""

  def __init__(self, region_name: str) -> None:
    self.region_name = region_name


class FakeEc2Client:
  """Delegates boto EC2 calls to one Ec2Simulator instance."""

  def __init__(
    self,
    simulator: creatify_as.ec2.simulator.Ec2Simulator,
    *,
    region_name: str | None = None,
  ) -> None:
    self._simulator = simulator
    self.meta = FakeEc2ClientMeta(
      region_name or simulator._config.region_name,
    )
    self.describe_instances_calls: list[dict[str, typing.Any]] = []
    self.start_instances_calls: list[dict[str, typing.Any]] = []
    self.stop_instances_calls: list[dict[str, typing.Any]] = []
    self.terminate_instances_calls: list[dict[str, typing.Any]] = []
    self.create_tags_calls: list[dict[str, typing.Any]] = []
    self.run_instances_calls: list[dict[str, typing.Any]] = []

  def describe_instances(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.describe_instances_calls.append(keyword_arguments)

    return self._simulator.describe_instances(
      instance_ids=keyword_arguments.get("InstanceIds"),
      filters=keyword_arguments.get("Filters"),
    )

  def start_instances(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.start_instances_calls.append(keyword_arguments)

    return self._simulator.start_instances(
      instance_ids=keyword_arguments["InstanceIds"],
    )

  def stop_instances(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.stop_instances_calls.append(keyword_arguments)

    return self._simulator.stop_instances(
      instance_ids=keyword_arguments["InstanceIds"],
    )

  def terminate_instances(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.terminate_instances_calls.append(keyword_arguments)

    return self._simulator.terminate_instances(
      instance_ids=keyword_arguments["InstanceIds"],
    )

  def create_tags(self, **keyword_arguments: typing.Any) -> None:
    self.create_tags_calls.append(keyword_arguments)

    self._simulator.create_tags(
      resource_ids=keyword_arguments["Resources"],
      tags=keyword_arguments["Tags"],
    )

  def run_instances(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    self.run_instances_calls.append(keyword_arguments)

    return self._simulator.run_instances(**keyword_arguments)
