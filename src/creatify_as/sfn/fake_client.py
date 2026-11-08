"""Boto-shaped Step Functions client backed by the ASL simulator."""

import json
import typing

import creatify_as.sfn.simulator


class FakeSfnClientMeta:
  """Minimal client.meta shim for region-aware wrappers."""

  def __init__(self, region_name: str) -> None:
    self.region_name = region_name


class FakeSfnClient:
  """Delegates boto Step Functions calls to one StateMachineSimulator instance."""

  def __init__(
    self,
    simulator: creatify_as.sfn.simulator.StateMachineSimulator,
    *,
    region_name: str = "us-east-1",
  ) -> None:
    self._simulator = simulator
    self._region_name = region_name
    self.meta = FakeSfnClientMeta(region_name)
    self.start_execution_calls: list[dict[str, typing.Any]] = []
    self.describe_execution_calls: list[dict[str, typing.Any]] = []

  def start_execution(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    """Start one simulated execution from a JSON input string."""

    self.start_execution_calls.append(keyword_arguments)

    execution_input = json.loads(keyword_arguments["input"])

    return self._simulator.start_execution(
      state_machine_arn_or_name=keyword_arguments["stateMachineArn"],
      execution_name=keyword_arguments["name"],
      execution_input=execution_input,
    )

  def describe_execution(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    """Describe one simulated execution."""

    self.describe_execution_calls.append(keyword_arguments)

    return self._simulator.describe_execution(
      execution_arn=keyword_arguments["executionArn"],
    )
