"""Unit tests for EC2 simulator semantics."""

import pytest
from botocore.exceptions import ClientError

import creatify_as.ec2.simulator


@pytest.fixture
def recorded_sleep_delays() -> list[float]:
  return []


@pytest.fixture
def simulator(
  recorded_sleep_delays: list[float],
) -> creatify_as.ec2.simulator.Ec2Simulator:
  def record_sleep(delay_seconds: float) -> None:
    recorded_sleep_delays.append(delay_seconds)

  return creatify_as.ec2.simulator.Ec2Simulator(
    config=creatify_as.ec2.simulator.Ec2SimulatorConfig(be_quick=True),
    sleep_fn=record_sleep,
    time_fn=lambda: 0.0,
  )


def test_start_from_stopped_transitions_pending_then_running(
  simulator: creatify_as.ec2.simulator.Ec2Simulator,
  recorded_sleep_delays: list[float],
) -> None:
  instance_id = simulator.seed_instance(
    instance_type="t3.medium",
    image_id="ami-linux",
    platform_type="linux",
    state_name="stopped",
  )

  simulator.start_instances(instance_ids=[instance_id])

  assert simulator.gather_instance_state_name(instance_id) == "pending"

  simulator.wait_for_instance_state(instance_id, "running", timeout_seconds=1.0)

  assert simulator.gather_instance_state_name(instance_id) == "running"
  assert 10.0 in recorded_sleep_delays


def test_stop_from_running_transitions_stopping_then_stopped(
  simulator: creatify_as.ec2.simulator.Ec2Simulator,
  recorded_sleep_delays: list[float],
) -> None:
  instance_id = simulator.seed_instance(
    instance_type="g4dn.xlarge",
    image_id="ami-windows",
    platform_type="windows",
    state_name="running",
  )

  simulator.stop_instances(instance_ids=[instance_id])

  assert simulator.gather_instance_state_name(instance_id) == "stopping"

  simulator.wait_for_instance_state(instance_id, "stopped", timeout_seconds=1.0)

  assert simulator.gather_instance_state_name(instance_id) == "stopped"
  assert 10.0 in recorded_sleep_delays


def test_terminate_from_stopped_uses_platform_delay(
  recorded_sleep_delays: list[float],
) -> None:
  linux_simulator = creatify_as.ec2.simulator.Ec2Simulator(
    config=creatify_as.ec2.simulator.Ec2SimulatorConfig(be_quick=False),
    sleep_fn=lambda delay: recorded_sleep_delays.append(delay),
  )

  linux_instance_id = linux_simulator.seed_instance(
    instance_type="t3.medium",
    image_id="ami-linux",
    platform_type="linux",
    state_name="stopped",
  )

  linux_simulator.terminate_instances(instance_ids=[linux_instance_id])

  assert linux_simulator.gather_instance_state_name(linux_instance_id) == "shutting-down"
  linux_simulator.wait_for_instance_state(
    linux_instance_id,
    "terminated",
    timeout_seconds=1.0,
    poll_interval_seconds=0.0,
  )

  assert 60.0 in recorded_sleep_delays

  recorded_sleep_delays.clear()

  windows_simulator = creatify_as.ec2.simulator.Ec2Simulator(
    config=creatify_as.ec2.simulator.Ec2SimulatorConfig(be_quick=False),
    sleep_fn=lambda delay: recorded_sleep_delays.append(delay),
  )

  windows_instance_id = windows_simulator.seed_instance(
    instance_type="g4dn.xlarge",
    image_id="ami-windows",
    platform_type="windows",
    state_name="stopped",
  )

  windows_simulator.terminate_instances(instance_ids=[windows_instance_id])
  windows_simulator.wait_for_instance_state(
    windows_instance_id,
    "terminated",
    timeout_seconds=1.0,
    poll_interval_seconds=0.0,
  )

  assert 180.0 in recorded_sleep_delays


def test_windows_start_delay_when_not_be_quick(
  recorded_sleep_delays: list[float],
) -> None:
  simulator = creatify_as.ec2.simulator.Ec2Simulator(
    config=creatify_as.ec2.simulator.Ec2SimulatorConfig(be_quick=False),
    sleep_fn=lambda delay: recorded_sleep_delays.append(delay),
  )

  instance_id = simulator.seed_instance(
    instance_type="g4dn.xlarge",
    image_id="ami-windows",
    platform_type="windows",
    state_name="stopped",
  )

  simulator.start_instances(instance_ids=[instance_id])
  simulator.wait_for_instance_state(instance_id, "running", timeout_seconds=1.0, poll_interval_seconds=0.0)

  assert 240.0 in recorded_sleep_delays


def test_create_tags_merges_on_describe(
  simulator: creatify_as.ec2.simulator.Ec2Simulator,
) -> None:
  instance_id = simulator.seed_instance(
    instance_type="t3.medium",
    image_id="ami-linux",
    tags=[{"Key": "SCHOOL_ID", "Value": "school-1"}],
  )

  simulator.create_tags(
    resource_ids=[instance_id],
    tags=[
      {"Key": "SCHOOL_ID", "Value": "school-2"},
      {"Key": "INSTANCE_TYPE", "Value": "student"},
    ],
  )

  describe_response = simulator.describe_instances(instance_ids=[instance_id])
  instance_object = describe_response["Reservations"][0]["Instances"][0]

  assert instance_object["Tags"] == [
    {"Key": "SCHOOL_ID", "Value": "school-2"},
    {"Key": "INSTANCE_TYPE", "Value": "student"},
  ]


def test_describe_instances_filter_instance_state_and_tag(
  simulator: creatify_as.ec2.simulator.Ec2Simulator,
) -> None:
  running_id = simulator.seed_instance(
    instance_type="t3.medium",
    image_id="ami-linux",
    state_name="running",
    tags=[{"Key": "INSTANCE_TYPE", "Value": "student"}],
  )
  simulator.seed_instance(
    instance_type="t3.medium",
    image_id="ami-linux",
    state_name="stopped",
    tags=[{"Key": "INSTANCE_TYPE", "Value": "student"}],
  )

  filtered_response = simulator.describe_instances(
    filters=[
      {"Name": "instance-state-name", "Values": ["running"]},
      {"Name": "tag:INSTANCE_TYPE", "Values": ["student"]},
    ],
  )

  filtered_instance_ids = [
    row["InstanceId"]
    for reservation in filtered_response["Reservations"]
    for row in reservation["Instances"]
  ]

  assert filtered_instance_ids == [running_id]


def test_start_when_already_running_is_idempotent(
  simulator: creatify_as.ec2.simulator.Ec2Simulator,
  recorded_sleep_delays: list[float],
) -> None:
  instance_id = simulator.seed_instance(
    instance_type="t3.medium",
    image_id="ami-linux",
    state_name="running",
  )

  simulator.start_instances(instance_ids=[instance_id])

  assert simulator.gather_instance_state_name(instance_id) == "running"
  assert recorded_sleep_delays == []


def test_terminate_from_pending_raises_incorrect_instance_state(
  simulator: creatify_as.ec2.simulator.Ec2Simulator,
) -> None:
  instance_id = simulator.seed_instance(
    instance_type="t3.medium",
    image_id="ami-linux",
    state_name="pending",
  )

  with pytest.raises(ClientError) as error_info:
    simulator.terminate_instances(instance_ids=[instance_id])

  assert error_info.value.response["Error"]["Code"] == "IncorrectInstanceState"


def test_run_instances_returns_pending_then_running(
  simulator: creatify_as.ec2.simulator.Ec2Simulator,
) -> None:
  run_response = simulator.run_instances(
    ImageId="ami-abc",
    InstanceType="t3.medium",
    MinCount=1,
    MaxCount=1,
    PlatformType="linux",
  )

  instance_object = run_response["Instances"][0]

  assert instance_object["State"]["Name"] == "pending"

  instance_id = instance_object["InstanceId"]

  simulator.wait_for_instance_state(instance_id, "running", timeout_seconds=1.0)

  assert simulator.gather_instance_state_name(instance_id) == "running"


def test_reset_clears_instances(
  simulator: creatify_as.ec2.simulator.Ec2Simulator,
) -> None:
  instance_id = simulator.seed_instance(
    instance_type="t3.medium",
    image_id="ami-linux",
  )

  simulator.reset()

  describe_response = simulator.describe_instances(instance_ids=[instance_id])

  assert describe_response["Reservations"] == []
