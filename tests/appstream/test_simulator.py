"""Unit tests for AppStream simulator semantics."""

import pytest
from botocore.exceptions import ClientError

import creatify_as.appstream.simulator


@pytest.fixture
def simulator() -> creatify_as.appstream.simulator.AppStreamSimulator:
  return creatify_as.appstream.simulator.AppStreamSimulator(
    config=creatify_as.appstream.simulator.AppStreamSimulatorConfig(be_quick=True),
    sleep_fn=lambda seconds: None,
  )


def test_associate_fleet_allows_one_stack_per_fleet(
  simulator: creatify_as.appstream.simulator.AppStreamSimulator,
) -> None:
  simulator.seed_fleet(fleet_name="fleet-a")
  simulator.seed_stack(stack_name="stack-one")
  simulator.seed_stack(stack_name="stack-two")

  simulator.associate_fleet(fleet_name="fleet-a", stack_name="stack-one")

  with pytest.raises(ClientError) as error_info:
    simulator.associate_fleet(fleet_name="fleet-a", stack_name="stack-two")

  assert error_info.value.response["Error"]["Code"] == "LimitExceededException"


def test_associate_fleet_is_idempotent_for_same_stack(
  simulator: creatify_as.appstream.simulator.AppStreamSimulator,
) -> None:
  simulator.seed_fleet(fleet_name="fleet-a")
  simulator.seed_stack(stack_name="stack-one")

  simulator.associate_fleet(fleet_name="fleet-a", stack_name="stack-one")
  simulator.associate_fleet(fleet_name="fleet-a", stack_name="stack-one")

  associated_stack_names = simulator.list_associated_stacks(fleet_name="fleet-a")["Names"]

  assert associated_stack_names == ["stack-one"]


def test_create_streaming_url_returns_distinct_urls_per_user(
  simulator: creatify_as.appstream.simulator.AppStreamSimulator,
) -> None:
  simulator.seed_fleet(fleet_name="fleet-a", desired_instances=2)
  simulator.seed_stack(stack_name="stack-one")
  simulator.associate_fleet(fleet_name="fleet-a", stack_name="stack-one")

  first_url_response = simulator.create_streaming_url(
    stack_name="stack-one",
    fleet_name="fleet-a",
    user_id="student-one@example.com",
    application_id="app-id",
    validity=3600,
    session_context="ctx",
  )
  second_url_response = simulator.create_streaming_url(
    stack_name="stack-one",
    fleet_name="fleet-a",
    user_id="student-two@example.com",
    application_id="app-id",
    validity=3600,
    session_context="ctx",
  )

  assert first_url_response["StreamingURL"] != second_url_response["StreamingURL"]
  assert "student-one@example.com" in first_url_response["StreamingURL"]
  assert "student-two@example.com" in second_url_response["StreamingURL"]


def test_fleet_start_transitions_to_running_after_delay(
  simulator: creatify_as.appstream.simulator.AppStreamSimulator,
) -> None:
  create_fleet_response = simulator.create_fleet(
    Name="new-fleet",
    DisplayName="New fleet",
    ComputeCapacity={"DesiredInstances": 2},
  )

  assert create_fleet_response["Fleet"]["State"] == "STOPPED"

  fleet_objects_before_start = simulator.describe_fleets(names=["new-fleet"])["Fleets"]

  assert fleet_objects_before_start[0]["State"] == "STOPPED"

  simulator.start_fleet(name="new-fleet")

  simulator.wait_for_fleet_state(fleet_name="new-fleet", state="RUNNING")

  fleet_objects_after_delay = simulator.describe_fleets(names=["new-fleet"])["Fleets"]

  assert fleet_objects_after_delay[0]["State"] == "RUNNING"


def test_stack_create_transitions_to_available_after_delay(
  simulator: creatify_as.appstream.simulator.AppStreamSimulator,
) -> None:
  create_stack_response = simulator.create_stack(
    Name="new-stack",
    DisplayName="New stack",
  )

  assert create_stack_response["Stack"]["State"] == "PENDING"

  simulator.wait_for_stack_state(stack_name="new-stack", state="AVAILABLE")

  stack_objects = simulator.describe_stacks(names=["new-stack"])["Stacks"]

  assert stack_objects[0]["State"] == "AVAILABLE"


def test_stack_available_delay_seconds_when_not_be_quick() -> None:
  recorded_sleep_seconds: list[float] = []

  def record_sleep(seconds: float) -> None:
    recorded_sleep_seconds.append(seconds)

  simulator = creatify_as.appstream.simulator.AppStreamSimulator(
    config=creatify_as.appstream.simulator.AppStreamSimulatorConfig(be_quick=False),
    sleep_fn=record_sleep,
  )
  simulator.create_stack(Name="timed-stack")
  simulator.wait_for_stack_state(stack_name="timed-stack", state="AVAILABLE")

  assert recorded_sleep_seconds[0] == 120.0


def test_stack_available_delay_seconds_when_be_quick() -> None:
  recorded_sleep_seconds: list[float] = []

  def record_sleep(seconds: float) -> None:
    recorded_sleep_seconds.append(seconds)

  simulator = creatify_as.appstream.simulator.AppStreamSimulator(
    config=creatify_as.appstream.simulator.AppStreamSimulatorConfig(be_quick=True),
    sleep_fn=record_sleep,
  )
  simulator.create_stack(Name="quick-stack")
  simulator.wait_for_stack_state(stack_name="quick-stack", state="AVAILABLE")

  assert recorded_sleep_seconds[0] == 10.0


def test_create_streaming_url_decrements_available_capacity(
  simulator: creatify_as.appstream.simulator.AppStreamSimulator,
) -> None:
  simulator.seed_fleet(fleet_name="capacity-fleet", desired_instances=1)
  simulator.seed_stack(stack_name="stack-one")
  simulator.associate_fleet(fleet_name="capacity-fleet", stack_name="stack-one")

  fleet_before = simulator.describe_fleets(names=["capacity-fleet"])["Fleets"][0]

  assert fleet_before["ComputeCapacityStatus"]["Available"] == 1

  simulator.create_streaming_url(
    stack_name="stack-one",
    fleet_name="capacity-fleet",
    user_id="student-one@example.com",
    application_id="app-id",
    validity=3600,
    session_context="ctx",
  )

  fleet_after = simulator.describe_fleets(names=["capacity-fleet"])["Fleets"][0]

  assert fleet_after["ComputeCapacityStatus"]["Available"] == 0
  assert fleet_after["ComputeCapacityStatus"]["InUse"] == 1


def test_expire_session_marks_session_inactive(
  simulator: creatify_as.appstream.simulator.AppStreamSimulator,
) -> None:
  simulator.seed_fleet(fleet_name="fleet-a")
  simulator.seed_stack(stack_name="stack-one")
  simulator.associate_fleet(fleet_name="fleet-a", stack_name="stack-one")

  streaming_url_response = simulator.create_streaming_url(
    stack_name="stack-one",
    fleet_name="fleet-a",
    user_id="student-one@example.com",
    application_id="app-id",
    validity=3600,
    session_context="ctx",
  )
  assert "StreamingURL" in streaming_url_response

  sessions_before_expire = simulator.describe_sessions(
    stack_name="stack-one",
    fleet_name="fleet-a",
    user_id="student-one@example.com",
  )["Sessions"]

  assert len(sessions_before_expire) == 1
  session_id = sessions_before_expire[0]["Id"]

  simulator.expire_session(session_id=session_id)

  sessions_after_expire = simulator.describe_sessions(
    stack_name="stack-one",
    fleet_name="fleet-a",
    user_id="student-one@example.com",
  )["Sessions"]

  assert sessions_after_expire == []

  fleet_after_expire = simulator.describe_fleets(names=["fleet-a"])["Fleets"][0]

  assert fleet_after_expire["ComputeCapacityStatus"]["Available"] == 1
  assert fleet_after_expire["ComputeCapacityStatus"]["InUse"] == 0


def test_create_imported_image_transitions_to_available(
  simulator: creatify_as.appstream.simulator.AppStreamSimulator,
) -> None:
  create_response = simulator.create_imported_image(
    Name="imported-image",
    SourceAmiId="ami-12345678",
    IamRoleArn="arn:aws:iam::123456789012:role/AppStream",
    Tags={"DEPLOYMENT_ENVIRONMENT": "staging"},
  )

  assert create_response["ImportedImage"]["State"] == "IMPORTING"

  simulator.wait_for_image_state(image_name="imported-image", state="AVAILABLE")

  images_available = simulator.describe_images(names=["imported-image"])["Images"]

  assert images_available[0]["State"] == "AVAILABLE"


def test_create_imported_image_dry_run_raises() -> None:
  simulator = creatify_as.appstream.simulator.AppStreamSimulator(
    config=creatify_as.appstream.simulator.AppStreamSimulatorConfig(be_quick=True),
    sleep_fn=lambda seconds: None,
  )

  with pytest.raises(ClientError) as error_info:
    simulator.create_imported_image(
      Name="dry-run-image",
      SourceAmiId="ami-12345678",
      IamRoleArn="arn:aws:iam::123456789012:role/AppStream",
      DryRun=True,
    )

  assert error_info.value.response["Error"]["Code"] == "DryRunOperationException"
