"""Unit tests for AppStream simulator semantics."""

import pytest
from botocore.exceptions import ClientError

import creatify_as.appstream.simulator


@pytest.fixture
def simulator() -> creatify_as.appstream.simulator.AppStreamSimulator:
  return creatify_as.appstream.simulator.AppStreamSimulator(
    config=creatify_as.appstream.simulator.AppStreamSimulatorConfig(
      fleet_state_transition_delay_seconds=0.1,
      stack_state_transition_delay_seconds=0.02,
    ),
    time_fn=lambda: 0.0,
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
  simulator.seed_fleet(fleet_name="fleet-a")
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

  fleet_objects_after_start = simulator.describe_fleets(names=["new-fleet"])["Fleets"]

  assert fleet_objects_after_start[0]["State"] == "STARTING"

  simulator.advance_time(0.1)

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

  simulator.advance_time(0.02)

  stack_objects = simulator.describe_stacks(names=["new-stack"])["Stacks"]

  assert stack_objects[0]["State"] == "AVAILABLE"


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
