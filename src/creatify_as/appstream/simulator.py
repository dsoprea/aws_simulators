"""In-memory AppStream state machine for tests without live AWS calls."""

import dataclasses
import time
import typing

from botocore.exceptions import ClientError


@dataclasses.dataclass
class AppStreamSimulatorConfig:
  """Timing and identity defaults for one simulator instance."""

  region_name: str = "us-east-1"
  account_id: str = "123456789012"
  fleet_state_transition_delay_seconds: float = 0.05
  stack_state_transition_delay_seconds: float = 0.01
  streaming_url_validity_seconds: int = 3600
  describe_list_page_size: int | None = None


@dataclasses.dataclass
class _ScheduledTransition:
  """One fleet or stack state change scheduled on the virtual clock."""

  fire_at: float
  resource_kind: str
  resource_name: str
  target_state: str


@dataclasses.dataclass
class _FleetRecord:
  """Mutable fleet row returned by DescribeFleets."""

  name: str
  arn: str
  display_name: str
  state: str
  desired_instances: int
  image_name: str
  instance_type: str
  max_user_duration_seconds: int | None = None


@dataclasses.dataclass
class _StackRecord:
  """Mutable stack row returned by DescribeStacks."""

  name: str
  arn: str
  display_name: str
  state: str


@dataclasses.dataclass
class _SessionRecord:
  """One active streaming session."""

  session_id: str
  stack_name: str
  fleet_name: str
  user_id: str
  state: str


class AppStreamSimulator:
  """Models fleet, stack, association, session, and streaming URL AppStream semantics."""

  def __init__(
    self,
    *,
    config: AppStreamSimulatorConfig | None = None,
    time_fn: typing.Callable[[], float] | None = None,
    sleep_fn: typing.Callable[[float], None] | None = None,
  ) -> None:
    self._config = config or AppStreamSimulatorConfig()
    self._time_fn = time_fn or time.monotonic
    self._virtual_clock_seconds = 0.0
    self._use_virtual_clock = time_fn is not None or sleep_fn is not None

    if sleep_fn is not None:
      self._sleep_fn = sleep_fn
    elif self._use_virtual_clock:
      self._sleep_fn = self._advance_virtual_clock
    else:
      self._sleep_fn = time.sleep

    self._fleets: dict[str, _FleetRecord] = {}
    self._stacks: dict[str, _StackRecord] = {}
    self._fleet_to_stack_name: dict[str, str] = {}
    self._stack_to_fleet_names: dict[str, set[str]] = {}
    self._tags_by_arn: dict[str, dict[str, str]] = {}
    self._sessions: list[_SessionRecord] = []
    self._scheduled_transitions: list[_ScheduledTransition] = []
    self._streaming_url_counter = 0
    self._image_objects_by_name: dict[str, dict[str, typing.Any]] = {}
    self._image_objects_by_arn: dict[str, dict[str, typing.Any]] = {}

  def reset(self) -> None:
    """Clear all simulator state (use between tests)."""

    self._fleets.clear()
    self._stacks.clear()
    self._fleet_to_stack_name.clear()
    self._stack_to_fleet_names.clear()
    self._tags_by_arn.clear()
    self._sessions.clear()
    self._scheduled_transitions.clear()
    self._streaming_url_counter = 0
    self._image_objects_by_name.clear()
    self._image_objects_by_arn.clear()
    self._virtual_clock_seconds = 0.0

  def advance_time(self, seconds: float) -> None:
    """Advance the virtual clock and apply due state transitions."""

    self._virtual_clock_seconds = self._virtual_clock_seconds + seconds
    self._apply_due_transitions()

  def _now(self) -> float:
    if self._use_virtual_clock:
      return self._virtual_clock_seconds

    return self._time_fn()

  def _advance_virtual_clock(self, seconds: float) -> None:
    self.advance_time(seconds)

  def _apply_due_transitions(self) -> None:
    current_time = self._now()
    remaining_transitions: list[_ScheduledTransition] = []

    for scheduled_transition in sorted(self._scheduled_transitions, key=lambda row: row.fire_at):
      if scheduled_transition.fire_at > current_time:
        remaining_transitions.append(scheduled_transition)
        continue

      if scheduled_transition.resource_kind == "fleet":
        fleet_record = self._fleets.get(scheduled_transition.resource_name)

        if fleet_record is not None:
          fleet_record.state = scheduled_transition.target_state

      if scheduled_transition.resource_kind == "stack":
        stack_record = self._stacks.get(scheduled_transition.resource_name)

        if stack_record is not None:
          stack_record.state = scheduled_transition.target_state

    self._scheduled_transitions = remaining_transitions

  def _schedule_transition(
    self,
    *,
    resource_kind: str,
    resource_name: str,
    target_state: str,
    delay_seconds: float,
  ) -> None:
    self._scheduled_transitions.append(
      _ScheduledTransition(
        fire_at=self._now() + delay_seconds,
        resource_kind=resource_kind,
        resource_name=resource_name,
        target_state=target_state,
      ),
    )

  def _build_fleet_arn(self, fleet_name: str) -> str:
    return "arn:aws:appstream:{region}:{account}:fleet/{name}".format(
      region=self._config.region_name,
      account=self._config.account_id,
      name=fleet_name,
    )

  def _build_stack_arn(self, stack_name: str) -> str:
    return "arn:aws:appstream:{region}:{account}:stack/{name}".format(
      region=self._config.region_name,
      account=self._config.account_id,
      name=stack_name,
    )

  def _parse_list_offset_token(self, next_token: str | None) -> int:
    """Return the list offset encoded in *next_token*, or zero when absent."""

    if next_token is None:
      return 0

    if next_token.startswith("offset:"):
      return int(next_token.split(":", 1)[1])

    return int(next_token)

  def _build_offset_next_token(self, next_offset: int) -> str:
    """Return the opaque pagination token for *next_offset*."""

    return "offset:{offset}".format(offset=next_offset)

  def _build_paginated_object_list_response(
    self,
    *,
    collection_key: str,
    object_rows: list[dict[str, typing.Any]],
    next_token: str | None,
  ) -> dict[str, typing.Any]:
    """Return one page of *object_rows* using ``describe_list_page_size`` when configured."""

    page_size = self._config.describe_list_page_size

    if page_size is None:
      return {collection_key: object_rows}

    list_offset = self._parse_list_offset_token(next_token)
    page_rows = object_rows[list_offset:list_offset + page_size]
    response: dict[str, typing.Any] = {collection_key: page_rows}

    if list_offset + page_size < len(object_rows):
      response["NextToken"] = self._build_offset_next_token(list_offset + page_size)

    return response

  def _fleet_object(self, fleet_record: _FleetRecord) -> dict[str, typing.Any]:
    fleet_object = {
      "Name": fleet_record.name,
      "Arn": fleet_record.arn,
      "DisplayName": fleet_record.display_name,
      "State": fleet_record.state,
      "ImageName": fleet_record.image_name,
      "InstanceType": fleet_record.instance_type,
      "ComputeCapacity": {
        "DesiredInstances": fleet_record.desired_instances,
        "Available": fleet_record.desired_instances if fleet_record.state == "RUNNING" else 0,
      },
      "ComputeCapacityStatus": {
        "Desired": fleet_record.desired_instances,
        "Available": fleet_record.desired_instances if fleet_record.state == "RUNNING" else 0,
        "Running": fleet_record.desired_instances if fleet_record.state == "RUNNING" else 0,
      },
    }

    if fleet_record.max_user_duration_seconds is not None:
      fleet_object["MaxUserDurationInSeconds"] = fleet_record.max_user_duration_seconds

    return fleet_object

  def _stack_object(self, stack_record: _StackRecord) -> dict[str, typing.Any]:
    return {
      "Name": stack_record.name,
      "Arn": stack_record.arn,
      "DisplayName": stack_record.display_name,
      "State": stack_record.state,
    }

  def _raise_client_error(self, *, operation_name: str, error_code: str, message: str) -> None:
    raise ClientError(
      {
        "Error": {
          "Code": error_code,
          "Message": message,
        },
      },
      operation_name,
    )

  def _require_fleet(self, fleet_name: str) -> _FleetRecord:
    fleet_record = self._fleets.get(fleet_name)

    if fleet_record is None:
      self._raise_client_error(
        operation_name="DescribeFleets",
        error_code="ResourceNotFoundException",
        message="Fleet not found: {name}".format(name=fleet_name),
      )

    return fleet_record

  def _require_stack(self, stack_name: str) -> _StackRecord:
    stack_record = self._stacks.get(stack_name)

    if stack_record is None:
      self._raise_client_error(
        operation_name="DescribeStacks",
        error_code="ResourceNotFoundException",
        message="Stack not found: {name}".format(name=stack_name),
      )

    return stack_record

  def associate_fleet(self, *, fleet_name: str, stack_name: str) -> None:
    """Associate one fleet with one stack; enforce one stack per fleet."""

    self._apply_due_transitions()
    self._require_fleet(fleet_name)
    self._require_stack(stack_name)

    existing_stack_name = self._fleet_to_stack_name.get(fleet_name)

    if existing_stack_name is not None and existing_stack_name != stack_name:
      attached_count = 2
      self._raise_client_error(
        operation_name="AssociateFleet",
        error_code="LimitExceededException",
        message=(
          "Stack attachment limit to fleet {fleet_name} exceeded. "
          "Requested: {attached_count}, Limit: 1"
        ).format(
          fleet_name=fleet_name,
          attached_count=attached_count,
        ),
      )

    if existing_stack_name == stack_name:
      return

    self._fleet_to_stack_name[fleet_name] = stack_name
    self._stack_to_fleet_names.setdefault(stack_name, set()).add(fleet_name)

  def disassociate_fleet(self, *, fleet_name: str, stack_name: str) -> None:
    """Remove one fleet-stack association."""

    self._apply_due_transitions()
    existing_stack_name = self._fleet_to_stack_name.get(fleet_name)

    if existing_stack_name != stack_name:
      return

    del self._fleet_to_stack_name[fleet_name]
    fleet_names_for_stack = self._stack_to_fleet_names.get(stack_name)

    if fleet_names_for_stack is not None:
      fleet_names_for_stack.discard(fleet_name)

      if len(fleet_names_for_stack) < 1:
        del self._stack_to_fleet_names[stack_name]

  def list_associated_stacks(self, *, fleet_name: str, next_token: str | None = None) -> dict[str, typing.Any]:
    """Return stack names linked to one fleet."""

    self._apply_due_transitions()
    self._require_fleet(fleet_name)

    if next_token is not None:
      return {"Names": []}

    associated_stack_name = self._fleet_to_stack_name.get(fleet_name)
    associated_stack_names = []

    if associated_stack_name is not None:
      associated_stack_names.append(associated_stack_name)

    return {"Names": associated_stack_names}

  def list_associated_fleets(self, *, stack_name: str, next_token: str | None = None) -> dict[str, typing.Any]:
    """Return fleet names linked to one stack."""

    self._apply_due_transitions()
    self._require_stack(stack_name)

    if next_token is not None:
      return {"Names": []}

    associated_fleet_names = sorted(self._stack_to_fleet_names.get(stack_name, set()))

    return {"Names": associated_fleet_names}

  def create_streaming_url(
    self,
    *,
    stack_name: str,
    fleet_name: str,
    user_id: str,
    application_id: str,
    validity: int,
    session_context: str,
  ) -> dict[str, typing.Any]:
    """Create a deterministic streaming URL and record one session."""

    self._apply_due_transitions()
    self._require_fleet(fleet_name)
    self._require_stack(stack_name)

    associated_stack_name = self._fleet_to_stack_name.get(fleet_name)

    if associated_stack_name != stack_name:
      self._raise_client_error(
        operation_name="CreateStreamingURL",
        error_code="InvalidParameterException",
        message="Fleet {fleet_name} is not associated with stack {stack_name}".format(
          fleet_name=fleet_name,
          stack_name=stack_name,
        ),
      )

    self._streaming_url_counter = self._streaming_url_counter + 1
    session_id = "sim-session-{counter}".format(counter=self._streaming_url_counter)
    streaming_url = (
      "https://appstream.{region}.amazonaws.com/authenticate?"
      "parameters=sim-{session_id}-{user_id}"
    ).format(
      region=self._config.region_name,
      session_id=session_id,
      user_id=user_id,
    )
    expires_at = self._now() + validity

    self._sessions.append(
      _SessionRecord(
        session_id=session_id,
        stack_name=stack_name,
        fleet_name=fleet_name,
        user_id=user_id,
        state="ACTIVE",
      ),
    )

    return {
      "StreamingURL": streaming_url,
      "Expires": expires_at,
    }

  def describe_fleets(
    self,
    *,
    names: list[str] | None = None,
    next_token: str | None = None,
  ) -> dict[str, typing.Any]:
    """Return fleet objects visible in the simulator."""

    self._apply_due_transitions()

    if names is not None:
      fleet_objects = []

      for fleet_name in names:
        fleet_record = self._fleets.get(fleet_name)

        if fleet_record is not None:
          fleet_objects.append(self._fleet_object(fleet_record))

      return {"Fleets": fleet_objects}

    fleet_objects = [
      self._fleet_object(fleet_record)
      for fleet_record in self._fleets.values()
    ]

    return self._build_paginated_object_list_response(
      collection_key="Fleets",
      object_rows=fleet_objects,
      next_token=next_token,
    )

  def describe_stacks(
    self,
    *,
    names: list[str] | None = None,
    next_token: str | None = None,
  ) -> dict[str, typing.Any]:
    """Return stack objects visible in the simulator."""

    self._apply_due_transitions()

    if names is not None:
      stack_objects = []

      for stack_name in names:
        stack_record = self._stacks.get(stack_name)

        if stack_record is not None:
          stack_objects.append(self._stack_object(stack_record))

      return {"Stacks": stack_objects}

    stack_objects = [
      self._stack_object(stack_record)
      for stack_record in self._stacks.values()
    ]

    return self._build_paginated_object_list_response(
      collection_key="Stacks",
      object_rows=stack_objects,
      next_token=next_token,
    )

  def create_fleet(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    """Create one fleet in STOPPED state until ``start_fleet`` is called."""

    self._apply_due_transitions()
    fleet_name = keyword_arguments["Name"]
    fleet_arn = self._build_fleet_arn(fleet_name)
    desired_instances = keyword_arguments.get("ComputeCapacity", {}).get("DesiredInstances", 1)
    fleet_record = _FleetRecord(
      name=fleet_name,
      arn=fleet_arn,
      display_name=keyword_arguments.get("DisplayName", fleet_name),
      state="STOPPED",
      desired_instances=desired_instances,
      image_name=keyword_arguments.get("ImageName", ""),
      instance_type=keyword_arguments.get("InstanceType", ""),
      max_user_duration_seconds=keyword_arguments.get("MaxUserDurationInSeconds"),
    )

    self._fleets[fleet_name] = fleet_record
    tags = keyword_arguments.get("Tags") or {}

    if len(tags) > 0:
      self._tags_by_arn[fleet_arn] = dict(tags)

    return {"Fleet": self._fleet_object(fleet_record)}

  def start_fleet(self, *, name: str) -> None:
    """Move a STOPPED fleet to STARTING then RUNNING."""

    self._apply_due_transitions()
    fleet_record = self._require_fleet(name)

    if fleet_record.state in {"RUNNING", "STARTING"}:
      return

    fleet_record.state = "STARTING"
    self._schedule_transition(
      resource_kind="fleet",
      resource_name=name,
      target_state="RUNNING",
      delay_seconds=self._config.fleet_state_transition_delay_seconds,
    )

  def stop_fleet(self, *, name: str) -> None:
    """Move a RUNNING fleet to STOPPING then STOPPED."""

    self._apply_due_transitions()
    fleet_record = self._require_fleet(name)

    if fleet_record.state in {"STOPPED", "STOPPING"}:
      return

    fleet_record.state = "STOPPING"
    self._schedule_transition(
      resource_kind="fleet",
      resource_name=name,
      target_state="STOPPED",
      delay_seconds=self._config.fleet_state_transition_delay_seconds,
    )

  def delete_fleet(self, *, name: str) -> None:
    """Delete one fleet and clear associations."""

    self._apply_due_transitions()
    self._require_fleet(name)
    associated_stack_name = self._fleet_to_stack_name.pop(name, None)

    if associated_stack_name is not None:
      fleet_names_for_stack = self._stack_to_fleet_names.get(associated_stack_name)

      if fleet_names_for_stack is not None:
        fleet_names_for_stack.discard(name)

        if len(fleet_names_for_stack) < 1:
          del self._stack_to_fleet_names[associated_stack_name]

    fleet_record = self._fleets.pop(name)
    self._tags_by_arn.pop(fleet_record.arn, None)

  def create_stack(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    """Create one stack in PENDING state and schedule AVAILABLE after the stack delay."""

    self._apply_due_transitions()
    stack_name = keyword_arguments["Name"]
    stack_arn = self._build_stack_arn(stack_name)
    stack_record = _StackRecord(
      name=stack_name,
      arn=stack_arn,
      display_name=keyword_arguments.get("DisplayName", stack_name),
      state="PENDING",
    )

    self._stacks[stack_name] = stack_record
    tags = keyword_arguments.get("Tags") or {}

    if len(tags) > 0:
      self._tags_by_arn[stack_arn] = dict(tags)

    self._schedule_transition(
      resource_kind="stack",
      resource_name=stack_name,
      target_state="AVAILABLE",
      delay_seconds=self._config.stack_state_transition_delay_seconds,
    )

    return {"Stack": self._stack_object(stack_record)}

  def delete_stack(self, *, name: str) -> None:
    """Delete one stack when no fleets remain associated."""

    self._apply_due_transitions()
    self._require_stack(name)
    associated_fleet_names = self._stack_to_fleet_names.get(name)

    if associated_fleet_names is not None and len(associated_fleet_names) > 0:
      self._raise_client_error(
        operation_name="DeleteStack",
        error_code="ResourceInUseException",
        message="Stack {name} is associated with fleets".format(name=name),
      )

    stack_record = self._stacks.pop(name)
    self._tags_by_arn.pop(stack_record.arn, None)

  def describe_sessions(
    self,
    *,
    stack_name: str,
    fleet_name: str,
    user_id: str | None = None,
    authentication_type: str | None = None,
    next_token: str | None = None,
  ) -> dict[str, typing.Any]:
    """Return active sessions for one fleet and stack."""

    self._apply_due_transitions()

    session_objects = []

    for session_record in self._sessions:
      if session_record.state != "ACTIVE":
        continue

      if session_record.stack_name != stack_name:
        continue

      if session_record.fleet_name != fleet_name:
        continue

      if user_id is not None and session_record.user_id != user_id:
        continue

      session_objects.append(
        {
          "Id": session_record.session_id,
          "UserId": session_record.user_id,
          "StackName": session_record.stack_name,
          "FleetName": session_record.fleet_name,
          "State": session_record.state,
        },
      )

    return self._build_paginated_object_list_response(
      collection_key="Sessions",
      object_rows=session_objects,
      next_token=next_token,
    )

  def expire_session(self, *, session_id: str) -> None:
    """Mark one session expired."""

    self._apply_due_transitions()

    for session_record in self._sessions:
      if session_record.session_id == session_id:
        session_record.state = "EXPIRED"
        return

    self._raise_client_error(
      operation_name="ExpireSession",
      error_code="ResourceNotFoundException",
      message="Session not found: {session_id}".format(session_id=session_id),
    )

  def tag_resource(self, *, resource_arn: str, tags: dict[str, str]) -> None:
    """Attach tags to one fleet or stack ARN."""

    self._apply_due_transitions()
    existing_tags = self._tags_by_arn.setdefault(resource_arn, {})
    existing_tags.update(tags)

  def untag_resource(self, *, resource_arn: str, tag_keys: list[str]) -> None:
    """Remove tags from one fleet or stack ARN."""

    self._apply_due_transitions()
    existing_tags = self._tags_by_arn.get(resource_arn)

    if existing_tags is None:
      return

    for tag_key in tag_keys:
      existing_tags.pop(tag_key, None)

  def list_tags_for_resource(self, *, resource_arn: str) -> dict[str, typing.Any]:
    """Return tags for one fleet or stack ARN."""

    self._apply_due_transitions()

    return {"Tags": dict(self._tags_by_arn.get(resource_arn, {}))}

  def describe_images(
    self,
    *,
    names: list[str] | None = None,
    arns: list[str] | None = None,
    next_token: str | None = None,
    max_results: int | None = None,
  ) -> dict[str, typing.Any]:
    """Return image objects visible in the simulator."""

    if arns is not None:
      image_objects = []

      for image_arn in arns:
        image_object = self._image_objects_by_arn.get(image_arn)

        if image_object is not None:
          image_objects.append(image_object)

      return {"Images": image_objects}

    if names is not None:
      image_objects = []

      for image_name in names:
        image_object = self._image_objects_by_name.get(image_name)

        if image_object is not None:
          image_objects.append(image_object)

      return {"Images": image_objects}

    image_objects = list(self._image_objects_by_name.values())

    return self._build_paginated_object_list_response(
      collection_key="Images",
      object_rows=image_objects,
      next_token=next_token,
    )

  def update_fleet(self, **keyword_arguments: typing.Any) -> dict[str, typing.Any]:
    """Update one fleet display name or image name."""

    self._apply_due_transitions()
    fleet_name = keyword_arguments["Name"]
    fleet_record = self._require_fleet(fleet_name)

    if "DisplayName" in keyword_arguments:
      fleet_record.display_name = keyword_arguments["DisplayName"]

    if "ImageName" in keyword_arguments:
      fleet_record.image_name = keyword_arguments["ImageName"]

    if "MaxUserDurationInSeconds" in keyword_arguments:
      fleet_record.max_user_duration_seconds = keyword_arguments["MaxUserDurationInSeconds"]

    return {"Fleet": self._fleet_object(fleet_record)}

  def seed_image(
    self,
    *,
    image_name: str,
    application_names: list[str] | None = None,
    image_type: str = "CUSTOM",
    state: str = "AVAILABLE",
    image_arn: str | None = None,
  ) -> None:
    """Insert one image catalog row without API calls (test helper)."""

    resolved_application_names = application_names or ["Fortnite"]
    resolved_image_arn = image_arn or "arn:aws:appstream:{region}:{account}:image/{image_name}".format(
      region=self._config.region_name,
      account=self._config.account_id,
      image_name=image_name,
    )
    image_object = {
      "Name": image_name,
      "Arn": resolved_image_arn,
      "State": state,
      "ImageType": image_type,
      "Applications": [
        {"Name": application_name}
        for application_name in resolved_application_names
      ],
    }

    self._image_objects_by_name[image_name] = image_object
    self._image_objects_by_arn[resolved_image_arn] = image_object

  def seed_session(
    self,
    *,
    session_id: str,
    stack_name: str,
    fleet_name: str,
    user_id: str,
    state: str = "ACTIVE",
  ) -> None:
    """Insert one session row without API calls (test helper)."""

    self._sessions.append(
      _SessionRecord(
        session_id=session_id,
        stack_name=stack_name,
        fleet_name=fleet_name,
        user_id=user_id,
        state=state,
      ),
    )

  def seed_fleet(
    self,
    *,
    fleet_name: str,
    display_name: str | None = None,
    state: str = "RUNNING",
    desired_instances: int = 1,
  ) -> None:
    """Insert one fleet without API calls (test helper)."""

    fleet_arn = self._build_fleet_arn(fleet_name)
    self._fleets[fleet_name] = _FleetRecord(
      name=fleet_name,
      arn=fleet_arn,
      display_name=display_name or fleet_name,
      state=state,
      desired_instances=desired_instances,
      image_name="seed-image",
      instance_type="stream.standard.medium",
    )
    self.seed_image(image_name="seed-image")

  def seed_stack(
    self,
    *,
    stack_name: str,
    display_name: str | None = None,
    state: str = "AVAILABLE",
  ) -> None:
    """Insert one stack without API calls (test helper)."""

    stack_arn = self._build_stack_arn(stack_name)
    self._stacks[stack_name] = _StackRecord(
      name=stack_name,
      arn=stack_arn,
      display_name=display_name or stack_name,
      state=state,
    )
