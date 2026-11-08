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


@dataclasses.dataclass
class _RegionalState:
  """In-memory AppStream inventory for one AWS region."""

  fleets: dict[str, _FleetRecord] = dataclasses.field(default_factory=dict)
  stacks: dict[str, _StackRecord] = dataclasses.field(default_factory=dict)
  fleet_to_stack_name: dict[str, str] = dataclasses.field(default_factory=dict)
  stack_to_fleet_names: dict[str, set[str]] = dataclasses.field(default_factory=dict)
  tags_by_arn: dict[str, dict[str, str]] = dataclasses.field(default_factory=dict)
  sessions: list[_SessionRecord] = dataclasses.field(default_factory=list)
  scheduled_transitions: list[_ScheduledTransition] = dataclasses.field(default_factory=list)
  streaming_url_counter: int = 0
  image_objects_by_name: dict[str, dict[str, typing.Any]] = dataclasses.field(default_factory=dict)
  image_objects_by_arn: dict[str, dict[str, typing.Any]] = dataclasses.field(default_factory=dict)


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

    self._regional_state_by_region_name: dict[str, _RegionalState] = {}

  def reset(self) -> None:
    """Clear all simulator state (use between tests)."""

    self._regional_state_by_region_name.clear()
    self._virtual_clock_seconds = 0.0

  def _resolve_region_name(self, region_name: str | None) -> str:
    """Return the effective region for one call (default from config)."""

    if region_name is not None:
      return region_name

    return self._config.region_name

  def _regional_state(self, region_name: str | None) -> _RegionalState:
    """Return regional inventory, creating an empty partition when needed."""

    resolved_region_name = self._resolve_region_name(region_name)

    if resolved_region_name not in self._regional_state_by_region_name:
      self._regional_state_by_region_name[resolved_region_name] = _RegionalState()

    return self._regional_state_by_region_name[resolved_region_name]

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

  def _apply_due_transitions(self, region_name: str | None = None) -> None:
    if region_name is not None:
      self._apply_due_transitions_for_state(self._regional_state(region_name))

      return

    for regional_state in self._regional_state_by_region_name.values():
      self._apply_due_transitions_for_state(regional_state)

  def _apply_due_transitions_for_state(self, regional_state: _RegionalState) -> None:
    current_time = self._now()
    remaining_transitions: list[_ScheduledTransition] = []

    for scheduled_transition in sorted(
      regional_state.scheduled_transitions,
      key=lambda row: row.fire_at):
      if scheduled_transition.fire_at > current_time:
        remaining_transitions.append(scheduled_transition)
        continue

      if scheduled_transition.resource_kind == "fleet":
        fleet_record = regional_state.fleets.get(scheduled_transition.resource_name)

        if fleet_record is not None:
          fleet_record.state = scheduled_transition.target_state

      if scheduled_transition.resource_kind == "stack":
        stack_record = regional_state.stacks.get(scheduled_transition.resource_name)

        if stack_record is not None:
          stack_record.state = scheduled_transition.target_state

    regional_state.scheduled_transitions = remaining_transitions

  def _schedule_transition(
    self,
    *,
    regional_state: _RegionalState,
    resource_kind: str,
    resource_name: str,
    target_state: str,
    delay_seconds: float,
  ) -> None:
    regional_state.scheduled_transitions.append(
      _ScheduledTransition(
        fire_at=self._now() + delay_seconds,
        resource_kind=resource_kind,
        resource_name=resource_name,
        target_state=target_state,
      ),
    )

  def _build_fleet_arn(self, fleet_name: str, *, region_name: str) -> str:
    return "arn:aws:appstream:{region}:{account}:fleet/{name}".format(
      region=region_name,
      account=self._config.account_id,
      name=fleet_name,
    )

  def _build_stack_arn(self, stack_name: str, *, region_name: str) -> str:
    return "arn:aws:appstream:{region}:{account}:stack/{name}".format(
      region=region_name,
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

  def _require_fleet(self, fleet_name: str, *, regional_state: _RegionalState) -> _FleetRecord:
    fleet_record = regional_state.fleets.get(fleet_name)

    if fleet_record is None:
      self._raise_client_error(
        operation_name="DescribeFleets",
        error_code="ResourceNotFoundException",
        message="Fleet not found: {name}".format(name=fleet_name),
      )

    return fleet_record

  def _require_stack(self, stack_name: str, *, regional_state: _RegionalState) -> _StackRecord:
    stack_record = regional_state.stacks.get(stack_name)

    if stack_record is None:
      self._raise_client_error(
        operation_name="DescribeStacks",
        error_code="ResourceNotFoundException",
        message="Stack not found: {name}".format(name=stack_name),
      )

    return stack_record

  def associate_fleet(
    self,
    *,
    region_name: str | None = None,
    fleet_name: str,
    stack_name: str,
  ) -> None:
    """Associate one fleet with one stack; enforce one stack per fleet."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    self._require_fleet(fleet_name, regional_state=regional_state)
    self._require_stack(stack_name, regional_state=regional_state)

    existing_stack_name = regional_state.fleet_to_stack_name.get(fleet_name)

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

    regional_state.fleet_to_stack_name[fleet_name] = stack_name
    regional_state.stack_to_fleet_names.setdefault(stack_name, set()).add(fleet_name)

  def disassociate_fleet(
    self,
    *,
    region_name: str | None = None,
    fleet_name: str,
    stack_name: str,
  ) -> None:
    """Remove one fleet-stack association."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    existing_stack_name = regional_state.fleet_to_stack_name.get(fleet_name)

    if existing_stack_name != stack_name:
      return

    del regional_state.fleet_to_stack_name[fleet_name]
    fleet_names_for_stack = regional_state.stack_to_fleet_names.get(stack_name)

    if fleet_names_for_stack is not None:
      fleet_names_for_stack.discard(fleet_name)

      if len(fleet_names_for_stack) < 1:
        del regional_state.stack_to_fleet_names[stack_name]

  def list_associated_stacks(
    self,
    *,
    region_name: str | None = None,
    fleet_name: str,
    next_token: str | None = None,
  ) -> dict[str, typing.Any]:
    """Return stack names linked to one fleet."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    self._require_fleet(fleet_name, regional_state=regional_state)

    if next_token is not None:
      return {"Names": []}

    associated_stack_name = regional_state.fleet_to_stack_name.get(fleet_name)
    associated_stack_names = []

    if associated_stack_name is not None:
      associated_stack_names.append(associated_stack_name)

    return {"Names": associated_stack_names}

  def list_associated_fleets(
    self,
    *,
    region_name: str | None = None,
    stack_name: str,
    next_token: str | None = None,
  ) -> dict[str, typing.Any]:
    """Return fleet names linked to one stack."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    self._require_stack(stack_name, regional_state=regional_state)

    if next_token is not None:
      return {"Names": []}

    associated_fleet_names = sorted(regional_state.stack_to_fleet_names.get(stack_name, set()))

    return {"Names": associated_fleet_names}

  def create_streaming_url(
    self,
    *,
    region_name: str | None = None,
    stack_name: str,
    fleet_name: str,
    user_id: str,
    application_id: str,
    validity: int,
    session_context: str,
  ) -> dict[str, typing.Any]:
    """Create a deterministic streaming URL and record one session."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    self._require_fleet(fleet_name, regional_state=regional_state)
    self._require_stack(stack_name, regional_state=regional_state)

    associated_stack_name = regional_state.fleet_to_stack_name.get(fleet_name)

    if associated_stack_name != stack_name:
      self._raise_client_error(
        operation_name="CreateStreamingURL",
        error_code="InvalidParameterException",
        message="Fleet {fleet_name} is not associated with stack {stack_name}".format(
          fleet_name=fleet_name,
          stack_name=stack_name,
        ),
      )

    regional_state.streaming_url_counter = regional_state.streaming_url_counter + 1
    session_id = "sim-session-{counter}".format(counter=regional_state.streaming_url_counter)
    streaming_url = (
      "https://appstream.{region}.amazonaws.com/authenticate?"
      "parameters=sim-{session_id}-{user_id}"
    ).format(
      region=resolved_region_name,
      session_id=session_id,
      user_id=user_id,
    )
    expires_at = self._now() + validity

    regional_state.sessions.append(
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
    region_name: str | None = None,
    names: list[str] | None = None,
    next_token: str | None = None,
  ) -> dict[str, typing.Any]:
    """Return fleet objects visible in the simulator."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)

    if names is not None:
      fleet_objects = []

      for fleet_name in names:
        fleet_record = regional_state.fleets.get(fleet_name)

        if fleet_record is not None:
          fleet_objects.append(self._fleet_object(fleet_record))

      return {"Fleets": fleet_objects}

    fleet_objects = [
      self._fleet_object(fleet_record)
      for fleet_record in regional_state.fleets.values()
    ]

    return self._build_paginated_object_list_response(
      collection_key="Fleets",
      object_rows=fleet_objects,
      next_token=next_token,
    )

  def describe_stacks(
    self,
    *,
    region_name: str | None = None,
    names: list[str] | None = None,
    next_token: str | None = None,
  ) -> dict[str, typing.Any]:
    """Return stack objects visible in the simulator."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)

    if names is not None:
      stack_objects = []

      for stack_name in names:
        stack_record = regional_state.stacks.get(stack_name)

        if stack_record is not None:
          stack_objects.append(self._stack_object(stack_record))

      return {"Stacks": stack_objects}

    stack_objects = [
      self._stack_object(stack_record)
      for stack_record in regional_state.stacks.values()
    ]

    return self._build_paginated_object_list_response(
      collection_key="Stacks",
      object_rows=stack_objects,
      next_token=next_token,
    )

  def create_fleet(
    self,
    *,
    region_name: str | None = None,
    **keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any]:
    """Create one fleet in STOPPED state until ``start_fleet`` is called."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    fleet_name = keyword_arguments["Name"]
    fleet_arn = self._build_fleet_arn(fleet_name, region_name=resolved_region_name)
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

    regional_state.fleets[fleet_name] = fleet_record
    tags = keyword_arguments.get("Tags") or {}

    if len(tags) > 0:
      regional_state.tags_by_arn[fleet_arn] = dict(tags)

    return {"Fleet": self._fleet_object(fleet_record)}

  def start_fleet(self, *, region_name: str | None = None, name: str) -> None:
    """Move a STOPPED fleet to STARTING then RUNNING."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    fleet_record = self._require_fleet(name, regional_state=regional_state)

    if fleet_record.state in {"RUNNING", "STARTING"}:
      return

    fleet_record.state = "STARTING"
    self._schedule_transition(
      regional_state=regional_state,
      resource_kind="fleet",
      resource_name=name,
      target_state="RUNNING",
      delay_seconds=self._config.fleet_state_transition_delay_seconds,
    )

  def stop_fleet(self, *, region_name: str | None = None, name: str) -> None:
    """Move a RUNNING fleet to STOPPING then STOPPED."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    fleet_record = self._require_fleet(name, regional_state=regional_state)

    if fleet_record.state in {"STOPPED", "STOPPING"}:
      return

    fleet_record.state = "STOPPING"
    self._schedule_transition(
      regional_state=regional_state,
      resource_kind="fleet",
      resource_name=name,
      target_state="STOPPED",
      delay_seconds=self._config.fleet_state_transition_delay_seconds,
    )

  def delete_fleet(self, *, region_name: str | None = None, name: str) -> None:
    """Delete one fleet and clear associations."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    self._require_fleet(name, regional_state=regional_state)
    associated_stack_name = regional_state.fleet_to_stack_name.pop(name, None)

    if associated_stack_name is not None:
      fleet_names_for_stack = regional_state.stack_to_fleet_names.get(associated_stack_name)

      if fleet_names_for_stack is not None:
        fleet_names_for_stack.discard(name)

        if len(fleet_names_for_stack) < 1:
          del regional_state.stack_to_fleet_names[associated_stack_name]

    fleet_record = regional_state.fleets.pop(name)
    regional_state.tags_by_arn.pop(fleet_record.arn, None)

  def create_stack(
    self,
    *,
    region_name: str | None = None,
    **keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any]:
    """Create one stack in PENDING state and schedule AVAILABLE after the stack delay."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    stack_name = keyword_arguments["Name"]
    stack_arn = self._build_stack_arn(stack_name, region_name=resolved_region_name)
    stack_record = _StackRecord(
      name=stack_name,
      arn=stack_arn,
      display_name=keyword_arguments.get("DisplayName", stack_name),
      state="PENDING",
    )

    regional_state.stacks[stack_name] = stack_record
    tags = keyword_arguments.get("Tags") or {}

    if len(tags) > 0:
      regional_state.tags_by_arn[stack_arn] = dict(tags)

    self._schedule_transition(
      regional_state=regional_state,
      resource_kind="stack",
      resource_name=stack_name,
      target_state="AVAILABLE",
      delay_seconds=self._config.stack_state_transition_delay_seconds,
    )

    return {"Stack": self._stack_object(stack_record)}

  def delete_stack(self, *, region_name: str | None = None, name: str) -> None:
    """Delete one stack when no fleets remain associated."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    self._require_stack(name, regional_state=regional_state)
    associated_fleet_names = regional_state.stack_to_fleet_names.get(name)

    if associated_fleet_names is not None and len(associated_fleet_names) > 0:
      self._raise_client_error(
        operation_name="DeleteStack",
        error_code="ResourceInUseException",
        message="Stack {name} is associated with fleets".format(name=name),
      )

    stack_record = regional_state.stacks.pop(name)
    regional_state.tags_by_arn.pop(stack_record.arn, None)

  def describe_sessions(
    self,
    *,
    region_name: str | None = None,
    stack_name: str,
    fleet_name: str,
    user_id: str | None = None,
    authentication_type: str | None = None,
    next_token: str | None = None,
  ) -> dict[str, typing.Any]:
    """Return active sessions for one fleet and stack."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)

    session_objects = []

    for session_record in regional_state.sessions:
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

  def expire_session(self, *, region_name: str | None = None, session_id: str) -> None:
    """Mark one session expired."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)

    for session_record in regional_state.sessions:
      if session_record.session_id == session_id:
        session_record.state = "EXPIRED"
        return

    self._raise_client_error(
      operation_name="ExpireSession",
      error_code="ResourceNotFoundException",
      message="Session not found: {session_id}".format(session_id=session_id),
    )

  def tag_resource(
    self,
    *,
    region_name: str | None = None,
    resource_arn: str,
    tags: dict[str, str],
  ) -> None:
    """Attach tags to one fleet or stack ARN."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    existing_tags = regional_state.tags_by_arn.setdefault(resource_arn, {})
    existing_tags.update(tags)

  def untag_resource(
    self,
    *,
    region_name: str | None = None,
    resource_arn: str,
    tag_keys: list[str],
  ) -> None:
    """Remove tags from one fleet or stack ARN."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    existing_tags = regional_state.tags_by_arn.get(resource_arn)

    if existing_tags is None:
      return

    for tag_key in tag_keys:
      existing_tags.pop(tag_key, None)

  def list_tags_for_resource(
    self,
    *,
    region_name: str | None = None,
    resource_arn: str,
  ) -> dict[str, typing.Any]:
    """Return tags for one fleet or stack ARN."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)

    return {"Tags": dict(regional_state.tags_by_arn.get(resource_arn, {}))}

  def describe_images(
    self,
    *,
    region_name: str | None = None,
    names: list[str] | None = None,
    arns: list[str] | None = None,
    next_token: str | None = None,
    max_results: int | None = None,
  ) -> dict[str, typing.Any]:
    """Return image objects visible in the simulator."""

    regional_state = self._regional_state(region_name)

    if arns is not None:
      image_objects = []

      for image_arn in arns:
        image_object = regional_state.image_objects_by_arn.get(image_arn)

        if image_object is not None:
          image_objects.append(image_object)

      return {"Images": image_objects}

    if names is not None:
      image_objects = []

      for image_name in names:
        image_object = regional_state.image_objects_by_name.get(image_name)

        if image_object is not None:
          image_objects.append(image_object)

      return {"Images": image_objects}

    image_objects = list(regional_state.image_objects_by_name.values())

    return self._build_paginated_object_list_response(
      collection_key="Images",
      object_rows=image_objects,
      next_token=next_token,
    )

  def update_fleet(
    self,
    *,
    region_name: str | None = None,
    **keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any]:
    """Update one fleet display name or image name."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    self._apply_due_transitions(resolved_region_name)
    fleet_name = keyword_arguments["Name"]
    fleet_record = self._require_fleet(fleet_name, regional_state=regional_state)

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
    region_name: str | None = None,
    image_name: str,
    application_names: list[str] | None = None,
    image_type: str = "CUSTOM",
    state: str = "AVAILABLE",
    image_arn: str | None = None,
  ) -> None:
    """Insert one image catalog row without API calls (test helper)."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)
    resolved_application_names = application_names or ["Fortnite"]
    resolved_image_arn = image_arn or "arn:aws:appstream:{region}:{account}:image/{image_name}".format(
      region=resolved_region_name,
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

    regional_state.image_objects_by_name[image_name] = image_object
    regional_state.image_objects_by_arn[resolved_image_arn] = image_object

  def seed_session(
    self,
    *,
    region_name: str | None = None,
    session_id: str,
    stack_name: str,
    fleet_name: str,
    user_id: str,
    state: str = "ACTIVE",
  ) -> None:
    """Insert one session row without API calls (test helper)."""

    regional_state = self._regional_state(region_name)

    regional_state.sessions.append(
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
    region_name: str | None = None,
    fleet_name: str,
    display_name: str | None = None,
    state: str = "RUNNING",
    desired_instances: int = 1,
  ) -> None:
    """Insert one fleet without API calls (test helper)."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)
    fleet_arn = self._build_fleet_arn(fleet_name, region_name=resolved_region_name)
    regional_state.fleets[fleet_name] = _FleetRecord(
      name=fleet_name,
      arn=fleet_arn,
      display_name=display_name or fleet_name,
      state=state,
      desired_instances=desired_instances,
      image_name="seed-image",
      instance_type="stream.standard.medium",
    )
    self.seed_image(region_name=resolved_region_name, image_name="seed-image")

  def seed_stack(
    self,
    *,
    region_name: str | None = None,
    stack_name: str,
    display_name: str | None = None,
    state: str = "AVAILABLE",
  ) -> None:
    """Insert one stack without API calls (test helper)."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)
    stack_arn = self._build_stack_arn(stack_name, region_name=resolved_region_name)
    regional_state.stacks[stack_name] = _StackRecord(
      name=stack_name,
      arn=stack_arn,
      display_name=display_name or stack_name,
      state=state,
    )
