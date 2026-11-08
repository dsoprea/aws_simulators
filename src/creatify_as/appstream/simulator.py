"""In-memory AppStream state machine for tests without live AWS calls."""

import dataclasses
import datetime
import threading
import time
import typing

from botocore.exceptions import ClientError

APPSTREAM_QUICK_DELAY_SECONDS = 10.0

DEFAULT_STREAMING_URL_TEMPLATE = (
  "https://appstream.{region}.amazonaws.com/authenticate?"
  "parameters=sim-{session_id}-{user_id}"
)

STREAM_INSTANCE_TYPE_TO_EC2_INSTANCE_TYPE = {
  "stream.standard.medium": "g4dn.xlarge",
  "stream.standard.large": "g4dn.2xlarge",
}


@dataclasses.dataclass
class AppStreamSimulatorConfig:
  """Timing and identity defaults for one simulator instance."""

  region_name: str = "us-east-1"
  account_id: str = "123456789012"
  be_quick: bool = False
  fleet_start_delay_seconds: float = 300.0
  fleet_stop_delay_seconds: float = 60.0
  stack_available_delay_seconds: float = 120.0
  image_import_delay_seconds: float = 300.0
  streaming_url_validity_seconds: int = 3600
  streaming_url_template: str = DEFAULT_STREAMING_URL_TEMPLATE
  describe_list_page_size: int | None = None
  fleet_state_transition_delay_seconds: float | None = None
  stack_state_transition_delay_seconds: float | None = None

  def resolve_quick_delay_seconds(self) -> float:
    return APPSTREAM_QUICK_DELAY_SECONDS

  def resolve_fleet_start_delay_seconds(self) -> float:
    if self.fleet_state_transition_delay_seconds is not None:
      return self.fleet_state_transition_delay_seconds

    if self.be_quick:
      return self.resolve_quick_delay_seconds()

    return self.fleet_start_delay_seconds

  def resolve_fleet_stop_delay_seconds(self) -> float:
    if self.fleet_state_transition_delay_seconds is not None:
      return self.fleet_state_transition_delay_seconds

    if self.be_quick:
      return self.resolve_quick_delay_seconds()

    return self.fleet_stop_delay_seconds

  def resolve_stack_available_delay_seconds(self) -> float:
    if self.stack_state_transition_delay_seconds is not None:
      return self.stack_state_transition_delay_seconds

    if self.be_quick:
      return self.resolve_quick_delay_seconds()

    return self.stack_available_delay_seconds

  def resolve_image_import_delay_seconds(self) -> float:
    if self.be_quick:
      return self.resolve_quick_delay_seconds()

    return self.image_import_delay_seconds


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
  lock: threading.Lock
  fleet_type: str = "ON_DEMAND"
  stream_view: str = "APP"
  max_user_duration_seconds: int | None = None
  backing_ec2_instance_ids: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class _StackRecord:
  """Mutable stack row returned by DescribeStacks."""

  name: str
  arn: str
  display_name: str
  state: str
  lock: threading.Lock
  embed_host_domains: list[str] = dataclasses.field(default_factory=list)


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
  streaming_url_counter: int = 0
  image_lock_by_name: dict[str, threading.Lock] = dataclasses.field(default_factory=dict)
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
    ec2_simulator: typing.Any | None = None,
  ) -> None:
    self._config = config or AppStreamSimulatorConfig()
    self._time_fn = time_fn or time.monotonic
    self._virtual_clock_seconds = 0.0
    self._use_virtual_clock = time_fn is not None or sleep_fn is not None
    self._ec2_simulator = ec2_simulator

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
    """Advance the virtual clock (async worker delays use ``sleep_fn``)."""

    self._virtual_clock_seconds = self._virtual_clock_seconds + seconds

  def _now(self) -> float:
    if self._use_virtual_clock:
      return self._virtual_clock_seconds

    return self._time_fn()

  def _advance_virtual_clock(self, seconds: float) -> None:
    self.advance_time(seconds)

  def _count_active_sessions_for_fleet(
    self,
    regional_state: _RegionalState,
    fleet_name: str,
  ) -> int:
    active_session_count = 0

    for session_record in regional_state.sessions:
      if session_record.fleet_name != fleet_name:
        continue

      if session_record.state != "ACTIVE":
        continue

      active_session_count = active_session_count + 1

    return active_session_count

  def _resolve_ec2_instance_type(self, stream_instance_type: str) -> str:
    mapped_instance_type = STREAM_INSTANCE_TYPE_TO_EC2_INSTANCE_TYPE.get(stream_instance_type)

    if mapped_instance_type is not None:
      return mapped_instance_type

    if stream_instance_type.startswith("stream."):
      return "g4dn.xlarge"

    return stream_instance_type

  def _provision_ec2_backing_for_fleet(
    self,
    *,
    fleet_record: _FleetRecord,
    region_name: str,
  ) -> None:
    if self._ec2_simulator is None:
      return

    image_object = self._regional_state(region_name).image_objects_by_name.get(fleet_record.image_name)
    source_ami_id = "ami-00000000"

    if image_object is not None and "SourceAmiId" in image_object:
      source_ami_id = image_object["SourceAmiId"]

    ec2_instance_type = self._resolve_ec2_instance_type(fleet_record.instance_type)
    new_instance_ids: list[str] = []

    for _instance_index in range(fleet_record.desired_instances):
      run_instances_response = self._ec2_simulator.run_instances(
        region_name=region_name,
        ImageId=source_ami_id,
        InstanceType=ec2_instance_type,
        PlatformType="windows",
        MinCount=1,
        MaxCount=1,
      )
      new_instance_ids.append(run_instances_response["Instances"][0]["InstanceId"])

    fleet_record.backing_ec2_instance_ids = new_instance_ids

  def _tear_down_ec2_backing_for_fleet(
    self,
    *,
    fleet_record: _FleetRecord,
    region_name: str,
  ) -> None:
    if self._ec2_simulator is None:
      fleet_record.backing_ec2_instance_ids = []

      return

    for instance_id in fleet_record.backing_ec2_instance_ids:
      self._ec2_simulator.terminate_instances(
        instance_ids=[instance_id],
        region_name=region_name,
      )

    fleet_record.backing_ec2_instance_ids = []

  def _run_fleet_transition_worker(
    self,
    *,
    region_name: str,
    fleet_name: str,
    expected_intermediate_state: str,
    target_state: str,
    delay_seconds: float,
  ) -> None:
    self._sleep_fn(delay_seconds)
    regional_state = self._regional_state(region_name)
    fleet_record = regional_state.fleets.get(fleet_name)

    if fleet_record is None:
      return

    with fleet_record.lock:
      if fleet_record.state != expected_intermediate_state:
        return

      fleet_record.state = target_state

      if target_state == "RUNNING":
        self._provision_ec2_backing_for_fleet(
          fleet_record=fleet_record,
          region_name=region_name,
        )

      if target_state == "STOPPED":
        self._tear_down_ec2_backing_for_fleet(
          fleet_record=fleet_record,
          region_name=region_name,
        )

  def _begin_fleet_state_transition(
    self,
    *,
    region_name: str,
    fleet_name: str,
    intermediate_state: str,
    target_state: str,
    delay_seconds: float,
  ) -> None:
    worker_thread = threading.Thread(
      target=self._run_fleet_transition_worker,
      kwargs={
        "region_name": region_name,
        "fleet_name": fleet_name,
        "expected_intermediate_state": intermediate_state,
        "target_state": target_state,
        "delay_seconds": delay_seconds,
      },
      daemon=True,
    )
    worker_thread.start()

  def _run_stack_available_worker(
    self,
    *,
    region_name: str,
    stack_name: str,
    delay_seconds: float,
  ) -> None:
    self._sleep_fn(delay_seconds)
    regional_state = self._regional_state(region_name)
    stack_record = regional_state.stacks.get(stack_name)

    if stack_record is None:
      return

    with stack_record.lock:
      if stack_record.state != "PENDING":
        return

      stack_record.state = "AVAILABLE"

  def _begin_stack_available_transition(
    self,
    *,
    region_name: str,
    stack_name: str,
    delay_seconds: float,
  ) -> None:
    worker_thread = threading.Thread(
      target=self._run_stack_available_worker,
      kwargs={
        "region_name": region_name,
        "stack_name": stack_name,
        "delay_seconds": delay_seconds,
      },
      daemon=True,
    )
    worker_thread.start()

  def _run_image_import_worker(
    self,
    *,
    region_name: str,
    image_name: str,
    delay_seconds: float,
  ) -> None:
    self._sleep_fn(delay_seconds)
    regional_state = self._regional_state(region_name)
    image_lock = regional_state.image_lock_by_name.get(image_name)

    if image_lock is None:
      return

    with image_lock:
      image_object = regional_state.image_objects_by_name.get(image_name)

      if image_object is None:
        return

      if image_object["State"] != "IMPORTING":
        return

      image_object["State"] = "AVAILABLE"
      image_arn = image_object["Arn"]
      mirrored_image = regional_state.image_objects_by_arn.get(image_arn)

      if mirrored_image is not None:
        mirrored_image["State"] = "AVAILABLE"

  def wait_for_fleet_state(
    self,
    *,
    region_name: str | None = None,
    fleet_name: str,
    state: str,
    timeout_seconds: float = 30.0,
    poll_sleep_seconds: float = 0.05,
  ) -> None:
    """Poll ``describe_fleets`` until *fleet_name* reaches *state* or time out."""

    deadline = self._now() + timeout_seconds

    while self._now() < deadline:
      fleet_objects = self.describe_fleets(region_name=region_name, names=[fleet_name])["Fleets"]

      if len(fleet_objects) > 0 and fleet_objects[0]["State"] == state:
        return

      self._sleep_fn(poll_sleep_seconds)

    raise TimeoutError(
      "fleet {fleet_name} did not reach state={state} within {timeout_seconds}s".format(
        fleet_name=fleet_name,
        state=state,
        timeout_seconds=timeout_seconds,
      ),
    )

  def wait_for_stack_state(
    self,
    *,
    region_name: str | None = None,
    stack_name: str,
    state: str = "AVAILABLE",
    timeout_seconds: float = 30.0,
    poll_sleep_seconds: float = 0.05,
  ) -> None:
    """Poll ``describe_stacks`` until *stack_name* reaches *state* or time out."""

    deadline = self._now() + timeout_seconds

    while self._now() < deadline:
      stack_objects = self.describe_stacks(region_name=region_name, names=[stack_name])["Stacks"]

      if len(stack_objects) > 0 and stack_objects[0]["State"] == state:
        return

      self._sleep_fn(poll_sleep_seconds)

    raise TimeoutError(
      "stack {stack_name} did not reach state={state} within {timeout_seconds}s".format(
        stack_name=stack_name,
        state=state,
        timeout_seconds=timeout_seconds,
      ),
    )

  def wait_for_image_state(
    self,
    *,
    region_name: str | None = None,
    image_name: str,
    state: str = "AVAILABLE",
    timeout_seconds: float = 30.0,
    poll_sleep_seconds: float = 0.05,
  ) -> None:
    """Poll ``describe_images`` until *image_name* reaches *state* or time out."""

    deadline = self._now() + timeout_seconds

    while self._now() < deadline:
      image_objects = self.describe_images(region_name=region_name, names=[image_name])["Images"]

      if len(image_objects) > 0 and image_objects[0]["State"] == state:
        return

      self._sleep_fn(poll_sleep_seconds)

    raise TimeoutError(
      "image {image_name} did not reach state={state} within {timeout_seconds}s".format(
        image_name=image_name,
        state=state,
        timeout_seconds=timeout_seconds,
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

  def _fleet_object(
    self,
    fleet_record: _FleetRecord,
    *,
    regional_state: _RegionalState,
  ) -> dict[str, typing.Any]:
    in_use_instances = self._count_active_sessions_for_fleet(regional_state, fleet_record.name)
    running_instances = fleet_record.desired_instances if fleet_record.state == "RUNNING" else 0
    available_instances = 0

    if fleet_record.state == "RUNNING":
      available_instances = max(0, running_instances - in_use_instances)

    fleet_object = {
      "Name": fleet_record.name,
      "Arn": fleet_record.arn,
      "DisplayName": fleet_record.display_name,
      "State": fleet_record.state,
      "ImageName": fleet_record.image_name,
      "InstanceType": fleet_record.instance_type,
      "FleetType": fleet_record.fleet_type,
      "StreamView": fleet_record.stream_view,
      "ComputeCapacity": {
        "DesiredInstances": fleet_record.desired_instances,
        "Available": available_instances,
      },
      "ComputeCapacityStatus": {
        "Desired": fleet_record.desired_instances,
        "Available": available_instances,
        "Running": running_instances,
        "InUse": in_use_instances,
      },
    }

    if fleet_record.max_user_duration_seconds is not None:
      fleet_object["MaxUserDurationInSeconds"] = fleet_record.max_user_duration_seconds

    return fleet_object

  def _stack_object(self, stack_record: _StackRecord) -> dict[str, typing.Any]:
    stack_object = {
      "Name": stack_record.name,
      "Arn": stack_record.arn,
      "DisplayName": stack_record.display_name,
      "State": stack_record.state,
    }

    if len(stack_record.embed_host_domains) > 0:
      stack_object["EmbedHostDomains"] = list(stack_record.embed_host_domains)

    return stack_object

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

    self._require_fleet(fleet_name, regional_state=regional_state)
    self._require_stack(stack_name, regional_state=regional_state)

    fleet_record = regional_state.fleets[fleet_name]

    if fleet_record.state != "RUNNING":
      self._raise_client_error(
        operation_name="CreateStreamingURL",
        error_code="InvalidParameterException",
        message="Fleet {fleet_name} is not RUNNING".format(fleet_name=fleet_name),
      )

    fleet_object = self._fleet_object(fleet_record, regional_state=regional_state)
    available_instances = fleet_object["ComputeCapacityStatus"]["Available"]

    if available_instances < 1:
      self._raise_client_error(
        operation_name="CreateStreamingURL",
        error_code="ResourceNotAvailableException",
        message="Fleet {fleet_name} has no available stream capacity".format(
          fleet_name=fleet_name,
        ),
      )

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
    streaming_url = self._config.streaming_url_template.format(
      region=resolved_region_name,
      session_id=session_id,
      user_id=user_id,
      stack_name=stack_name,
      fleet_name=fleet_name,
      application_id=application_id,
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

    if names is not None:
      fleet_objects = []

      for fleet_name in names:
        fleet_record = regional_state.fleets.get(fleet_name)

        if fleet_record is not None:
          fleet_objects.append(
            self._fleet_object(fleet_record, regional_state=regional_state),
          )

      return {"Fleets": fleet_objects}

    fleet_objects = [
      self._fleet_object(fleet_record, regional_state=regional_state)
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
      lock=threading.Lock(),
      fleet_type=keyword_arguments.get("FleetType", "ON_DEMAND"),
      stream_view=keyword_arguments.get("StreamView", "APP"),
      max_user_duration_seconds=keyword_arguments.get("MaxUserDurationInSeconds"),
    )

    regional_state.fleets[fleet_name] = fleet_record
    tags = keyword_arguments.get("Tags") or {}

    if len(tags) > 0:
      regional_state.tags_by_arn[fleet_arn] = dict(tags)

    return {"Fleet": self._fleet_object(fleet_record, regional_state=regional_state)}

  def start_fleet(self, *, region_name: str | None = None, name: str) -> None:
    """Move a STOPPED fleet to STARTING then RUNNING."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    fleet_record = self._require_fleet(name, regional_state=regional_state)

    if fleet_record.state in {"RUNNING", "STARTING"}:
      return

    fleet_record.state = "STARTING"
    self._begin_fleet_state_transition(
      region_name=resolved_region_name,
      fleet_name=name,
      intermediate_state="STARTING",
      target_state="RUNNING",
      delay_seconds=self._config.resolve_fleet_start_delay_seconds(),
    )

  def stop_fleet(self, *, region_name: str | None = None, name: str) -> None:
    """Move a RUNNING fleet to STOPPING then STOPPED."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    fleet_record = self._require_fleet(name, regional_state=regional_state)

    if fleet_record.state in {"STOPPED", "STOPPING"}:
      return

    fleet_record.state = "STOPPING"
    self._begin_fleet_state_transition(
      region_name=resolved_region_name,
      fleet_name=name,
      intermediate_state="STOPPING",
      target_state="STOPPED",
      delay_seconds=self._config.resolve_fleet_stop_delay_seconds(),
    )

  def delete_fleet(self, *, region_name: str | None = None, name: str) -> None:
    """Delete one fleet and clear associations."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    fleet_record = self._require_fleet(name, regional_state=regional_state)
    self._tear_down_ec2_backing_for_fleet(
      fleet_record=fleet_record,
      region_name=resolved_region_name,
    )
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

    stack_name = keyword_arguments["Name"]
    stack_arn = self._build_stack_arn(stack_name, region_name=resolved_region_name)
    embed_host_domains = keyword_arguments.get("EmbedHostDomains") or []
    stack_record = _StackRecord(
      name=stack_name,
      arn=stack_arn,
      display_name=keyword_arguments.get("DisplayName", stack_name),
      state="PENDING",
      lock=threading.Lock(),
      embed_host_domains=list(embed_host_domains),
    )

    regional_state.stacks[stack_name] = stack_record
    tags = keyword_arguments.get("Tags") or {}

    if len(tags) > 0:
      regional_state.tags_by_arn[stack_arn] = dict(tags)

    stack_object_for_response = self._stack_object(stack_record)
    stack_object_for_response["State"] = "PENDING"

    self._begin_stack_available_transition(
      region_name=resolved_region_name,
      stack_name=stack_name,
      delay_seconds=self._config.resolve_stack_available_delay_seconds(),
    )

    return {"Stack": stack_object_for_response}

  def delete_stack(self, *, region_name: str | None = None, name: str) -> None:
    """Delete one stack when no fleets remain associated."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    stack_record = self._require_stack(name, regional_state=regional_state)

    if stack_record.state == "PENDING":
      self._raise_client_error(
        operation_name="DeleteStack",
        error_code="ResourceInUseException",
        message="Stack {name} is not AVAILABLE".format(name=name),
      )

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

    fleet_name = keyword_arguments["Name"]
    fleet_record = self._require_fleet(fleet_name, regional_state=regional_state)

    if "DisplayName" in keyword_arguments:
      fleet_record.display_name = keyword_arguments["DisplayName"]

    if "ImageName" in keyword_arguments:
      fleet_record.image_name = keyword_arguments["ImageName"]

    if "MaxUserDurationInSeconds" in keyword_arguments:
      fleet_record.max_user_duration_seconds = keyword_arguments["MaxUserDurationInSeconds"]

    if "StreamView" in keyword_arguments:
      fleet_record.stream_view = keyword_arguments["StreamView"]

    if "ComputeCapacity" in keyword_arguments:
      compute_capacity = keyword_arguments["ComputeCapacity"]

      if "DesiredInstances" in compute_capacity:
        fleet_record.desired_instances = compute_capacity["DesiredInstances"]

    return {"Fleet": self._fleet_object(fleet_record, regional_state=regional_state)}

  def create_imported_image(
    self,
    *,
    region_name: str | None = None,
    **keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any]:
    """Start one image import; transition IMPORTING to AVAILABLE on a worker thread."""

    regional_state = self._regional_state(region_name)
    resolved_region_name = self._resolve_region_name(region_name)

    if keyword_arguments.get("DryRun"):
      self._raise_client_error(
        operation_name="CreateImportedImage",
        error_code="DryRunOperationException",
        message="DryRun validation succeeded",
      )

    image_name = keyword_arguments["Name"]

    if image_name in regional_state.image_objects_by_name:
      self._raise_client_error(
        operation_name="CreateImportedImage",
        error_code="ResourceAlreadyExistsException",
        message="Image {name} already exists".format(name=image_name),
      )

    image_arn = "arn:aws:appstream:{region}:{account}:image/{image_name}".format(
      region=resolved_region_name,
      account=self._config.account_id,
      image_name=image_name,
    )
    applications: list[dict[str, str]] = []

    if "AppCatalogConfig" in keyword_arguments:
      for app_catalog_row in keyword_arguments["AppCatalogConfig"]:
        applications.append({"Name": app_catalog_row["Name"]})

    created_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    image_object = {
      "Name": image_name,
      "Arn": image_arn,
      "State": "IMPORTING",
      "ImageType": "CUSTOM",
      "SourceAmiId": keyword_arguments.get("SourceAmiId", "ami-00000000"),
      "CreatedTime": created_time,
      "Applications": applications,
    }

    regional_state.image_objects_by_name[image_name] = image_object
    regional_state.image_objects_by_arn[image_arn] = image_object
    regional_state.image_lock_by_name[image_name] = threading.Lock()
    tags = keyword_arguments.get("Tags") or {}

    if len(tags) > 0:
      regional_state.tags_by_arn[image_arn] = dict(tags)

    import_delay_seconds = self._config.resolve_image_import_delay_seconds()
    worker_thread = threading.Thread(
      target=self._run_image_import_worker,
      kwargs={
        "region_name": resolved_region_name,
        "image_name": image_name,
        "delay_seconds": import_delay_seconds,
      },
      daemon=True,
    )
    imported_image_for_response = dict(image_object)
    imported_image_for_response["State"] = "IMPORTING"
    worker_thread.start()

    return {"ImportedImage": imported_image_for_response}

  def delete_image(
    self,
    *,
    region_name: str | None = None,
    name: str,
  ) -> None:
    """Delete one image when state is AVAILABLE or FAILED."""

    regional_state = self._regional_state(region_name)
    image_object = regional_state.image_objects_by_name.get(name)

    if image_object is None:
      self._raise_client_error(
        operation_name="DeleteImage",
        error_code="ResourceNotFoundException",
        message="Image not found: {name}".format(name=name),
      )

    image_state = image_object["State"]

    if image_state not in {"AVAILABLE", "FAILED"}:
      self._raise_client_error(
        operation_name="DeleteImage",
        error_code="InvalidImageStateException",
        message="Image {name} state={state} is not deletable".format(
          name=name,
          state=image_state,
        ),
      )

    image_arn = image_object["Arn"]
    del regional_state.image_objects_by_name[name]
    regional_state.image_objects_by_arn.pop(image_arn, None)
    regional_state.image_lock_by_name.pop(name, None)
    regional_state.tags_by_arn.pop(image_arn, None)

  def update_stack(
    self,
    *,
    region_name: str | None = None,
    **keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any]:
    """Update stack fields such as ``EmbedHostDomains``."""

    regional_state = self._regional_state(region_name)
    stack_name = keyword_arguments["Name"]
    stack_record = self._require_stack(stack_name, regional_state=regional_state)

    if "EmbedHostDomains" in keyword_arguments:
      stack_record.embed_host_domains = list(keyword_arguments["EmbedHostDomains"])

    if "DisplayName" in keyword_arguments:
      stack_record.display_name = keyword_arguments["DisplayName"]

    return {"Stack": self._stack_object(stack_record)}

  def seed_image(
    self,
    *,
    region_name: str | None = None,
    image_name: str,
    application_names: list[str] | None = None,
    image_type: str = "CUSTOM",
    state: str = "AVAILABLE",
    image_arn: str | None = None,
    source_ami_id: str = "ami-00000000",
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
      "SourceAmiId": source_ami_id,
      "Applications": [
        {"Name": application_name}
        for application_name in resolved_application_names
      ],
    }

    regional_state.image_objects_by_name[image_name] = image_object
    regional_state.image_objects_by_arn[resolved_image_arn] = image_object
    regional_state.image_lock_by_name[image_name] = threading.Lock()

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
      lock=threading.Lock(),
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
      lock=threading.Lock(),
    )
