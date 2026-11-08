"""In-memory EC2 instance lifecycle for tests without live AWS calls."""

import dataclasses
import queue
import secrets
import threading
import time
import typing

from botocore.exceptions import ClientError

EC2_STATE_NAME_TO_CODE = {
  "pending": 0,
  "running": 16,
  "shutting-down": 32,
  "terminated": 48,
  "stopping": 64,
  "stopped": 80,
}

SIMULATOR_PLATFORM_TYPE_TAG_KEY = "creatify_as_platform_type"

LINUX_PLATFORM_DETAILS = "Linux/UNIX"
WINDOWS_PLATFORM_DETAILS = "Windows"


@dataclasses.dataclass
class Ec2SimulatorConfig:
  """Timing and identity defaults for one EC2 simulator instance."""

  region_name: str = "us-east-1"
  account_id: str = "123456789012"
  be_quick: bool = False


@dataclasses.dataclass
class _InstanceWorkItem:
  """One delayed state transition executed on the instance worker thread."""

  delay_seconds: float
  target_state_name: str


@dataclasses.dataclass
class _InstanceRecord:
  """Mutable EC2 instance row backed by one worker thread."""

  instance_id: str
  instance_type: str
  image_id: str
  platform_type: str
  state_name: str
  tags: list[dict[str, str]]
  lock: threading.Lock
  command_queue: queue.Queue[_InstanceWorkItem | None]
  worker_thread: threading.Thread | None
  launch_time: float


def _resolve_platform_details(platform_type: str) -> str:
  """Return DescribeInstances ``PlatformDetails`` for a normalized platform type."""

  if platform_type == "windows":
    return WINDOWS_PLATFORM_DETAILS

  if platform_type == "linux":
    return LINUX_PLATFORM_DETAILS

  raise ValueError(
    "unsupported platform_type={platform_type}; expected linux or windows".format(
      platform_type=platform_type,
    ),
  )


def _run_instance_worker_loop(
  instance_record: _InstanceRecord,
  sleep_fn: typing.Callable[[float], None],
) -> None:
  """Process delayed transitions for one instance until shutdown or termination."""

  while True:
    work_item = instance_record.command_queue.get()

    if work_item is None:
      return

    sleep_fn(work_item.delay_seconds)

    with instance_record.lock:
      instance_record.state_name = work_item.target_state_name

    if work_item.target_state_name == "terminated":
      return


class Ec2Simulator:
  """Models EC2 start, stop, terminate, describe, and tag semantics with per-instance threads."""

  def __init__(
    self,
    *,
    config: Ec2SimulatorConfig | None = None,
    sleep_fn: typing.Callable[[float], None] | None = None,
    time_fn: typing.Callable[[], float] | None = None,
  ) -> None:
    self._config = config or Ec2SimulatorConfig()
    self._time_fn = time_fn or time.monotonic

    if sleep_fn is not None:
      self._sleep_fn = sleep_fn
    else:
      self._sleep_fn = time.sleep

    self._instances_by_region_name: dict[str, dict[str, _InstanceRecord]] = {}
    self._simulator_lock = threading.Lock()
    self._next_instance_sequence = 0

  def reset(self) -> None:
    """Clear all instances, joining worker threads first."""

    with self._simulator_lock:
      instance_records: list[_InstanceRecord] = []

      for instances_by_id in self._instances_by_region_name.values():
        instance_records.extend(instances_by_id.values())

      self._instances_by_region_name.clear()
      self._next_instance_sequence = 0

    for instance_record in instance_records:
      self._shutdown_instance_worker(instance_record)

  def _resolve_region_name(self, region_name: str | None) -> str:
    """Return the effective region for one call (default from config)."""

    if region_name is not None:
      return region_name

    return self._config.region_name

  def _instances_for_region(self, region_name: str | None) -> dict[str, _InstanceRecord]:
    """Return the instance map for one region, creating an empty partition when needed."""

    resolved_region_name = self._resolve_region_name(region_name)

    with self._simulator_lock:
      if resolved_region_name not in self._instances_by_region_name:
        self._instances_by_region_name[resolved_region_name] = {}

      return self._instances_by_region_name[resolved_region_name]

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

  def _build_instance_id(self) -> str:
    """Return a unique ``i-`` instance identifier."""

    with self._simulator_lock:
      self._next_instance_sequence = self._next_instance_sequence + 1
      sequence_token = self._next_instance_sequence

    suffix = "{sequence:08x}{random}".format(
      sequence=sequence_token,
      random=secrets.token_hex(4),
    )

    return "i-{suffix}".format(suffix=suffix)

  def _start_delay_seconds(self, platform_type: str) -> float:
    """Return wall time from ``pending`` to ``running`` for *platform_type*."""

    if self._config.be_quick:
      return 10.0

    if platform_type == "windows":
      return 240.0

    return 60.0

  def _stop_delay_seconds(self, platform_type: str) -> float:
    """Return wall time from ``stopping`` to ``stopped`` for *platform_type*."""

    if self._config.be_quick:
      return 10.0

    return 60.0

  def _terminate_delay_seconds(self, platform_type: str) -> float:
    """Return wall time from ``shutting-down`` to ``terminated`` for *platform_type*."""

    if self._config.be_quick:
      return 10.0

    if platform_type == "windows":
      return 180.0

    return 60.0

  def _require_instance(
    self,
    instance_id: str,
    *,
    region_name: str | None,
    operation_name: str,
  ) -> _InstanceRecord:
    """Return one instance row or raise ``InvalidInstanceID.NotFound``."""

    instances_by_id = self._instances_for_region(region_name)
    instance_record = instances_by_id.get(instance_id)

    if instance_record is None:
      self._raise_client_error(
        operation_name=operation_name,
        error_code="InvalidInstanceID.NotFound",
        message="The instance ID '{instance_id}' does not exist".format(
          instance_id=instance_id,
        ),
      )

    return instance_record

  def _ensure_instance_worker(self, instance_record: _InstanceRecord) -> None:
    """Start the worker thread when the instance has no active worker yet."""

    with instance_record.lock:
      if instance_record.worker_thread is not None:
        if instance_record.worker_thread.is_alive():
          return

      worker_thread = threading.Thread(
        target=_run_instance_worker_loop,
        args=(instance_record, self._sleep_fn),
        name="ec2-sim-{instance_id}".format(instance_id=instance_record.instance_id),
        daemon=True,
      )
      instance_record.worker_thread = worker_thread
      worker_thread.start()

  def _shutdown_instance_worker(self, instance_record: _InstanceRecord) -> None:
    """Signal the instance worker to exit and join when possible."""

    instance_record.command_queue.put(None)
    worker_thread = instance_record.worker_thread

    if worker_thread is not None and worker_thread.is_alive():
      worker_thread.join(timeout=5.0)

  def _enqueue_transition(
    self,
    instance_record: _InstanceRecord,
    *,
    target_state_name: str,
    delay_seconds: float,
  ) -> None:
    """Schedule one delayed transition on the instance worker thread."""

    self._ensure_instance_worker(instance_record)
    instance_record.command_queue.put(
      _InstanceWorkItem(
        delay_seconds=delay_seconds,
        target_state_name=target_state_name,
      ),
    )

  def _normalize_platform_type(
    self,
    *,
    platform_type: str | None,
    tag_rows: list[dict[str, str]],
  ) -> str:
    """Resolve linux vs windows from an explicit value or the simulator platform tag."""

    if platform_type is not None:
      normalized = platform_type.strip().lower()

      if normalized in ("linux", "windows"):
        return normalized

      raise ValueError(
        "unsupported platform_type={platform_type}".format(
          platform_type=platform_type,
        ),
      )

    for tag_row in tag_rows:
      if tag_row["Key"] == SIMULATOR_PLATFORM_TYPE_TAG_KEY:
        tag_value = tag_row["Value"].strip().lower()

        if tag_value in ("linux", "windows"):
          return tag_value

        raise ValueError(
          "tag {tag_key} must be linux or windows".format(
            tag_key=SIMULATOR_PLATFORM_TYPE_TAG_KEY,
          ),
        )

    return "linux"

  def _gather_tags_from_run_instances_keyword_arguments(
    self,
    keyword_arguments: dict[str, typing.Any],
  ) -> list[dict[str, str]]:
    """Extract instance ``Tags`` rows from ``TagSpecifications``."""

    tag_rows: list[dict[str, str]] = []
    tag_specifications = keyword_arguments.get("TagSpecifications")

    if not isinstance(tag_specifications, list):
      return tag_rows

    for specification in tag_specifications:
      if not isinstance(specification, dict):
        continue

      if specification.get("ResourceType") != "instance":
        continue

      specification_tags = specification.get("Tags")

      if not isinstance(specification_tags, list):
        continue

      for tag_entry in specification_tags:
        if not isinstance(tag_entry, dict):
          continue

        if "Key" not in tag_entry:
          continue

        tag_key = str(tag_entry["Key"])
        tag_value = ""

        if "Value" in tag_entry and tag_entry["Value"] is not None:
          tag_value = str(tag_entry["Value"])

        tag_rows.append({"Key": tag_key, "Value": tag_value})

    return tag_rows

  def _build_instance_object(self, instance_record: _InstanceRecord) -> dict[str, typing.Any]:
    """Return one boto-shaped instance dict for DescribeInstances / RunInstances."""

    state_code = EC2_STATE_NAME_TO_CODE[instance_record.state_name]

    return {
      "InstanceId": instance_record.instance_id,
      "ImageId": instance_record.image_id,
      "InstanceType": instance_record.instance_type,
      "PlatformDetails": _resolve_platform_details(instance_record.platform_type),
      "State": {
        "Name": instance_record.state_name,
        "Code": state_code,
      },
      "Tags": [dict(tag_row) for tag_row in instance_record.tags],
    }

  def seed_instance(
    self,
    *,
    region_name: str | None = None,
    instance_id: str | None = None,
    instance_type: str,
    image_id: str,
    platform_type: str = "linux",
    state_name: str = "stopped",
    tags: list[dict[str, str]] | None = None,
  ) -> str:
    """Register one instance for tests; worker thread starts on the first lifecycle action."""

    normalized_platform = self._normalize_platform_type(
      platform_type=platform_type,
      tag_rows=tags or [],
    )

    if state_name not in EC2_STATE_NAME_TO_CODE:
      raise ValueError(
        "unsupported state_name={state_name}".format(state_name=state_name),
      )

    resolved_instance_id = instance_id or self._build_instance_id()

    instance_record = _InstanceRecord(
      instance_id=resolved_instance_id,
      instance_type=instance_type,
      image_id=image_id,
      platform_type=normalized_platform,
      state_name=state_name,
      tags=list(tags or []),
      lock=threading.Lock(),
      command_queue=queue.Queue(),
      worker_thread=None,
      launch_time=self._time_fn(),
    )

    instances_by_id = self._instances_for_region(region_name)

    with self._simulator_lock:
      if resolved_instance_id in instances_by_id:
        raise ValueError(
          "instance_id={instance_id} already exists in region={region_name}".format(
            instance_id=resolved_instance_id,
            region_name=self._resolve_region_name(region_name),
          ),
        )

      instances_by_id[resolved_instance_id] = instance_record

    return resolved_instance_id

  def run_instances(
    self,
    *,
    region_name: str | None = None,
    **keyword_arguments: typing.Any,
  ) -> dict[str, typing.Any]:
    """Launch one instance into ``pending``, then ``running`` after the platform start delay."""

    min_count = keyword_arguments.get("MinCount", 1)
    max_count = keyword_arguments.get("MaxCount", 1)

    if min_count != 1 or max_count != 1:
      raise ValueError("Ec2Simulator supports only MinCount=1 and MaxCount=1")

    image_id = keyword_arguments["ImageId"]
    instance_type = keyword_arguments["InstanceType"]
    tag_rows = self._gather_tags_from_run_instances_keyword_arguments(keyword_arguments)
    platform_type = self._normalize_platform_type(
      platform_type=keyword_arguments.get("PlatformType"),
      tag_rows=tag_rows,
    )

    instance_id = self._build_instance_id()
    instance_record = _InstanceRecord(
      instance_id=instance_id,
      instance_type=instance_type,
      image_id=image_id,
      platform_type=platform_type,
      state_name="pending",
      tags=tag_rows,
      lock=threading.Lock(),
      command_queue=queue.Queue(),
      worker_thread=None,
      launch_time=self._time_fn(),
    )

    instances_by_id = self._instances_for_region(region_name)

    with self._simulator_lock:
      instances_by_id[instance_id] = instance_record

    delay_seconds = self._start_delay_seconds(platform_type)
    self._enqueue_transition(
      instance_record,
      target_state_name="running",
      delay_seconds=delay_seconds,
    )

    instance_object = self._build_instance_object(instance_record)

    return {
      "Instances": [instance_object],
      "ReservationId": "r-{token}".format(token=secrets.token_hex(8)),
    }

  def start_instances(
    self,
    *,
    region_name: str | None = None,
    instance_ids: list[str],
  ) -> dict[str, typing.Any]:
    """Move stopped instances to ``pending``, then ``running``."""

    starting_instances: list[dict[str, typing.Any]] = []

    for instance_id in instance_ids:
      instance_record = self._require_instance(
        instance_id,
        region_name=region_name,
        operation_name="StartInstances",
      )

      with instance_record.lock:
        current_state = instance_record.state_name

        if current_state in ("running", "pending"):
          continue

        if current_state != "stopped":
          self._raise_client_error(
            operation_name="StartInstances",
            error_code="IncorrectInstanceState",
            message=(
              "The instance '{instance_id}' is not in a state from which it can be started"
            ).format(instance_id=instance_id),
          )

        instance_record.state_name = "pending"

      delay_seconds = self._start_delay_seconds(instance_record.platform_type)
      self._enqueue_transition(
        instance_record,
        target_state_name="running",
        delay_seconds=delay_seconds,
      )

      with instance_record.lock:
        starting_instances.append(
          {
            "InstanceId": instance_id,
            "CurrentState": {
              "Name": instance_record.state_name,
              "Code": EC2_STATE_NAME_TO_CODE[instance_record.state_name],
            },
            "PreviousState": {
              "Name": "stopped",
              "Code": EC2_STATE_NAME_TO_CODE["stopped"],
            },
          },
        )

    return {"StartingInstances": starting_instances}

  def stop_instances(
    self,
    *,
    region_name: str | None = None,
    instance_ids: list[str],
  ) -> dict[str, typing.Any]:
    """Move running instances to ``stopping``, then ``stopped``."""

    stopping_instances: list[dict[str, typing.Any]] = []

    for instance_id in instance_ids:
      instance_record = self._require_instance(
        instance_id,
        region_name=region_name,
        operation_name="StopInstances",
      )

      with instance_record.lock:
        current_state = instance_record.state_name

        if current_state in ("stopped", "stopping"):
          continue

        if current_state != "running":
          self._raise_client_error(
            operation_name="StopInstances",
            error_code="IncorrectInstanceState",
            message=(
              "The instance '{instance_id}' is not in a state from which it can be stopped"
            ).format(instance_id=instance_id),
          )

        previous_state_name = current_state
        instance_record.state_name = "stopping"

      delay_seconds = self._stop_delay_seconds(instance_record.platform_type)
      self._enqueue_transition(
        instance_record,
        target_state_name="stopped",
        delay_seconds=delay_seconds,
      )

      with instance_record.lock:
        stopping_instances.append(
          {
            "InstanceId": instance_id,
            "CurrentState": {
              "Name": instance_record.state_name,
              "Code": EC2_STATE_NAME_TO_CODE[instance_record.state_name],
            },
            "PreviousState": {
              "Name": previous_state_name,
              "Code": EC2_STATE_NAME_TO_CODE[previous_state_name],
            },
          },
        )

    return {"StoppingInstances": stopping_instances}

  def terminate_instances(
    self,
    *,
    region_name: str | None = None,
    instance_ids: list[str],
  ) -> dict[str, typing.Any]:
    """Enter ``shutting-down``, then ``terminated`` after the platform terminate delay."""

    terminating_instances: list[dict[str, typing.Any]] = []

    for instance_id in instance_ids:
      instance_record = self._require_instance(
        instance_id,
        region_name=region_name,
        operation_name="TerminateInstances",
      )

      with instance_record.lock:
        current_state = instance_record.state_name

        if current_state in ("shutting-down", "terminated"):
          continue

        if current_state not in ("running", "stopped"):
          self._raise_client_error(
            operation_name="TerminateInstances",
            error_code="IncorrectInstanceState",
            message=(
              "The instance '{instance_id}' is not in a state from which it can be terminated"
            ).format(instance_id=instance_id),
          )

        previous_state_name = current_state
        instance_record.state_name = "shutting-down"

      delay_seconds = self._terminate_delay_seconds(instance_record.platform_type)
      self._enqueue_transition(
        instance_record,
        target_state_name="terminated",
        delay_seconds=delay_seconds,
      )

      with instance_record.lock:
        terminating_instances.append(
          {
            "InstanceId": instance_id,
            "CurrentState": {
              "Name": instance_record.state_name,
              "Code": EC2_STATE_NAME_TO_CODE[instance_record.state_name],
            },
            "PreviousState": {
              "Name": previous_state_name,
              "Code": EC2_STATE_NAME_TO_CODE[previous_state_name],
            },
          },
        )

    return {"TerminatingInstances": terminating_instances}

  def create_tags(
    self,
    *,
    region_name: str | None = None,
    resource_ids: list[str],
    tags: list[dict[str, str]],
  ) -> None:
    """Apply tag rows to every instance in *resource_ids* (upsert by key)."""

    for resource_id in resource_ids:
      instance_record = self._require_instance(
        resource_id,
        region_name=region_name,
        operation_name="CreateTags",
      )

      with instance_record.lock:
        tag_index_by_key: dict[str, int] = {}

        for index, tag_row in enumerate(instance_record.tags):
          tag_index_by_key[tag_row["Key"]] = index

        for tag_row in tags:
          tag_key = tag_row["Key"]
          tag_value = tag_row["Value"]

          if tag_key in tag_index_by_key:
            existing_index = tag_index_by_key[tag_key]
            instance_record.tags[existing_index] = {"Key": tag_key, "Value": tag_value}
          else:
            instance_record.tags.append({"Key": tag_key, "Value": tag_value})
            tag_index_by_key[tag_key] = len(instance_record.tags) - 1

  def _instance_matches_filters(
    self,
    instance_record: _InstanceRecord,
    filters: list[dict[str, typing.Any]],
  ) -> bool:
    """Return whether *instance_record* satisfies every filter row."""

    for filter_row in filters:
      filter_name = filter_row["Name"]
      filter_values = filter_row["Values"]

      if not isinstance(filter_values, list):
        raise ValueError("filter Values must be a list")

      if filter_name == "instance-id":
        if instance_record.instance_id not in filter_values:
          return False

        continue

      if filter_name == "instance-state-name":
        if instance_record.state_name not in filter_values:
          return False

        continue

      if filter_name.startswith("tag:"):
        tag_key = filter_name.split(":", 1)[1]
        tag_value_by_key: dict[str, str] = {}

        for tag_row in instance_record.tags:
          tag_value_by_key[tag_row["Key"]] = tag_row["Value"]

        if tag_key not in tag_value_by_key:
          return False

        if tag_value_by_key[tag_key] not in filter_values:
          return False

        continue

      raise ValueError(
        "unsupported DescribeInstances filter Name={filter_name}".format(
          filter_name=filter_name,
        ),
      )

    return True

  def describe_instances(
    self,
    *,
    region_name: str | None = None,
    instance_ids: list[str] | None = None,
    filters: list[dict[str, typing.Any]] | None = None,
  ) -> dict[str, typing.Any]:
    """Return Reservations/Instances for optional id list and supported filters."""

    matched_instance_objects: list[dict[str, typing.Any]] = []
    instances_by_id = self._instances_for_region(region_name)

    with self._simulator_lock:
      instance_records = list(instances_by_id.values())

    for instance_record in instance_records:
      if instance_ids is not None:
        if instance_record.instance_id not in instance_ids:
          continue

      if filters is not None and len(filters) > 0:
        if not self._instance_matches_filters(instance_record, filters):
          continue

      with instance_record.lock:
        matched_instance_objects.append(self._build_instance_object(instance_record))

    if len(matched_instance_objects) < 1:
      return {"Reservations": []}

    return {
      "Reservations": [
        {
          "Instances": matched_instance_objects,
        },
      ],
    }

  def gather_instance_state_name(
    self,
    instance_id: str,
    *,
    region_name: str | None = None,
  ) -> str:
    """Return the current ``State.Name`` for one instance (test helper)."""

    instance_record = self._require_instance(
      instance_id,
      region_name=region_name,
      operation_name="DescribeInstances",
    )

    with instance_record.lock:
      return instance_record.state_name

  def wait_for_instance_state(
    self,
    instance_id: str,
    state_name: str,
    *,
    region_name: str | None = None,
    timeout_seconds: float = 30.0,
    poll_interval_seconds: float = 0.01,
  ) -> None:
    """Poll until *instance_id* reaches *state_name* or *timeout_seconds* elapses."""

    deadline = self._time_fn() + timeout_seconds

    while self._time_fn() < deadline:
      current_state_name = self.gather_instance_state_name(
        instance_id,
        region_name=region_name)

      if current_state_name == state_name:
        return

      self._sleep_fn(poll_interval_seconds)

    raise TimeoutError(
      "instance {instance_id} did not reach state {state_name} within {timeout_seconds}s".format(
        instance_id=instance_id,
        state_name=state_name,
        timeout_seconds=timeout_seconds,
      ),
    )
