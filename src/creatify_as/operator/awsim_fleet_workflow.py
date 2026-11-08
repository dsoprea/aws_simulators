"""Simulated WorkSpaces Applications fleet workflow for awsim_fleet_create."""

import datetime
import hashlib
import json
import os
import sys
import time
import typing

from botocore.exceptions import ClientError

import creatify_as.appstream.fake_client
import creatify_as.appstream.simulator

AWSIM_IMPORT_AGENT_SOFTWARE_VERSION = "CURRENT_LATEST"

AWSIM_IAM_ROLE_ARN_ENVIRONMENT_VARIABLE_NAME = "AWSIM_IAM_ROLE_ARN"
AWSIM_IAM_ROLE_NAME_ENVIRONMENT_VARIABLE_NAME = "AWSIM_IAM_ROLE_NAME"
AWSIM_LAUNCH_ENV_FILEPATH_ENVIRONMENT_VARIABLE_NAME = "AWSIM_LAUNCH_ENV_FILEPATH"
AWSIM_SUBNET_IDS_ENVIRONMENT_VARIABLE_NAME = "AWSIM_SUBNET_IDS"
AWSIM_SECURITY_GROUP_IDS_ENVIRONMENT_VARIABLE_NAME = "AWSIM_SECURITY_GROUP_IDS"
LAUNCH_ENV_FILEPATH_ENVIRONMENT_VARIABLE_NAME = "LAUNCH_ENV_FILEPATH"
DEPLOYMENT_ENVIRONMENT_ENVIRONMENT_VARIABLE_NAME = "DEPLOYMENT_ENVIRONMENT"
LAUNCH_ENV_PUBLIC_SUBNET_IDS_BY_AZ_KEY = "PUBLIC_SUBNET_IDS_BY_AZ"
LAUNCH_ENV_SECURITY_GROUP_IDS_SPACE_SEPARATED_KEY = "SECURITY_GROUP_IDS_SPACE_SEPARATED"
LAUNCH_ENV_SUBNET_ID_KEY = "SUBNET_ID"
DEFAULT_AWA_AMI_IMPORT_IAM_ROLE_NAME_SUFFIX = "creatify-awa-ami-import"
STAGING_RESOURCE_NAME_PREFIX = "staging-"
CREATE_FLEET_MAX_SUBNET_COUNT = 3
SIMULATOR_DEFAULT_ACCOUNT_ID = "123456789012"
AWSIM_SIMULATOR_ACCOUNT_ID_ENVIRONMENT_VARIABLE_NAME = "AWSIM_SIMULATOR_ACCOUNT_ID"
DEFAULT_SIMULATOR_SUBNET_IDS = (
  "subnet-awsim-1a",
  "subnet-awsim-1b",
  "subnet-awsim-1c",
)
DEFAULT_SIMULATOR_SECURITY_GROUP_IDS = ("sg-awsim-default",)

IMAGE_POLL_SLEEP_SECONDS = 15.0
FLEET_POLL_SLEEP_SECONDS = 15.0
DEFAULT_IMAGE_WAIT_MAX_SECONDS = 3600
DEFAULT_FLEET_WAIT_MAX_SECONDS = 3600

DEFAULT_APP_CATALOG_CONFIG = [
  {
    "Name": "Desktop",
    "DisplayName": "Desktop",
    "IconURL": "https://d1kt5ahx4gsr3.cloudfront.net/appstream/Fortnite/Icon.png",
    "LaunchPath": "C:\\Windows\\explorer.exe",
    "WorkingDirectory": "C:\\Windows",
  },
]


def resolve_simulator_source_ami_id(ami_name_or_id: str) -> str:
  """Return a synthetic EC2 AMI ID for simulator import (no EC2 API calls)."""

  if ami_name_or_id.startswith("ami-"):
    return ami_name_or_id

  digest_hex = hashlib.sha256(ami_name_or_id.encode("utf-8")).hexdigest()[:12]

  return "ami-sim{digest_hex}".format(digest_hex=digest_hex)


def build_awsim_resource_suffix() -> str:
  """Return a short unique suffix for awsim image and fleet names."""

  timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")

  return timestamp


def parse_comma_separated_identifiers(raw_value: str) -> list[str]:
  """Split a comma-separated subnet or security group list."""

  identifiers = [segment.strip() for segment in raw_value.split(",")]

  return [identifier for identifier in identifiers if len(identifier) > 0]


def build_default_awa_iam_role_name() -> str:
  """Return the Creatify default AMI import / fleet IAM role name for the active account."""

  deployment_environment = os.environ.get(
    DEPLOYMENT_ENVIRONMENT_ENVIRONMENT_VARIABLE_NAME,
    "production",
  )
  resource_name_prefix = ""

  if deployment_environment == "staging":
    resource_name_prefix = STAGING_RESOURCE_NAME_PREFIX

  return "{resource_name_prefix}{iam_role_name_suffix}".format(
    resource_name_prefix=resource_name_prefix,
    iam_role_name_suffix=DEFAULT_AWA_AMI_IMPORT_IAM_ROLE_NAME_SUFFIX,
  )


def resolve_simulator_account_id() -> str:
  """Return the simulated AWS account ID for synthetic ARNs."""

  if AWSIM_SIMULATOR_ACCOUNT_ID_ENVIRONMENT_VARIABLE_NAME in os.environ:
    return os.environ[AWSIM_SIMULATOR_ACCOUNT_ID_ENVIRONMENT_VARIABLE_NAME].strip()

  return SIMULATOR_DEFAULT_ACCOUNT_ID


def resolve_iam_role_arn(
  *,
  explicit_iam_role_arn: str | None = None,
  account_id: str | None = None,
) -> str:
  """Return an IAM role ARN from CLI flag, env, or a synthetic Creatify-default role ARN."""

  if explicit_iam_role_arn is not None and len(explicit_iam_role_arn.strip()) > 0:
    return explicit_iam_role_arn.strip()

  if AWSIM_IAM_ROLE_ARN_ENVIRONMENT_VARIABLE_NAME in os.environ:
    return os.environ[AWSIM_IAM_ROLE_ARN_ENVIRONMENT_VARIABLE_NAME].strip()

  if AWSIM_IAM_ROLE_NAME_ENVIRONMENT_VARIABLE_NAME in os.environ:
    iam_role_name = os.environ[AWSIM_IAM_ROLE_NAME_ENVIRONMENT_VARIABLE_NAME].strip()
  else:
    iam_role_name = build_default_awa_iam_role_name()

  resolved_account_id = account_id
  if resolved_account_id is None:
    resolved_account_id = resolve_simulator_account_id()

  return "arn:aws:iam::{account_id}:role/{iam_role_name}".format(
    account_id=resolved_account_id,
    iam_role_name=iam_role_name,
  )


def build_default_simulator_vpc_config() -> dict[str, list[str]]:
  """Return built-in subnet and security group IDs for simulator fleet create."""

  return {
    "SubnetIds": list(DEFAULT_SIMULATOR_SUBNET_IDS),
    "SecurityGroupIds": list(DEFAULT_SIMULATOR_SECURITY_GROUP_IDS),
  }


def _parse_launch_env_double_quoted_fragment(value_raw: str, line_number: int) -> str:
  """Recover inner text from launch-env ``"…"`` values (shell-style escapes)."""

  if not value_raw.startswith('"') or len(value_raw) < 2:
    raise ValueError(
      "launch env: line {line_number}: internal double-quoted fragment parse expected quoted value".format(
        line_number=line_number,
      ),
    )

  character_index = 1
  char_buffer: list[str] = []

  while character_index < len(value_raw):
    character = value_raw[character_index]

    if character == "\\":
      if character_index + 1 >= len(value_raw):
        raise ValueError(
          "launch env: line {line_number}: incomplete escape in double-quoted value".format(
            line_number=line_number,
          ),
        )

      follower = value_raw[character_index + 1]

      if follower in "\\\"":
        char_buffer.append(follower)
      elif follower == "n":
        char_buffer.append("\n")
      elif follower == "r":
        char_buffer.append("\r")
      else:
        raise ValueError(
          "launch env: line {line_number}: unknown escape \\{follower}".format(
            line_number=line_number,
            follower=follower,
          ),
        )

      character_index = character_index + 2

      continue

    if character == '"':
      if character_index != len(value_raw) - 1:
        raise ValueError(
          "launch env: line {line_number}: garbage after closing quote in value".format(
            line_number=line_number,
          ),
        )

      return "".join(char_buffer)

    char_buffer.append(character)
    character_index = character_index + 1

  raise ValueError(
    "launch env: line {line_number}: unterminated double-quoted value".format(
      line_number=line_number,
    ),
  )


def _extract_launch_env_value_token(value_raw: str, line_number: int) -> str:
  """Turn the raw ``=value`` remainder into decoded UTF-8 text."""

  clipped = value_raw.rstrip("\r")

  if clipped == "":
    return ""

  if clipped[0] == '"':
    return _parse_launch_env_double_quoted_fragment(clipped, line_number)

  return clipped


def parse_launch_parameters_from_body_text(full_text: str) -> dict[str, str]:
  """Parse ``launch.env``-shaped payloads into normalized ``KEY→str`` mappings."""

  launch_parameters: dict[str, str] = {}
  body_line_rows = full_text.splitlines()

  for line_counter, recorded_line in enumerate(body_line_rows, 1):
    normalized_line = recorded_line.rstrip("\r\n")

    if not normalized_line.strip() or normalized_line.lstrip().startswith("#"):
      continue

    if "=" not in normalized_line:
      raise ValueError(
        "launch env: line {line_counter}: expected KEY=value".format(
          line_counter=line_counter,
        ),
      )

    key_fragment, _, tail_fragment = normalized_line.partition("=")
    stripped_key = key_fragment.strip()
    launch_parameters[stripped_key] = _extract_launch_env_value_token(
      tail_fragment,
      line_counter,
    )

  return launch_parameters


def load_launch_parameters_from_env_file_path(launch_env_filepath: str) -> dict[str, str]:
  """Read one ``launch.env`` export file into launch-parameter mappings."""

  with open(launch_env_filepath, encoding="utf-8", newline="\n") as env_file:
    env_file_text = env_file.read()

  return parse_launch_parameters_from_body_text(env_file_text)


def _validate_public_subnet_ids_by_az_blob(map_blob: str) -> dict[str, object]:
  """Reject empty, malformed, or all-blank ``PUBLIC_SUBNET_IDS_BY_AZ`` export JSON."""

  stripped_map_blob = map_blob.strip()

  if not stripped_map_blob:
    raise ValueError(
      "PUBLIC_SUBNET_IDS_BY_AZ must be nonempty JSON in launch.env "
      "(object mapping each availability zone name to a subnet id).",
    )

  try:
    deserialized_plaintext = json.loads(stripped_map_blob)
  except json.JSONDecodeError as json_decode_error:
    raise ValueError(
      "PUBLIC_SUBNET_IDS_BY_AZ must be valid JSON (got {map_blob!r})".format(
        map_blob=stripped_map_blob,
      ),
    ) from json_decode_error

  if not isinstance(deserialized_plaintext, dict):
    raise ValueError(
      "PUBLIC_SUBNET_IDS_BY_AZ must be a JSON object mapping AZ names to subnet ids; "
      "got type {type_name}".format(type_name=type(deserialized_plaintext).__name__),
    )

  nonempty_subnet_value_count = 0

  for availability_zone in deserialized_plaintext.keys():
    subnet_token = str(deserialized_plaintext[availability_zone]).strip()

    if subnet_token:
      nonempty_subnet_value_count = nonempty_subnet_value_count + 1

  if nonempty_subnet_value_count == 0:
    raise ValueError(
      "PUBLIC_SUBNET_IDS_BY_AZ must list at least one nonempty subnet id "
      "(JSON keys are availability zone names).",
    )

  return deserialized_plaintext


def derive_ordered_subnet_candidates(launch_parameters: dict[str, str]) -> list[str]:
  """Prefer ``SUBNET_ID`` then alphabetical AZ-key subnets from ``PUBLIC_SUBNET_IDS_BY_AZ``."""

  ordered: list[str] = []
  primary_subnet = launch_parameters.get(LAUNCH_ENV_SUBNET_ID_KEY, "").strip()

  if primary_subnet:
    ordered.append(primary_subnet)

  map_blob = launch_parameters[LAUNCH_ENV_PUBLIC_SUBNET_IDS_BY_AZ_KEY]
  deserialized_plaintext = _validate_public_subnet_ids_by_az_blob(map_blob)
  subnet_identifier_values_from_availability_zone_object: set[str] = set()
  availability_zone_keys_sorted = sorted(deserialized_plaintext.keys())

  for availability_zone in availability_zone_keys_sorted:
    subnet_token = str(deserialized_plaintext[availability_zone]).strip()

    if subnet_token:
      subnet_identifier_values_from_availability_zone_object.add(subnet_token)

    if subnet_token and subnet_token not in ordered:
      ordered.append(subnet_token)

  if primary_subnet:
    if primary_subnet not in subnet_identifier_values_from_availability_zone_object:
      raise ValueError(
        "SUBNET_ID={primary_subnet!r} must appear as a subnet id value inside PUBLIC_SUBNET_IDS_BY_AZ".format(
          primary_subnet=primary_subnet,
        ),
      )

  if not ordered:
    raise ValueError("SUBNET discovery produced an empty subnet list")

  return ordered


def gather_security_group_identifiers_from_launch_parameters(
  launch_parameters: dict[str, str],
) -> list[str]:
  """Split ``SECURITY_GROUP_IDS_SPACE_SEPARATED`` like image-windows-lambda shell tooling."""

  combined = launch_parameters[LAUNCH_ENV_SECURITY_GROUP_IDS_SPACE_SEPARATED_KEY].replace(",", " ")
  split_security_token_rows = combined.split()
  groups = [token for token in split_security_token_rows if token]

  if not groups:
    raise ValueError("SECURITY_GROUP_IDS_SPACE_SEPARATED yielded zero identifiers")

  return groups


def build_vpc_config_from_launch_parameters(
  launch_parameters: dict[str, str],
) -> dict[str, list[str]]:
  """Build AppStream ``VpcConfig`` from Creatify ``launch.env`` exports."""

  subnet_ids = derive_ordered_subnet_candidates(launch_parameters)
  subnet_ids = subnet_ids[:CREATE_FLEET_MAX_SUBNET_COUNT]
  security_group_ids = gather_security_group_identifiers_from_launch_parameters(launch_parameters)

  return {
    "SubnetIds": subnet_ids,
    "SecurityGroupIds": security_group_ids,
  }


def resolve_launch_env_filepath() -> str | None:
  """Return a launch.env filepath when ``AWSIM_LAUNCH_ENV_FILEPATH`` or ``LAUNCH_ENV_FILEPATH`` is set."""

  for environment_variable_name in (
    AWSIM_LAUNCH_ENV_FILEPATH_ENVIRONMENT_VARIABLE_NAME,
    LAUNCH_ENV_FILEPATH_ENVIRONMENT_VARIABLE_NAME,
  ):
    if environment_variable_name in os.environ:
      launch_env_filepath = os.environ[environment_variable_name].strip()

      if not os.path.isfile(launch_env_filepath):
        raise ValueError(
          "{environment_variable_name}={launch_env_filepath} is not a readable file".format(
            environment_variable_name=environment_variable_name,
            launch_env_filepath=launch_env_filepath,
          ),
        )

      return launch_env_filepath

  return None


def load_vpc_config_from_environment() -> dict[str, list[str]]:
  """Return ``VpcConfig`` from ``AWSIM_SUBNET_IDS`` and ``AWSIM_SECURITY_GROUP_IDS`` only."""

  if AWSIM_SUBNET_IDS_ENVIRONMENT_VARIABLE_NAME not in os.environ:
    raise ValueError(
      "{environment_variable_name} must be set to a comma-separated list of subnet IDs".format(
        environment_variable_name=AWSIM_SUBNET_IDS_ENVIRONMENT_VARIABLE_NAME,
      ),
    )

  if AWSIM_SECURITY_GROUP_IDS_ENVIRONMENT_VARIABLE_NAME not in os.environ:
    raise ValueError(
      "{environment_variable_name} must be set to a comma-separated list of security group IDs".format(
        environment_variable_name=AWSIM_SECURITY_GROUP_IDS_ENVIRONMENT_VARIABLE_NAME,
      ),
    )

  return {
    "SubnetIds": parse_comma_separated_identifiers(
      os.environ[AWSIM_SUBNET_IDS_ENVIRONMENT_VARIABLE_NAME],
    ),
    "SecurityGroupIds": parse_comma_separated_identifiers(
      os.environ[AWSIM_SECURITY_GROUP_IDS_ENVIRONMENT_VARIABLE_NAME],
    ),
  }


def resolve_vpc_config() -> dict[str, list[str]]:
  """Return AppStream ``VpcConfig`` from env overrides, optional launch.env file, or simulator defaults."""

  has_subnet_override = AWSIM_SUBNET_IDS_ENVIRONMENT_VARIABLE_NAME in os.environ
  has_security_group_override = AWSIM_SECURITY_GROUP_IDS_ENVIRONMENT_VARIABLE_NAME in os.environ

  if has_subnet_override or has_security_group_override:
    if not has_subnet_override or not has_security_group_override:
      raise ValueError(
        "set both {subnet_variable_name} and {security_group_variable_name}, or neither".format(
          subnet_variable_name=AWSIM_SUBNET_IDS_ENVIRONMENT_VARIABLE_NAME,
          security_group_variable_name=AWSIM_SECURITY_GROUP_IDS_ENVIRONMENT_VARIABLE_NAME,
        ),
      )

    return load_vpc_config_from_environment()

  launch_env_filepath = resolve_launch_env_filepath()

  if launch_env_filepath is not None:
    sys.stderr.write(
      "resolved vpc from launch env filepath={launch_env_filepath}\n".format(
        launch_env_filepath=launch_env_filepath,
      ),
    )
    launch_parameters = load_launch_parameters_from_env_file_path(launch_env_filepath)

    return build_vpc_config_from_launch_parameters(launch_parameters)

  sys.stderr.write("using built-in simulator VpcConfig (subnet-awsim-*, sg-awsim-default)\n")

  return build_default_simulator_vpc_config()


def start_imported_image(
  appstream_client: typing.Any,
  *,
  image_name: str,
  source_ami_id: str,
  iam_role_arn: str,
) -> None:
  """Call ``CreateImportedImage`` for one EC2 AMI."""

  appstream_client.create_imported_image(
    Name=image_name,
    SourceAmiId=source_ami_id,
    IamRoleArn=iam_role_arn,
    AgentSoftwareVersion=AWSIM_IMPORT_AGENT_SOFTWARE_VERSION,
    AppCatalogConfig=DEFAULT_APP_CATALOG_CONFIG,
    Tags={
      "CREATED_BY": "awsim_fleet_create",
    },
  )


def poll_image_until_state(
  appstream_client: typing.Any,
  *,
  image_name: str,
  target_state: str,
  max_wait_seconds: int,
  poll_sleep_seconds: float = IMAGE_POLL_SLEEP_SECONDS,
) -> dict[str, typing.Any]:
  """Poll ``describe_images`` until *image_name* reaches *target_state*."""

  poll_started_at = time.monotonic()

  while True:
    describe_images_response = appstream_client.describe_images(Names=[image_name])
    image_objects = describe_images_response["Images"]

    if len(image_objects) < 1:
      raise ValueError("image {image_name} disappeared during import poll".format(image_name=image_name))

    image_object = image_objects[0]
    image_state = image_object["State"]
    elapsed_seconds = int(time.monotonic() - poll_started_at)

    sys.stderr.write(
      "image {image_name} state={image_state} target={target_state} ({elapsed_seconds}s / max {max_wait_seconds}s)\n".format(
        image_name=image_name,
        image_state=image_state,
        target_state=target_state,
        elapsed_seconds=elapsed_seconds,
        max_wait_seconds=max_wait_seconds,
      ),
    )

    if image_state == target_state:
      return image_object

    if image_state == "FAILED":
      raise RuntimeError(
        "image import failed image_name={image_name} image={image_object!r}".format(
          image_name=image_name,
          image_object=image_object,
        ),
      )

    if elapsed_seconds >= max_wait_seconds:
      raise TimeoutError(
        "image {image_name} did not reach state={target_state} within {max_wait_seconds}s".format(
          image_name=image_name,
          target_state=target_state,
          max_wait_seconds=max_wait_seconds,
        ),
      )

    time.sleep(poll_sleep_seconds)


def create_and_start_on_demand_fleet(
  appstream_client: typing.Any,
  *,
  fleet_name: str,
  image_name: str,
  instance_type: str,
  desired_instances: int,
  iam_role_arn: str,
  vpc_config: dict[str, list[str]],
) -> dict[str, typing.Any]:
  """Create one ON_DEMAND fleet and call ``StartFleet``."""

  create_fleet_response = appstream_client.create_fleet(
    Name=fleet_name,
    DisplayName=fleet_name,
    ImageName=image_name,
    InstanceType=instance_type,
    FleetType="ON_DEMAND",
    ComputeCapacity={"DesiredInstances": desired_instances},
    EnableDefaultInternetAccess=True,
    StreamView="APP",
    VpcConfig=vpc_config,
    IamRoleArn=iam_role_arn,
    Tags={
      "CREATED_BY": "awsim_fleet_create",
    },
  )

  appstream_client.start_fleet(Name=fleet_name)

  return create_fleet_response["Fleet"]


def poll_fleet_until_state(
  appstream_client: typing.Any,
  *,
  fleet_name: str,
  target_state: str,
  max_wait_seconds: int,
  poll_sleep_seconds: float = FLEET_POLL_SLEEP_SECONDS,
) -> dict[str, typing.Any]:
  """Poll ``describe_fleets`` until *fleet_name* reaches *target_state*."""

  poll_started_at = time.monotonic()

  while True:
    describe_fleets_response = appstream_client.describe_fleets(Names=[fleet_name])
    fleet_objects = describe_fleets_response["Fleets"]

    if len(fleet_objects) < 1:
      raise ValueError("fleet {fleet_name} disappeared during poll".format(fleet_name=fleet_name))

    fleet_object = fleet_objects[0]
    fleet_state = fleet_object["State"]
    elapsed_seconds = int(time.monotonic() - poll_started_at)
    capacity_summary = ""

    if "ComputeCapacityStatus" in fleet_object:
      compute_capacity_status = fleet_object["ComputeCapacityStatus"]
      capacity_summary = (
        " running={running}/{desired} available={available}".format(
          running=compute_capacity_status.get("Running", "?"),
          desired=compute_capacity_status.get("Desired", "?"),
          available=compute_capacity_status.get("Available", "?"),
        )
      )

    sys.stderr.write(
      "fleet {fleet_name} state={fleet_state} target={target_state}{capacity_summary} "
      "({elapsed_seconds}s / max {max_wait_seconds}s)\n".format(
        fleet_name=fleet_name,
        fleet_state=fleet_state,
        target_state=target_state,
        capacity_summary=capacity_summary,
        elapsed_seconds=elapsed_seconds,
        max_wait_seconds=max_wait_seconds,
      ),
    )

    if fleet_state == target_state:
      return fleet_object

    if elapsed_seconds >= max_wait_seconds:
      raise TimeoutError(
        "fleet {fleet_name} did not reach state={target_state} within {max_wait_seconds}s".format(
          fleet_name=fleet_name,
          target_state=target_state,
          max_wait_seconds=max_wait_seconds,
        ),
      )

    time.sleep(poll_sleep_seconds)


def stop_fleet_and_wait(
  appstream_client: typing.Any,
  *,
  fleet_name: str,
  max_wait_seconds: int,
) -> dict[str, typing.Any]:
  """Stop one fleet and poll until ``STOPPED``."""

  appstream_client.stop_fleet(Name=fleet_name)

  return poll_fleet_until_state(
    appstream_client,
    fleet_name=fleet_name,
    target_state="STOPPED",
    max_wait_seconds=max_wait_seconds,
  )


def delete_fleet_and_image(
  appstream_client: typing.Any,
  *,
  fleet_name: str,
  image_name: str,
) -> list[str]:
  """Delete fleet and image; return human-readable cleanup lines for stderr."""

  cleanup_messages: list[str] = []

  try:
    appstream_client.delete_fleet(Name=fleet_name)
    cleanup_messages.append("deleted fleet name={fleet_name}".format(fleet_name=fleet_name))
  except ClientError as client_error:
    cleanup_messages.append(
      "delete_fleet failed fleet_name={fleet_name} error={error}".format(
        fleet_name=fleet_name,
        error=client_error.response["Error"]["Code"],
      ),
    )
    raise

  try:
    appstream_client.delete_image(Name=image_name)
    cleanup_messages.append("deleted image name={image_name}".format(image_name=image_name))
  except ClientError as client_error:
    cleanup_messages.append(
      "delete_image failed image_name={image_name} error={error}".format(
        image_name=image_name,
        error=client_error.response["Error"]["Code"],
      ),
    )
    raise

  return cleanup_messages


def build_simulated_appstream_client(
  *,
  region_name: str,
  be_quick: bool,
  account_id: str | None = None,
) -> creatify_as.appstream.fake_client.FakeAppStreamClient:
  """Return one ``FakeAppStreamClient`` backed by ``AppStreamSimulator`` (no live AWS)."""

  resolved_account_id = account_id
  if resolved_account_id is None:
    resolved_account_id = resolve_simulator_account_id()

  simulator_config = creatify_as.appstream.simulator.AppStreamSimulatorConfig(
    region_name=region_name,
    account_id=resolved_account_id,
    be_quick=be_quick,
  )
  appstream_simulator = creatify_as.appstream.simulator.AppStreamSimulator(
    config=simulator_config,
  )

  return creatify_as.appstream.fake_client.FakeAppStreamClient(
    appstream_simulator,
    region_name=region_name,
  )
