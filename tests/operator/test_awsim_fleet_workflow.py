"""Unit tests for awsim_fleet_create workflow helpers."""

import json

import pytest

import creatify_as.operator.awsim_fleet_workflow


def test_build_default_awa_iam_role_name_uses_staging_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
  monkeypatch.setenv(
    creatify_as.operator.awsim_fleet_workflow.DEPLOYMENT_ENVIRONMENT_ENVIRONMENT_VARIABLE_NAME,
    "staging",
  )

  iam_role_name = creatify_as.operator.awsim_fleet_workflow.build_default_awa_iam_role_name()

  assert iam_role_name == "staging-creatify-awa-ami-import"


def test_resolve_iam_role_arn_builds_synthetic_arn(monkeypatch: pytest.MonkeyPatch) -> None:
  monkeypatch.delenv(
    creatify_as.operator.awsim_fleet_workflow.AWSIM_IAM_ROLE_ARN_ENVIRONMENT_VARIABLE_NAME,
    raising=False,
  )
  monkeypatch.delenv(
    creatify_as.operator.awsim_fleet_workflow.DEPLOYMENT_ENVIRONMENT_ENVIRONMENT_VARIABLE_NAME,
    raising=False,
  )

  iam_role_arn = creatify_as.operator.awsim_fleet_workflow.resolve_iam_role_arn(
    account_id="111122223333",
  )

  assert iam_role_arn == "arn:aws:iam::111122223333:role/creatify-awa-ami-import"


def test_resolve_simulator_source_ami_id_passes_through_ami_id() -> None:
  source_ami_id = creatify_as.operator.awsim_fleet_workflow.resolve_simulator_source_ami_id(
    "ami-0abc1234567890def",
  )

  assert source_ami_id == "ami-0abc1234567890def"


def test_resolve_simulator_source_ami_id_maps_image_name() -> None:
  source_ami_id_a = creatify_as.operator.awsim_fleet_workflow.resolve_simulator_source_ami_id(
    "test-ami",
  )
  source_ami_id_b = creatify_as.operator.awsim_fleet_workflow.resolve_simulator_source_ami_id(
    "test-ami",
  )

  assert source_ami_id_a == source_ami_id_b
  assert source_ami_id_a.startswith("ami-sim")


def test_resolve_vpc_config_uses_builtin_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
  monkeypatch.delenv(
    creatify_as.operator.awsim_fleet_workflow.AWSIM_SUBNET_IDS_ENVIRONMENT_VARIABLE_NAME,
    raising=False,
  )
  monkeypatch.delenv(
    creatify_as.operator.awsim_fleet_workflow.AWSIM_SECURITY_GROUP_IDS_ENVIRONMENT_VARIABLE_NAME,
    raising=False,
  )
  monkeypatch.delenv(
    creatify_as.operator.awsim_fleet_workflow.AWSIM_LAUNCH_ENV_FILEPATH_ENVIRONMENT_VARIABLE_NAME,
    raising=False,
  )
  monkeypatch.delenv(
    creatify_as.operator.awsim_fleet_workflow.LAUNCH_ENV_FILEPATH_ENVIRONMENT_VARIABLE_NAME,
    raising=False,
  )

  vpc_config = creatify_as.operator.awsim_fleet_workflow.resolve_vpc_config()

  assert vpc_config == creatify_as.operator.awsim_fleet_workflow.build_default_simulator_vpc_config()


def test_build_vpc_config_from_launch_parameters_orders_and_caps_subnets() -> None:
  launch_parameters = {
    "SUBNET_ID": "subnet-primary",
    "PUBLIC_SUBNET_IDS_BY_AZ": json.dumps(
      {
        "us-east-1a": "subnet-primary",
        "us-east-1b": "subnet-b",
        "us-east-1c": "subnet-c",
        "us-east-1d": "subnet-d",
      },
    ),
    "SECURITY_GROUP_IDS_SPACE_SEPARATED": "sg-one sg-two",
  }

  vpc_config = creatify_as.operator.awsim_fleet_workflow.build_vpc_config_from_launch_parameters(
    launch_parameters,
  )

  assert vpc_config["SubnetIds"] == ["subnet-primary", "subnet-b", "subnet-c"]
  assert vpc_config["SecurityGroupIds"] == ["sg-one", "sg-two"]


def test_resolve_vpc_config_uses_explicit_environment(monkeypatch: pytest.MonkeyPatch) -> None:
  monkeypatch.setenv(
    creatify_as.operator.awsim_fleet_workflow.AWSIM_SUBNET_IDS_ENVIRONMENT_VARIABLE_NAME,
    "subnet-aaa, subnet-bbb",
  )
  monkeypatch.setenv(
    creatify_as.operator.awsim_fleet_workflow.AWSIM_SECURITY_GROUP_IDS_ENVIRONMENT_VARIABLE_NAME,
    "sg-ccc",
  )

  vpc_config = creatify_as.operator.awsim_fleet_workflow.resolve_vpc_config()

  assert vpc_config == {
    "SubnetIds": ["subnet-aaa", "subnet-bbb"],
    "SecurityGroupIds": ["sg-ccc"],
  }


def test_parse_launch_parameters_from_body_text_handles_quoted_json() -> None:
  body_text = (
    'PUBLIC_SUBNET_IDS_BY_AZ="{\\"us-east-1a\\":\\"subnet-quoted\\"}"\n'
    "SECURITY_GROUP_IDS_SPACE_SEPARATED=sg-quoted\n"
    "SUBNET_ID=subnet-quoted\n"
  )

  launch_parameters = creatify_as.operator.awsim_fleet_workflow.parse_launch_parameters_from_body_text(
    body_text,
  )

  assert launch_parameters["SUBNET_ID"] == "subnet-quoted"
  assert launch_parameters["SECURITY_GROUP_IDS_SPACE_SEPARATED"] == "sg-quoted"
  assert '"us-east-1a"' in launch_parameters["PUBLIC_SUBNET_IDS_BY_AZ"]


def test_poll_image_until_state_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
  class _FakeAppStreamClient:
    def describe_images(self, **keyword_arguments):  # noqa: ANN003
      return {
        "Images": [
          {"Name": keyword_arguments["Names"][0], "State": "IMPORTING"},
        ],
      }

  monotonic_values = [0.0, 0.0, 100.0]

  def fake_monotonic() -> float:
    return monotonic_values.pop(0)

  monkeypatch.setattr(creatify_as.operator.awsim_fleet_workflow.time, "monotonic", fake_monotonic)
  monkeypatch.setattr(
    creatify_as.operator.awsim_fleet_workflow.time,
    "sleep",
    lambda seconds: None,
  )

  with pytest.raises(TimeoutError):
    creatify_as.operator.awsim_fleet_workflow.poll_image_until_state(
      _FakeAppStreamClient(),
      image_name="test-image",
      target_state="AVAILABLE",
      max_wait_seconds=5,
    )
