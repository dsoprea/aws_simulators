"""Regional partition behavior for EC2 simulator and fake client."""

import botocore.exceptions
import pytest

import creatify_as.ec2.fake_client
import creatify_as.ec2.simulator


def test_run_instances_is_visible_only_in_client_region() -> None:
  simulator = creatify_as.ec2.simulator.Ec2Simulator(
    config=creatify_as.ec2.simulator.Ec2SimulatorConfig(be_quick=True),
    sleep_fn=lambda seconds: None,
  )
  east_client = creatify_as.ec2.fake_client.FakeEc2Client(simulator, region_name="us-east-1")
  west_client = creatify_as.ec2.fake_client.FakeEc2Client(simulator, region_name="us-west-2")

  run_response = east_client.run_instances(
    ImageId="ami-east",
    InstanceType="t3.small",
    MinCount=1,
    MaxCount=1,
  )
  instance_id = run_response["Instances"][0]["InstanceId"]

  east_describe = east_client.describe_instances(InstanceIds=[instance_id])
  west_describe = west_client.describe_instances(InstanceIds=[instance_id])

  assert len(east_describe["Reservations"]) == 1
  assert len(west_describe["Reservations"]) == 0


def test_start_instances_wrong_region_raises_not_found() -> None:
  simulator = creatify_as.ec2.simulator.Ec2Simulator(
    config=creatify_as.ec2.simulator.Ec2SimulatorConfig(be_quick=True),
    sleep_fn=lambda seconds: None,
  )
  instance_id = simulator.seed_instance(
    region_name="us-east-1",
    instance_type="t3.small",
    image_id="ami-1",
    state_name="stopped",
  )

  with pytest.raises(botocore.exceptions.ClientError) as error_info:
    simulator.start_instances(region_name="eu-central-1", instance_ids=[instance_id])

  assert error_info.value.response["Error"]["Code"] == "InvalidInstanceID.NotFound"
