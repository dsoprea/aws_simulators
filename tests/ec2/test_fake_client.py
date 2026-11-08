"""Unit tests for boto-shaped FakeEc2Client."""

import creatify_as.ec2.fake_client
import creatify_as.ec2.simulator


def test_fake_client_maps_boto_parameter_names() -> None:
  recorded_sleep_delays: list[float] = []

  simulator = creatify_as.ec2.simulator.Ec2Simulator(
    config=creatify_as.ec2.simulator.Ec2SimulatorConfig(be_quick=True),
    sleep_fn=recorded_sleep_delays.append,
  )
  client = creatify_as.ec2.fake_client.FakeEc2Client(simulator)

  run_response = client.run_instances(
    ImageId="ami-test",
    InstanceType="t3.small",
    MinCount=1,
    MaxCount=1,
  )

  instance_id = run_response["Instances"][0]["InstanceId"]

  simulator.wait_for_instance_state(instance_id, "running", timeout_seconds=1.0, poll_interval_seconds=0.0)

  client.stop_instances(InstanceIds=[instance_id])
  simulator.wait_for_instance_state(instance_id, "stopped", timeout_seconds=1.0, poll_interval_seconds=0.0)

  client.create_tags(
    Resources=[instance_id],
    Tags=[{"Key": "ENVIRONMENT", "Value": "staging"}],
  )

  describe_response = client.describe_instances(InstanceIds=[instance_id])

  assert len(client.run_instances_calls) == 1
  assert len(client.create_tags_calls) == 1
  assert describe_response["Reservations"][0]["Instances"][0]["Tags"] == [
    {"Key": "ENVIRONMENT", "Value": "staging"},
  ]
