"""Regional partition behavior for AppStream simulator and fake client."""

import creatify_as.appstream.fake_client
import creatify_as.appstream.simulator


def test_seed_fleet_same_name_in_two_regions_is_isolated() -> None:
  simulator = creatify_as.appstream.simulator.AppStreamSimulator()
  fleet_name = "staging.same-name"

  simulator.seed_fleet(region_name="us-east-1", fleet_name=fleet_name, state="RUNNING")
  simulator.seed_fleet(region_name="eu-central-1", fleet_name=fleet_name, state="STOPPED")

  east_client = creatify_as.appstream.fake_client.FakeAppStreamClient(simulator, region_name="us-east-1")
  west_client = creatify_as.appstream.fake_client.FakeAppStreamClient(simulator, region_name="eu-central-1")

  east_fleets = east_client.describe_fleets(Names=[fleet_name])["Fleets"]
  central_fleets = west_client.describe_fleets(Names=[fleet_name])["Fleets"]

  assert east_fleets[0]["State"] == "RUNNING"
  assert central_fleets[0]["State"] == "STOPPED"
