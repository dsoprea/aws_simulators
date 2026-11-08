"""Simulated import, fleet launch, and teardown using the in-memory AppStream simulator."""

import argparse
import os
import sys

import creatify_as.operator.awsim_fleet_workflow


def build_argument_parser() -> argparse.ArgumentParser:
  """Return the ``awsim_fleet_create`` argument parser."""

  parser = argparse.ArgumentParser(
    prog="awsim_fleet_create",
    description=(
      "Run a simulated WorkSpaces Applications workflow: import an AMI label, "
      "start an ON_DEMAND fleet in AppStreamSimulator, and delete fleet plus image after Enter."
    ),
  )
  parser.add_argument(
    "ami_name_or_id",
    metavar="AMI_NAME_OR_ID",
    help="Synthetic EC2 AMI ID (ami-…) or image name (mapped to ami-sim… for import)",
  )
  parser.add_argument(
    "instance_type",
    metavar="INSTANCE_TYPE",
    help="AppStream instance type (e.g. stream.standard.medium)",
  )
  parser.add_argument(
    "instance_count",
    metavar="INSTANCE_COUNT",
    type=int,
    help="DesiredInstances for the fleet",
  )
  parser.add_argument(
    "--region",
    default=None,
    help="Simulator region label (default: AWS_REGION or us-east-1)",
  )
  parser.add_argument(
    "--iam-role-arn",
    default=None,
    metavar="IAM_ROLE_ARN",
    help=(
      "IamRoleArn passed to CreateImportedImage / CreateFleet "
      "(default: synthetic ARN for creatify-awa-ami-import, or AWSIM_IAM_ROLE_ARN)"
    ),
  )
  parser.add_argument(
    "--image-name",
    default=None,
    help="AppStream image name (default: awsim-<timestamp>-<ami suffix>)",
  )
  parser.add_argument(
    "--fleet-name",
    default=None,
    help="Fleet name (default: awsim-fleet-<timestamp>)",
  )
  parser.add_argument(
    "--image-wait-max-seconds",
    type=int,
    default=creatify_as.operator.awsim_fleet_workflow.DEFAULT_IMAGE_WAIT_MAX_SECONDS,
    help="Max seconds to wait for image import",
  )
  parser.add_argument(
    "--fleet-wait-max-seconds",
    type=int,
    default=creatify_as.operator.awsim_fleet_workflow.DEFAULT_FLEET_WAIT_MAX_SECONDS,
    help="Max seconds to wait for fleet start/stop",
  )
  parser.add_argument(
    "--slow",
    action="store_true",
    help="Use full simulator delays (default: be_quick, 10s transitions)",
  )
  parser.add_argument(
    "--skip-teardown",
    action="store_true",
    help="Exit after fleet RUNNING without waiting for Enter or deleting resources",
  )

  return parser


def resolve_region_name(parsed_arguments: argparse.Namespace) -> str:
  """Return the effective simulator region name."""

  if parsed_arguments.region is not None:
    return parsed_arguments.region

  if "AWS_REGION" in os.environ:
    return os.environ["AWS_REGION"]

  return "us-east-1"


def main(argv: list[str] | None = None) -> int:
  """Run the simulated import, fleet launch, interactive stop, and cleanup workflow."""

  argument_parser = build_argument_parser()
  parsed_arguments = argument_parser.parse_args(argv)

  if parsed_arguments.instance_count < 1:
    sys.stderr.write("error: INSTANCE_COUNT must be at least 1\n")
    return 2

  region_name = resolve_region_name(parsed_arguments)
  be_quick = not parsed_arguments.slow
  vpc_config = creatify_as.operator.awsim_fleet_workflow.resolve_vpc_config()
  account_id = creatify_as.operator.awsim_fleet_workflow.resolve_simulator_account_id()
  appstream_client = creatify_as.operator.awsim_fleet_workflow.build_simulated_appstream_client(
    region_name=region_name,
    be_quick=be_quick,
    account_id=account_id,
  )
  iam_role_arn = creatify_as.operator.awsim_fleet_workflow.resolve_iam_role_arn(
    explicit_iam_role_arn=parsed_arguments.iam_role_arn,
    account_id=account_id,
  )

  source_ami_id = creatify_as.operator.awsim_fleet_workflow.resolve_simulator_source_ami_id(
    parsed_arguments.ami_name_or_id,
  )
  resource_suffix = creatify_as.operator.awsim_fleet_workflow.build_awsim_resource_suffix()
  ami_suffix = source_ami_id.replace("ami-", "")[-8:]

  image_name = parsed_arguments.image_name
  if image_name is None:
    image_name = "awsim-{suffix}-{ami_suffix}".format(
      suffix=resource_suffix,
      ami_suffix=ami_suffix,
    )

  fleet_name = parsed_arguments.fleet_name
  if fleet_name is None:
    fleet_name = "awsim-fleet-{suffix}".format(suffix=resource_suffix)

  sys.stderr.write(
    "using in-memory AppStream simulator region={region_name} be_quick={be_quick}\n".format(
      region_name=region_name,
      be_quick=be_quick,
    ),
  )
  sys.stderr.write(
    "region={region_name} source_ami_id={source_ami_id} image_name={image_name} "
    "fleet_name={fleet_name} instance_type={instance_type} desired_instances={instance_count}\n".format(
      region_name=region_name,
      source_ami_id=source_ami_id,
      image_name=image_name,
      fleet_name=fleet_name,
      instance_type=parsed_arguments.instance_type,
      instance_count=parsed_arguments.instance_count,
    ),
  )

  sys.stderr.write("starting CreateImportedImage…\n")
  creatify_as.operator.awsim_fleet_workflow.start_imported_image(
    appstream_client,
    image_name=image_name,
    source_ami_id=source_ami_id,
    iam_role_arn=iam_role_arn,
  )

  creatify_as.operator.awsim_fleet_workflow.poll_image_until_state(
    appstream_client,
    image_name=image_name,
    target_state="AVAILABLE",
    max_wait_seconds=parsed_arguments.image_wait_max_seconds,
  )

  sys.stderr.write("creating and starting fleet…\n")
  creatify_as.operator.awsim_fleet_workflow.create_and_start_on_demand_fleet(
    appstream_client,
    fleet_name=fleet_name,
    image_name=image_name,
    instance_type=parsed_arguments.instance_type,
    desired_instances=parsed_arguments.instance_count,
    iam_role_arn=iam_role_arn,
    vpc_config=vpc_config,
  )

  fleet_object = creatify_as.operator.awsim_fleet_workflow.poll_fleet_until_state(
    appstream_client,
    fleet_name=fleet_name,
    target_state="RUNNING",
    max_wait_seconds=parsed_arguments.fleet_wait_max_seconds,
  )

  sys.stderr.write(
    "fleet {fleet_name} is RUNNING (state={state})\n".format(
      fleet_name=fleet_name,
      state=fleet_object["State"],
    ),
  )

  if parsed_arguments.skip_teardown:
    return 0

  sys.stderr.write(
    "Press Enter to stop the fleet, wait until STOPPED, and delete fleet plus image…\n",
  )
  sys.stdin.readline()

  sys.stderr.write("stopping fleet…\n")
  creatify_as.operator.awsim_fleet_workflow.stop_fleet_and_wait(
    appstream_client,
    fleet_name=fleet_name,
    max_wait_seconds=parsed_arguments.fleet_wait_max_seconds,
  )

  sys.stderr.write("cleanup: deleting fleet and imported image…\n")
  cleanup_messages = creatify_as.operator.awsim_fleet_workflow.delete_fleet_and_image(
    appstream_client,
    fleet_name=fleet_name,
    image_name=image_name,
  )

  for cleanup_message in cleanup_messages:
    sys.stderr.write("cleanup: {cleanup_message}\n".format(cleanup_message=cleanup_message))

  sys.stderr.write("done\n")

  return 0


if __name__ == "__main__":
  sys.exit(main())
