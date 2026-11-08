# creatify_as — features

In-memory AWS simulators for Creatify tests. Install and pytest commands live in [README.md](README.md).

## Modules

| Module | Capabilities |
|--------|----------------|
| **AppStream** | Fleet/stack lifecycle, associations, streaming URLs, sessions, tagging |
| **DynamoDB** | Regional table partitions; in-memory query/scan subset, transact writes; `FakeDynamoDbClient` |
| **EC2** | Per-thread instance lifecycle: `RunInstances`, start/stop/terminate, describe, `create_tags`; linux/windows timings and `be_quick` mode |

## EC2 lifecycle (summary)

- States follow AWS order: `pending`, `running`, `stopping`, `stopped`, `shutting-down`, `terminated`.
- Platform-specific delays for start, stop, and terminate; `be_quick=True` uses 10s segments for tests.
- Regional fake clients (`FakeEc2Client`, `FakeAppStreamClient`, `FakeDynamoDbClient`) default to `us-east-1` and scope inventory per region.
- Boto-shaped entrypoint: `FakeEc2Client` delegates to `Ec2Simulator`.
