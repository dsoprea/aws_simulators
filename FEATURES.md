# creatify_as — features

In-memory AWS simulators for Creatify tests. Install and pytest commands live in [README.md](README.md).

## Modules

| Module | Capabilities |
|--------|----------------|
| **AppStream** | Fleet/stack/image lifecycle (worker threads + `be_quick`), capacity-aware streaming URLs, sessions, tagging, optional EC2 fleet backing |
| **DynamoDB** | Regional table partitions; in-memory query/scan subset, transact writes; `FakeDynamoDbClient` |
| **EC2** | Per-thread instance lifecycle: `RunInstances`, start/stop/terminate, describe, `create_tags`; linux/windows timings and `be_quick` mode |

## AppStream lifecycle (summary)

- Fleets, stacks, and image imports transition on per-resource daemon threads via `sleep_fn` (same pattern as EC2).
- Default delays when `be_quick=False`: fleet start **300s**, stop **60s**; stack create **120s**; image import **300s**. `be_quick=True` uses **10s** for each.
- `ComputeCapacityStatus` tracks `InUse` from active sessions; `create_streaming_url` requires `RUNNING` and available capacity.
- Optional `ec2_simulator=` provisions Windows EC2 instances when a fleet reaches `RUNNING`.

## EC2 lifecycle (summary)

- States follow AWS order: `pending`, `running`, `stopping`, `stopped`, `shutting-down`, `terminated`.
- Platform-specific delays for start, stop, and terminate; `be_quick=True` uses 10s segments for tests.
- Regional fake clients (`FakeEc2Client`, `FakeAppStreamClient`, `FakeDynamoDbClient`) default to `us-east-1` and scope inventory per region.
- Boto-shaped entrypoint: `FakeEc2Client` delegates to `Ec2Simulator`.
