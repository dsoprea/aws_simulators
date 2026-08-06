# creatify_as

In-memory AWS service simulators for Creatify tests. Each module provides:

1. A **semantic simulator** — domain behavior without boto or consumer imports.
2. A **boto-shaped fake client** (where applicable) — maps AWS parameter names to the simulator.
3. **Standalone pytest coverage** in this package — behavior is proven here before Creatify wires it in.

This package is vendored at [`vendor/internal/creatify_aws_simulators/`](.) and installed as a path dependency from [`lambda/instance_lifecycle/project/pyproject.toml`](../../../lambda/instance_lifecycle/project/pyproject.toml).

## What this is (and is not)

| | **creatify_as** | **Generic AWS emulator** (LocalStack, full boto stub, etc.) |
|---|-----------------|-------------------------------------------------------------|
| Goal | Honest semantics for **Creatify code paths under test** | API-complete service replacement |
| Scope | Curated methods and expression forms | All operations, edge cases, IAM, quotas |
| Config | Consumer supplies table/index metadata (Dynamo) via glue | Discovers or mocks everything |
| Dependencies | `boto3`, `botocore` only | Varies |

**DynamoDB:** `DynamoDbSimulator` is a **purpose-built in-memory store** with transact support for AWA-shaped workloads. It is fully implemented for those paths; it is **not** a generic DynamoDB emulator.

**AppStream:** `AppStreamSimulator` models fleet/stack lifecycle, associations, sessions, and streaming URLs for AWA utilities. It is **not** a complete AppStream 2.0 service.

---

## Design rules

1. **No `creatify_lambda` imports** — table names, GSIs, and production helpers live in the consumer repo.
2. **Simulator first** — add behavior to `*.simulator` modules; keep `fake_client` as a thin boto adapter.
3. **Test in vendor** — every new semantic behavior gets a pytest in `tests/` before Creatify integration tests rely on it.
4. **Fail loud on unsupported syntax** — raise `ValueError` or `ClientError` rather than silently accepting unknown Dynamo expressions or AppStream calls.
5. **Virtual time by default in tests** — pass `time_fn` (and optionally `sleep_fn`) so polls and delays do not sleep in CI.

---

## Install and run vendor tests

```bash
cd vendor/internal/creatify_aws_simulators
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest -q
```

From **instance_lifecycle** (pulls `creatify_as` via dev dependency):

```bash
cd lambda/instance_lifecycle/project
pip install -e ".[dev]"
./script/test.sh          # unit pytest, then -m integration AWA tests
```

---

## Package layout

```text
src/creatify_as/
  appstream/
    simulator.py      # AppStreamSimulator, AppStreamSimulatorConfig
    fake_client.py    # FakeAppStreamClient (boto-shaped)
  dynamodb/
    simulator.py      # DynamoDbSimulator, DynamoDbSimulatorConfig
tests/
  appstream/test_simulator.py
  dynamodb/test_dynamodb_simulator.py
```

There is **no** `FakeDynamoDbClient`. Creatify patches high-level `creatify_lambda.aws.dynamodb` helpers to delegate to a configured `DynamoDbSimulator` subclass (glue adapter pattern below).

---

## AppStream module

### Quick start

```python
import creatify_as.appstream.fake_client
import creatify_as.appstream.simulator

simulator = creatify_as.appstream.simulator.AppStreamSimulator(
  config=creatify_as.appstream.simulator.AppStreamSimulatorConfig(
    fleet_state_transition_delay_seconds=0.05,
    stack_state_transition_delay_seconds=0.01,
  ),
  time_fn=lambda: 0.0,  # virtual clock — no real sleeps
)

client = creatify_as.appstream.fake_client.FakeAppStreamClient(simulator)

simulator.seed_fleet(fleet_name="staging.my-fleet")
simulator.seed_stack(stack_name="staging.my-stack")
client.associate_fleet(FleetName="staging.my-fleet", StackName="staging.my-stack")
```

### `AppStreamSimulatorConfig`

| Field | Default | Role |
|-------|---------|------|
| `region_name` | `us-east-1` | ARN construction and `FakeAppStreamClient.meta.region_name` |
| `account_id` | `123456789012` | ARN construction |
| `fleet_state_transition_delay_seconds` | `0.05` | `create_fleet` / `start_fleet` → `RUNNING` |
| `stack_state_transition_delay_seconds` | `0.01` | `create_stack` → `AVAILABLE` |
| `streaming_url_validity_seconds` | `3600` | Default validity hint |

### Virtual clock

When `time_fn` is provided (or `sleep_fn` is overridden), the simulator uses a **virtual clock**:

- `advance_time(seconds)` — jump time and apply scheduled fleet/stack transitions.
- Poll helpers in consumer code should use the same simulator instance and advance time in tests instead of sleeping.

### Key semantics (test-critical)

- **One stack per fleet** — second `associate_fleet` with a different stack raises `LimitExceededException` (matches AWA SCTI regression).
- **Idempotent associate** — re-associating the same fleet/stack pair is a no-op.
- **Distinct streaming URLs** per `user_id`.
- **State machine** — fleets move `STOPPED` → `RUNNING`; stacks `PENDING` → `AVAILABLE` after configured delays.

### `FakeAppStreamClient` coverage

Methods delegate to `AppStreamSimulator` with boto parameter names (`FleetName`, `StackName`, …):

| Method | Notes |
|--------|--------|
| `associate_fleet` / `disassociate_fleet` | One-stack-per-fleet enforcement |
| `list_associated_stacks` / `list_associated_fleets` | `NextToken` returns empty (no pagination) |
| `create_streaming_url` | Returns `StreamingURL` + `Expires` |
| `describe_fleets` / `describe_stacks` | Filter by `Names` |
| `create_fleet` / `start_fleet` / `stop_fleet` / `delete_fleet` | Lifecycle + delays |
| `create_stack` / `delete_stack` | Stack lifecycle |
| `describe_sessions` / `expire_session` | Session tracking |
| `tag_resource` / `untag_resource` / `list_tags_for_resource` | Tags by ARN |
| `describe_images` | Uses `seed_image` data |
| `update_fleet` | Subset of fields used by AWA |

### Test helpers on simulator

| Method | Purpose |
|--------|---------|
| `reset()` | Clear all state between tests |
| `seed_fleet(...)` | Insert fleet without API call |
| `seed_stack(...)` | Insert stack without API call |
| `seed_image(...)` | Insert describe_images row |

### Creatify integration

Production code uses `AwaAppStreamInterface`. Integration tests use [`MockAwaAppStream`](../../../lambda/instance_lifecycle/project/tests/integration/awa/awa_appstream_mock.py) — subclasses `BotoAwaAppStream` and returns `FakeAppStreamClient` per region from one shared simulator.

**Legacy unit tests:** [`tests/aws/appstream/`](../../../lambda/instance_lifecycle/project/tests/aws/appstream/) still use inline `_FakeAppStreamClient` classes. Migrate them to `FakeAppStreamClient` + shared fixture (see follow-up plan Phase A).

---

## DynamoDB module

### Quick start (vendor tests)

Vendor tests build a **hardcoded** `DynamoDbSimulatorConfig` (see [`tests/dynamodb/test_dynamodb_simulator.py`](tests/dynamodb/test_dynamodb_simulator.py)). Consumers bind real table names from their config module.

```python
import creatify_as.dynamodb.simulator

config = creatify_as.dynamodb.simulator.DynamoDbSimulatorConfig(
  allowed_table_names=frozenset({"my_table"}),
  table_primary_key_attribute_names_by_table_name={"my_table": ("id",)},
  gsi_key_attribute_names_by_index_name={"gsi_name": ("name", None)},
)
store = creatify_as.dynamodb.simulator.DynamoDbSimulator(config)
store.seed_item("my_table", {"id": "row-1", "name": "example"})
row = store.get_item_or_none("my_table", {"id": "row-1"})
```

### `DynamoDbSimulatorConfig`

| Field | Role |
|-------|------|
| `allowed_table_names` | Reject unknown `TableName` values |
| `table_primary_key_attribute_names_by_table_name` | PK attributes per table (simple or composite) |
| `gsi_key_attribute_names_by_index_name` | `(partition_key, sort_key_or_none)` per GSI |

### Public API (`DynamoDbSimulator`)

| Method | Role |
|--------|------|
| `clear()` | Drop all rows (test isolation) |
| `seed_item(table, item)` | Insert row without condition checks |
| `get_item_or_none` | Point read |
| `put_item` | Put with optional condition |
| `update_item` | Update with `UpdateExpression` subset |
| `delete_item` | Delete with optional condition |
| `query_collect_items` | Query PK/GSI with boto3 `Key` conditions |
| `query_first_item_or_none` | First match or `None` |
| `scan_collect_items` / `scan_all_items` | Table scan (filter subset) |
| `transact_write_items` | Atomic multi-item writes; `TransactionCanceledException` on condition failure |

Items are stored as **Python dicts** (deserialized), not low-level Dynamo attribute maps, at the simulator boundary. `transact_write_items` accepts **low-level** transact entries (`Put`, `Update`, `Delete`, `ConditionCheck`) like production boto transact builders.

### Supported expression subset

**Condition expressions**

- `attribute_exists` / `attribute_not_exists`
- Comparisons: `=`, `<>`, `<`, `<=`, `>`, `>=` (numeric coercion for inequalities)
- `AND` between clauses

**Update expressions**

- `SET` assignments: literal `:value`, `#attr = #other - :n`, `#attr = #other + :n`
- `REMOVE` attribute list
- Combined `SET ... REMOVE ...`

Unsupported forms raise `ValueError` with the expression text — add support in vendor when a real test needs it.

**Query key conditions**

- boto3 `Key(...).eq(...)` and `Key(...).begins_with(...)` via `KeyConditionExpression`
- GSI name passed as `IndexName`

### Creatify glue adapter (consumer-owned)

Creatify does **not** import table names into `creatify_as`. Binding lives in:

| File | Role |
|------|------|
| [`awa_dynamodb_glue.py`](../../../lambda/instance_lifecycle/project/tests/integration/awa/awa_dynamodb_glue.py) | `AwaInMemoryDynamo(DynamoDbSimulator)` + `build_awa_in_memory_dynamo()` using `creatify_lambda.config.dynamodb` |
| [`awa_dynamodb_patch.py`](../../../lambda/instance_lifecycle/project/tests/integration/awa/awa_dynamodb_patch.py) | Patches `creatify_lambda.aws.dynamodb.*` to delegate to glue instance |
| [`conftest.py`](../../../lambda/instance_lifecycle/project/tests/integration/awa/conftest.py) | Integration-only autouse patch |

AWA tables wired in glue: `awa_fleets`, `awa_stacks`, `awa_associations_student`, `awa_associations_student_queue`, `awa_fleets_queue`, `awa_stacks_queue`, `awa_log_items`, plus GSIs from config.

---

## End-to-end flow in Creatify AWA integration tests

```text
@pytest.mark.integration test
    │
    ├─► awa_appstream fixture → MockAwaAppStream → FakeAppStreamClient → AppStreamSimulator
    │
    └─► autouse patch → creatify_lambda.aws.dynamodb.* → AwaInMemoryDynamo → DynamoDbSimulator
```

Seed data: [`seed_helpers.py`](../../../lambda/instance_lifecycle/project/tests/integration/awa/seed_helpers.py).

Non-AWA dependencies (students, schools, SCTI, URLs) remain **per-test monkeypatches** in the consumer repo — not part of `creatify_as`.

---

## Adding a new AWS service module

1. Create `src/creatify_as/<service>/simulator.py` with config dataclass + simulator class.
2. Add `fake_client.py` if production code expects boto clients.
3. Add `tests/<service>/` with pytest covering each semantic rule you implement.
4. Export from `src/creatify_as/<service>/__init__.py` if needed.
5. In the consumer repo: glue module (config binding), patch layer, fixtures, integration tests.

**Keep in vendor:** state machines, error codes, atomicity, expression evaluation for that service.

**Keep in consumer:** table names, ARNs tied to Creatify config, patching production entrypoints, seed builders, E2E scenarios.

---

## Extending an existing simulator

### When to change `creatify_as`

- New AppStream API used by multiple tests and mapping is generic (e.g. `wait_for_*` polling behavior).
- New Dynamo condition/update operator required by production transact builders — **add a failing vendor test first**.
- Bug in simulated semantics (wrong error code, non-atomic transact).

### When to change only the consumer

- New AWA table or GSI → update `awa_dynamodb_glue.py` and `config/dynamodb.py`, not vendor.
- New optional `awa_appstream` parameter on a utility → production + integration test only.
- Stubbing schools/students/SSM/Route53 → integration test monkeypatch.

### Workflow

```bash
# 1. Vendor
cd vendor/internal/creatify_aws_simulators && .venv/bin/python -m pytest -q

# 2. Consumer glue / integration
cd lambda/instance_lifecycle/project
./script/test.sh

# 3. Default unit suite (integration excluded)
.venv/bin/python -m pytest -q
```

---

## Limitations (intentional)

**AppStream**

- No multi-region isolation (one simulator; clients may set `region_name` on meta only).
- Pagination tokens return empty follow-up pages.
- No IAM, service quotas, or throttling.
- Not all boto AppStream operations exist — add to `simulator.py` + `fake_client.py` when needed.

**DynamoDB**

- No `FakeDynamoDbClient` / botocore client hook — patch high-level helpers or call simulator methods directly.
- No streams, TTL, batch_get/write, PartiQL, transactions beyond `transact_write_items`.
- Expression parser is regex-based and limited; unknown syntax raises `ValueError`.
- No cross-table enforcement beyond transact atomicity in one call.
- Scan/query filters only support paths exercised in tests.

**General**

- Simulators do not replace staging smoke tests for IAM and regional behavior.

---

## Versioning and dependency

- Package name: `creatify_as` (PyPI name if published later).
- Version: see [`pyproject.toml`](pyproject.toml) (`0.1.0` at time of writing).
- Python: `>=3.12`.
- Runtime deps: `boto3`, `botocore`.

Bump `version` in `pyproject.toml` when vendor semantics change in a way consumers must notice; reinstall consumer venv after vendor edits (`pip install -e ".[dev]"`).

---

## Related documentation

| Document | Content |
|----------|---------|
| [AWA Phase 2+ phased plan](file:///home/dustin/.cursor/plans/master-dc6-cfc5d8e6c462/AWA%20Phase%202%20Phased-646e9d78.plan.md) | Shipped Phases 1–8 (`92089532`) |
| [AWA simulation follow-up plan](file:///home/dustin/.cursor/plans/AWA%20simulation%20follow-up-646e9d78.plan.md) | Legacy test migration, cleanup_used integration, optional extensions |
| [`lambda/instance_lifecycle/project/script/test.sh`](../../../lambda/instance_lifecycle/project/script/test.sh) | Unit then integration test runner |

---

## Checklist for contributors

- [ ] New behavior has a test in `vendor/internal/creatify_aws_simulators/tests/`
- [ ] No import of `creatify_lambda` or other app packages
- [ ] Simulator method documented with one-line docstring
- [ ] `fake_client` method added if boto callers need it
- [ ] Consumer glue/patch updated if table set or patch surface changes
- [ ] `./script/test.sh` or documented pytest commands pass
