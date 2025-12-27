# Automation Ops

Minimal scripts to manage DataInjector roles without the test harness.

## Usage

Apply roles from a JSON payload:

```bash
python automation/ops/role_apply.py --config path/to/roles.json
```

Apply roles by role_id from config.yaml:

```bash
python automation/ops/role_apply_from_config.py --role-id binance-spot-link-kline-batch
```

Stop roles:

```bash
python automation/ops/role_stop.py --role-ids role-a,role-b
```

List roles:

```bash
python automation/ops/role_list.py
```

## Options

- `--api` DataInjector API base URL (uses HTTP directly when set).
- `--container` DataInjector container name (default: `datainjector-worker`).
- `--token` X-Worker-Token value if auth is enabled.
- `--config` JSON payload file path.
- `--json` JSON payload string.
