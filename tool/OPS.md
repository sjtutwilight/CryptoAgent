# Ops Script Specification

This document defines the standard interface for all operational scripts.
The only public entrypoint is `./tool/ops.sh`.

## Command Grammar

- Format: `<domain:action> [args...]`
- Domains must be lowercase ASCII with optional underscores.
- Actions must be lowercase ASCII with optional underscores.
- Parameters should use GNU-style flags where needed.

## Directory Layout

- `tool/ops.sh` is the entrypoint.
- Implementations live in `automation/ops/<domain>/`.
- Each domain provides one script per action:
  - Python: `automation/ops/<domain>/<action>.py`
  - Shell:  `automation/ops/<domain>/<action>.sh`

## Output Rules

- Default output is human-readable.
- Use `--output-json` to emit machine-readable JSON.
- Errors must return a non-zero exit code and include a clear message.

## Flink Defaults

- `flink:upload` and `flink:run` use the latest built jar in `process/aggregator/target/`.
- `flink:status` shows all jobs by default (pass a job ID to inspect one).
- `flink:cancel` cancels all running jobs by default; supports `all` or job keywords (kline, balance, token, pnl, perp).
- `flink:cancel` keywords are resolved by matching current Flink job names.

## Parameter Rules

- Keep parameters minimal; prefer fixed defaults in scripts.
- Prefer positional args for required identifiers.
- Use environment variables for infra endpoints and credentials.

## Naming Examples

- `role:start <role_id...>`
- `role:stop <role_id...>` or `role:stop all`
- `role:alive_list`, `role:task <role_id>`
- `init:schema`, `init:data`, `init:all`
- `http:get`, `http:post`
- `flink:build`, `flink:upload`, `flink:list`, `flink:run`, `flink:status`, `flink:cancel`
- `flink:job <keyword...>` (kline, balance, token, pnl, perp)
- `sqlite:query`, `sqlite:clean`
- `starrocks:query`

## Compatibility

- Do not add legacy wrappers.
- Remove deprecated commands rather than aliasing them.
