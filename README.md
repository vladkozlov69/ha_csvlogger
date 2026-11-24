# CSVLogger

CSVLogger periodically writes Home Assistant template values to CSV files.

## YAML Configuration

Configure `csvlogger` in `configuration.yaml` with:

- `time_interval`: sample interval in seconds; defaults to `60`.
- `tables`: output tables, each with a `file_path`, `file_pattern`, and `columns`.
- `columns`: each column has a CSV `name` and a Jinja `template`.

Output patterns use `strftime`. The Home Assistant configuration repository contains the active
example configuration and host-specific paths.

## Services

- `csvlogger.flush` closes currently open CSV files.
- `csvlogger.reload` re-reads and validates CSVLogger's YAML configuration, then reloads the
  integration without restarting Home Assistant.

## Development

CSVLogger is maintained as a standalone repository and is included in the Home Assistant
configuration repository as a Git submodule. Use the shared development environment documented in
the main repository's `DEV_README`, then run:

```sh
.venv/bin/python -m pytest custom_components/csvlogger/tests
```

The integration's tests can also run from this repository's root with:

```sh
python -m pytest
```
