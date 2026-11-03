# Agent instructions

## Raw data

The `data/` directory contains raw data and is immutable. Read files there as needed, but never modify, delete, rename, move, or overwrite anything in `data/`. Do not run commands or code that writes generated files or outputs into `data/`; save generated outputs elsewhere.

The raw data is CSV energy-consumption data sampled every minute from one or more devices.

When exploring or analyzing `data/`, use only `*.csv` files. Do not inspect or process compressed archives or other non-CSV data files.

## CSV structure

Each CSV has a header row and one row per minute. `timestamp` is a Unix timestamp in seconds. For each phase `a`, `b`, and `c`, the remaining phase columns follow these patterns:

- Energy: `total_act_energy`, `fund_act_energy`, `total_act_ret_energy`, `fund_act_ret_energy`, `lag_react_energy`, `lead_react_energy`
- Power: `max_act_power`, `min_act_power`, `max_aprt_power`, `min_aprt_power`
- Voltage: `max_voltage`, `min_voltage`, `avg_voltage`
- Current: `max_current`, `min_current`, `avg_current`

Each phase column is prefixed by its phase letter and an underscore (for example, `a_total_act_energy`). The three neutral-current columns are `n_max_current`, `n_min_current`, and `n_avg_current`. Do not assume measurement units unless they are established elsewhere.

## Source code overview

- `src/importer/` communicates with the Shelly energy meters. Its CLI downloads CSV exports, subscribes to live status updates, and imports CSV or live readings into InfluxDB. `src/importer/db/` converts readings to InfluxDB points and handles database writes.
- `src/analyze/` reads per-device CSV directories, combines files, removes duplicate timestamps, reports coverage and gaps, and provides Polars-based phase and energy summaries (including daily totals and weekday averages).
- `src/config.py` loads the project configuration; `src/example_config.py` shows its expected shape. `src/util.py` contains timezone-aware timestamp formatting. Tests are colocated in `*_test.py` files.
- The CSV downloader writes into the configured data directory and also creates compressed backups there. Never run it or any other operation that writes to `data/`; raw data must remain unchanged.
