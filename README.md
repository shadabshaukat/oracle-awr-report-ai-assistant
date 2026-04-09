# Oracle AWR Deterministic Miner (File-Based)

Professional Oracle performance diagnostics from uploaded AWR/SQL/log artifacts using a deterministic rules engine (no AI/LLM dependency).

## Why this version

- ✅ No AI model integration
- ✅ No `.env` configuration required
- ✅ No database connection or credentials
- ✅ Rule-based AWR mining focused on Oracle performance tuning signals

## What it analyzes

- Top wait events and bottleneck categories (IO, REDO, LOCK, RAC, CONCURRENCY)
- ORA error signatures from alert/log snippets
- SQL pressure markers (full scans, nested loops stress, temp/sort spill indicators)
- Module-level status table with severity markers
- Priority recommendations and expected outcomes

## Features

- Drag-and-drop upload + file browser upload
- Professional tabular report output
- CSV export for waits/findings/module-status/recommendations
- Processing overlay while report is being mined

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```

Open: `http://localhost:8080`

## Run with Docker

```bash
docker build -t awr-deterministic-miner .
docker run --rm -p 8080:8080 awr-deterministic-miner
```

The container runs Gunicorn bound to `0.0.0.0:8080`.

## Supported files

- `.html`, `.htm`, `.txt`, `.csv`, `.log`, `.sql`
