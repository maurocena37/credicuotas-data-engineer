# Setup

This challenge runs **locally** with Docker — you do **not** need a Databricks
account. Everything is Spark in `local[*]` mode.

## Option A — Docker (recommended)

Pre-requisites: Docker + Docker Compose.

```bash
make build      # build the image (Python + JDK + PySpark)
make run        # run the pipeline end-to-end (bronze -> silver -> gold)
make shell      # drop into a shell in the container (for iterating)
make clean      # remove generated output/
```

Or without make:

```bash
docker compose build
docker compose run --rm pipeline python -m src.pipeline
```

## Option B — Local Python

Pre-requisites: Python 3.10+ and a JDK 11/17 (`java -version`).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m src.pipeline
```

## What ships

```
credicuotas-data-engineer/
├── README.md            # the challenge brief — read this first
├── SETUP.md             # this file
├── Makefile             # build / run / shell / clean
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── data/                # raw input extracts (CSV) + the generator (for transparency)
│   ├── customers.csv
│   ├── loan_applications.csv
│   ├── disbursements.csv
│   ├── installments.csv
│   └── generate_data.py
└── src/
    └── pipeline.py      # a minimal runnable scaffold — bronze works, silver/gold are TODO
```

The scaffold is deliberately thin: bronze ingestion already works so you can run it
on minute one, and `silver`/`gold` are stubs for you to implement. You are free to
**restructure anything** — split modules, add a config layer, swap the runner, add
tests, etc. The scaffold is a starting point, not a constraint.

## Reference date

The delinquency metric uses a **fixed reference date of `2026-01-15`** so results are
deterministic regardless of when you run it. It's defined in `src/pipeline.py`.

## Deliverable

Push your solution to the repo we shared with you (a branch or `main` is fine), make
sure `make run` works from a clean checkout, and include your technical write-up and
your **AI usage log**.
