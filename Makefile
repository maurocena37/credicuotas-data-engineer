.PHONY: build run shell data test clean

build:
	docker compose build

run:
	docker compose run --rm pipeline python -m src.pipeline

test:
	docker compose run --rm pipeline pytest -v

shell:
	docker compose run --rm pipeline bash

# Regenerate the synthetic dataset (deterministic; not normally needed)
data:
	docker compose run --rm pipeline python data/generate_data.py

clean:
	rm -rf output/ src/__pycache__ .pytest_cache
