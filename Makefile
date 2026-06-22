.PHONY: install demo test clean show-catalog

install:
	python -m pip install --upgrade pip setuptools wheel
	pip install -e ".[dev]"

demo:
	python pipeline.py --input-dir data/landing/batch_demo --output-dir data/object_store --force

test:
	pytest -v

show-catalog:
	python scripts/query_catalog.py --db data/object_store/metadata.db --limit 20

clean:
	rm -rf data/object_store data/object_store_validate_only data/catalog.db .pytest_cache
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
