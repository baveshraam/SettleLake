PY ?= python

setup:
	$(PY) -m pip install -e ".[dev]"

lint:
	ruff check src tests && ruff format --check src tests

test:
	pytest --cov=settlelake --cov-report=term-missing

demo:
	settlelake generate --profile quick --seed 42
	settlelake run
	settlelake report

benchmark:
	settlelake benchmark --payments 100000 --seed 42

dashboard:
	settlelake dashboard
