.PHONY: install test run down compose-up compose-down scenario

install:
	python3 -m pip install -r requirements-dev.txt

test:
	python3 -m pytest -q

run:
	./scripts/live_run.sh up

down:
	./scripts/live_run.sh down

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v

scenario:
	python3 scripts/live_scenario.py
