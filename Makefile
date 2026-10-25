# Start/stop local Airflow. `up` passes your user id as AIRFLOW_UID so files the
# pipeline writes into the repo (data/, logs/) stay owned by you on Linux.
AIRFLOW_UID := $(shell id -u)

.PHONY: up down

up:  # build and start Airflow; UI at http://localhost:8080
	AIRFLOW_UID=$(AIRFLOW_UID) docker compose up -d --build

down:  # stop Airflow (data in Postgres is kept)
	docker compose down
