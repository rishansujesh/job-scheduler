SHELL := /bin/bash -eu -o pipefail

.PHONY: up down build seed verify runs demo logs admin-lag admin-pending admin-requeue smoke

up:
\tdocker compose up -d --build

down:
\tdocker compose down -v

build:
\tdocker compose build

seed:
\tbash scripts/seed.sh

verify:
\tbash scripts/verify_step11.sh

runs:
\tbash scripts/list_all_runs.sh

demo: up seed verify runs

logs:
\tdocker compose logs -f --since=2m api scheduler worker

admin-lag:
\tdocker compose run --rm admin lag

admin-pending:
\tdocker compose run --rm admin pending --stream jobs:retry || true; \\
\tdocker compose run --rm admin pending --stream jobs:scheduled || true; \\
\tdocker compose run --rm admin pending --stream jobs:adhoc || true

admin-requeue:
\tdocker compose run --rm admin requeue-dlq --count 10

smoke:
\tbash scripts/smoke.sh
