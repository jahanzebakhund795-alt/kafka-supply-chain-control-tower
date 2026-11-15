#!/bin/sh
set -eu
docker compose up --build -d kafka postgres worker api
docker compose run --rm seed
echo 'Inspect warehouse aggregates: curl http://localhost:8000/warehouses'
echo 'Replay seed, restart worker, and compare warehouse counts to verify sink idempotency.'
