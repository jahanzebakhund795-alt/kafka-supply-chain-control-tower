# Kafka Supply Chain Control Tower

A newly built portfolio implementation for Jahanzeb Akhund, aligned with the data architecture and engineering work described in his resume. All example data is synthetic. This repository is independent sample work; it does not contain employer code, claim a historical deployment, or reproduce resume performance metrics.

## Implemented system

A Kafka producer emits versioned shipment events keyed by shipment ID. A consumer validates each event, commits the event ledger and latest shipment state in one SQL transaction, and advances its Kafka offset only afterward. Poison messages go to a durable SQL rejection ledger. A FastAPI query service reads warehouse aggregates and shipment state. The store supports PostgreSQL for deployment and SQLite for deterministic local tests.

[Architecture diagram and design review](docs/ARCHITECTURE.md)

## Run

Use Python 3.11 for the documented development environment.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[test]"
pytest -q
python -m supplychain.demo
cp .env.example .env
# Edit .env, then:
docker compose up --build -d kafka postgres worker api
docker compose run --rm seed
curl http://localhost:8000/warehouses
```

## Design and operating limits

At-least-once Kafka delivery with an idempotent SQL sink, not distributed exactly-once transactions. Event IDs must remain stable on replay. Source versions determine ordering; event timestamps are retained but do not close event-time windows. A single-node broker is a development topology. API endpoints are read-only and require an authentication gateway before exposure. Tests exercise SQLite persistence, replay, failures, and HTTP endpoints; Kafka/PostgreSQL Compose checks require Docker.

See [the operating guide](docs/OPERATIONS.md) for recovery, deployment, and validation boundaries. An optional GitHub Actions definition is in `docs/ci-template.yml`; enabling it requires installing it under `.github/workflows/` with workflow permission.

## References

- [Confluent Python consumer](https://docs.confluent.io/kafka-clients/python/current/index.html)
- [FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/)

## Executed validation

3 pytest tests passed. Persistent event replay, stale source versions, conflicting events, poison-message quarantine, SQL failure propagation, and FastAPI queries. The local service demo was executed.

[Full validation evidence and limits](VALIDATION.md)

[Browse all 10 advanced portfolio repositories](PORTFOLIO.md).
