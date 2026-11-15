# Validation evidence

Prepared November 15, 2026.

- **3 pytest tests passed** using Python 3.9.6.
- Persistent event replay, stale source versions, conflicting events, poison-message quarantine, SQL failure propagation, and FastAPI queries. The local service demo was executed.
- Kafka and PostgreSQL containers were not started; the PostgreSQL adapter and Compose topology require their integration environment.

Source Python and YAML configurations were parsed. Docker and Java were unavailable on the preparation machine; a parsed configuration is not an executed deployment. The documented Python 3.11 containers are supplied for reproducible integration checks and were not run here.

## Reproduce

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[test]"
pytest -q
```

See the README for optional full-stack commands and `reports/validation-summary.json` for the recorded local result.
