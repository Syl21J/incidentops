# IncidentOps contribution rules

- Keep all tracked code, names, comments, docstrings, log messages, and public documentation in English.
- Keep `realisation.md` in French and ignored by Git.
- Never delete Docker volumes without explicit authorization.
- Never run destructive Docker commands, including `docker compose down -v`, `docker system prune`, or `docker volume prune`.
- Never use Docker with `--privileged`.
- Always run `docker compose config` before starting services after a Compose change.
- Inspect health checks and relevant service logs before fixing a failed service.
- Never remove or modify containers or volumes outside this project.
- Never delete a complete Elasticsearch index without explicit authorization.
- Never execute a raw Elasticsearch query produced at runtime by an LLM.
- Preserve the enforced time-window and result-size limits in log searches.
- Never add high-cardinality Prometheus labels such as event, order, run, or exception values.
- Never expose unrestricted PromQL to an LLM; preserve metric allowlists and query bounds.
- Never delete the `prometheus_data` volume or other Prometheus storage without explicit authorization.
- Keep all incident injection disabled by default.
- Keep services simple, typed, testable, and limited to the current requirement.
- Do not add unrelated services or begin LangGraph, Grafana, agents, RAG, MCP, Kubernetes, or additional incident scenarios without an explicit request.
