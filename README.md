# Microservices Observability Platform

A lightweight observability backend that collects metrics, traces, and logs from microservices — the three pillars of observability. Includes alerting with on-call routing.

## The Three Pillars

| Pillar | What | Tool Equivalent |
|---|---|---|
| Metrics | Aggregated numbers over time (req/sec, error rate, latency p99) | Prometheus |
| Traces | Full request journey across services | Jaeger / Zipkin |
| Logs | Discrete events with context | ELK Stack |

## Architecture

```
Services → SDK (metrics + traces + logs)
                ↓
         Collector API (FastAPI)
                ↓
    ┌──────────┬──────────┐
 TimescaleDB  PostgreSQL  S3
 (metrics)    (traces)   (logs archive)
                ↓
         Alert Evaluator (runs every 30s)
                ↓
         PagerDuty / Slack / Email
```

## Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Metrics storage | TimescaleDB (PostgreSQL extension) | Time-series optimized, SQL queryable |
| Trace format | OpenTelemetry | Industry standard, vendor-neutral |
| Alert evaluation | Pull model (evaluate on schedule) | Simpler than push, easier to replay |
| Aggregation | Pre-compute 1m/5m/1h rollups | Dashboard queries stay fast at scale |

## Running Locally

```bash
docker-compose up -d
pip install -r requirements.txt
uvicorn app.main:app --reload
# Dashboard at localhost:8000/dashboard
```

## What to implement next

- [ ] Service dependency map (infer from trace data)
- [ ] Anomaly detection on metrics using Z-score
- [ ] SLO/SLA tracking (99.9% uptime = max 8.7 hours downtime/year)
- [ ] Trace sampling (record 1% of normal, 100% of errors)

## Interview Talking Points

**"What's the difference between monitoring and observability?"**
Monitoring tells you WHEN something is broken (threshold alerts). Observability lets you figure out WHY — by correlating metrics, traces, and logs. A system is observable if you can answer arbitrary questions about its internal state from its external outputs.

**"How does distributed tracing work?"**
Each request gets a unique trace_id at entry. Every service propagates it in headers. Each service creates a "span" with its processing time and metadata. The tracing backend stitches spans into a full trace — you can see exactly where 500ms request spent its time.

**"What's your alerting strategy?"**
Alert on symptoms, not causes. "Error rate > 1%" (symptom) is better than "CPU > 80%" (cause). Use multi-window alerts: fire if error rate > 1% for 5 minutes AND trending up — reduces false positives from transient spikes.
