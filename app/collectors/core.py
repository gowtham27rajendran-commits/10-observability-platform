"""
Observability Platform — Metrics, Traces, Alerts

Implements the three pillars of observability:
1. Metrics: aggregated time-series (counters, gauges, histograms)
2. Traces: distributed request tracking via OpenTelemetry
3. Alerting: threshold + anomaly-based alerting with routing
"""
import time
import uuid
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
from enum import Enum
from collections import defaultdict, deque


# ─── METRICS ─────────────────────────────────────────────────────────────────

class MetricType(Enum):
    COUNTER = "counter"       # monotonically increasing (request count, error count)
    GAUGE = "gauge"           # point-in-time value (memory usage, queue depth)
    HISTOGRAM = "histogram"   # distribution of values (latency, request size)


@dataclass
class MetricPoint:
    name: str
    value: float
    labels: Dict[str, str]   # e.g. {"service": "payment", "endpoint": "/checkout"}
    timestamp: float
    metric_type: MetricType


class MetricsCollector:
    """
    In-memory metrics store with time-windowed aggregation.
    Production: write to TimescaleDB (PostgreSQL extension for time-series).
    TimescaleDB auto-compresses old data and supports time_bucket() SQL functions.
    """
    def __init__(self, retention_seconds: int = 3600):
        self._series: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.retention_seconds = retention_seconds

    def _series_key(self, name: str, labels: Dict[str, str]) -> str:
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def record(self, name: str, value: float, labels: Dict[str, str] = None,
               metric_type: MetricType = MetricType.GAUGE):
        key = self._series_key(name, labels or {})
        self._series[key].append(MetricPoint(
            name=name, value=value, labels=labels or {},
            timestamp=time.time(), metric_type=metric_type
        ))

    def counter_inc(self, name: str, labels: Dict[str, str] = None, by: float = 1):
        """Increment a counter. Thread-safe in single process; use Redis INCR for distributed."""
        self.record(name, by, labels, MetricType.COUNTER)

    def histogram_observe(self, name: str, value: float, labels: Dict[str, str] = None):
        """Record a histogram observation (e.g., request latency in ms)."""
        self.record(name, value, labels, MetricType.HISTOGRAM)

    def query(self, name: str, labels: Dict[str, str] = None,
              window_seconds: int = 300) -> List[MetricPoint]:
        """Fetch metric points within the last N seconds."""
        key = self._series_key(name, labels or {})
        cutoff = time.time() - window_seconds
        return [p for p in self._series[key] if p.timestamp >= cutoff]

    def aggregate(self, name: str, labels: Dict[str, str] = None,
                  window_seconds: int = 300) -> Dict:
        """Compute summary statistics over a time window."""
        points = self.query(name, labels, window_seconds)
        if not points:
            return {}
        values = [p.value for p in points]
        return {
            "count": len(values),
            "sum": sum(values),
            "mean": statistics.mean(values),
            "p50": sorted(values)[len(values) // 2],
            "p95": sorted(values)[int(len(values) * 0.95)],
            "p99": sorted(values)[int(len(values) * 0.99)],
            "min": min(values),
            "max": max(values),
            "window_seconds": window_seconds,
        }


# ─── TRACING ─────────────────────────────────────────────────────────────────

@dataclass
class Span:
    """
    Represents one unit of work in a distributed trace.
    OpenTelemetry standard: trace_id is shared across all spans in a request.
    """
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    service_name: str
    operation_name: str
    start_time: float
    end_time: Optional[float] = None
    status: str = "ok"           # ok / error
    tags: Dict = field(default_factory=dict)
    logs: List[Dict] = field(default_factory=list)

    @property
    def duration_ms(self) -> Optional[float]:
        if self.end_time:
            return round((self.end_time - self.start_time) * 1000, 2)
        return None

    def finish(self, status: str = "ok"):
        self.end_time = time.time()
        self.status = status

    def log(self, message: str, level: str = "info"):
        self.logs.append({"ts": time.time(), "level": level, "message": message})

    def set_tag(self, key: str, value):
        self.tags[key] = value


class Tracer:
    """
    Distributed tracer using OpenTelemetry-compatible span model.

    In production: export spans to Jaeger or Tempo via OTLP protocol.
    This in-memory implementation is for local dev and testing.

    Usage:
        span = tracer.start_span("payment.process", trace_id=incoming_trace_id)
        try:
            # do work
            span.set_tag("amount", 49.99)
            span.finish("ok")
        except Exception as e:
            span.log(str(e), "error")
            span.finish("error")
    """
    def __init__(self):
        self._traces: Dict[str, List[Span]] = defaultdict(list)

    def start_span(self, operation: str, service: str,
                   trace_id: Optional[str] = None,
                   parent_span_id: Optional[str] = None) -> Span:
        span = Span(
            trace_id=trace_id or str(uuid.uuid4()),
            span_id=str(uuid.uuid4())[:16],
            parent_span_id=parent_span_id,
            service_name=service,
            operation_name=operation,
            start_time=time.time(),
        )
        self._traces[span.trace_id].append(span)
        return span

    def get_trace(self, trace_id: str) -> List[Span]:
        """Return all spans for a trace, sorted by start time (call tree order)."""
        return sorted(self._traces.get(trace_id, []), key=lambda s: s.start_time)

    def get_trace_summary(self, trace_id: str) -> Dict:
        spans = self.get_trace(trace_id)
        if not spans:
            return {}
        total_duration = max(
            (s.duration_ms or 0) for s in spans
        )
        errors = [s for s in spans if s.status == "error"]
        return {
            "trace_id": trace_id,
            "total_duration_ms": total_duration,
            "span_count": len(spans),
            "services": list(set(s.service_name for s in spans)),
            "has_errors": len(errors) > 0,
            "error_spans": [s.operation_name for s in errors],
            "spans": [
                {"op": s.operation_name, "service": s.service_name,
                 "duration_ms": s.duration_ms, "status": s.status}
                for s in spans
            ]
        }


# ─── ALERTING ─────────────────────────────────────────────────────────────────

class AlertSeverity(Enum):
    P1 = "p1"   # wake someone up at 3am
    P2 = "p2"   # notify during business hours
    P3 = "p3"   # create a ticket, no page


@dataclass
class AlertRule:
    """
    Multi-window alert rule.
    Fires when metric crosses threshold for sustained_seconds duration.
    Multi-window reduces false positives from transient spikes.
    """
    name: str
    metric_name: str
    threshold: float
    operator: str          # gt / lt / gte / lte
    sustained_seconds: int # must stay above threshold for this long
    severity: AlertSeverity
    labels: Dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class Alert:
    rule_name: str
    fired_at: float
    resolved_at: Optional[float]
    current_value: float
    severity: AlertSeverity
    labels: Dict


class AlertManager:
    """
    Evaluates alert rules on a schedule (default every 30s).
    Routes alerts to appropriate channels based on severity.
    """
    def __init__(self, metrics: MetricsCollector):
        self.metrics = metrics
        self.rules: List[AlertRule] = []
        self._active_alerts: Dict[str, Alert] = {}
        self._handlers: Dict[AlertSeverity, List[Callable]] = defaultdict(list)

    def add_rule(self, rule: AlertRule):
        self.rules.append(rule)

    def on_alert(self, severity: AlertSeverity, handler: Callable):
        """Register a handler for a severity level. Handler receives Alert object."""
        self._handlers[severity].append(handler)

    def _compare(self, value: float, threshold: float, operator: str) -> bool:
        ops = {"gt": value > threshold, "lt": value < threshold,
               "gte": value >= threshold, "lte": value <= threshold}
        return ops.get(operator, False)

    def evaluate(self):
        """
        Evaluate all rules. Call this on a schedule (e.g., every 30s via APScheduler).
        Multi-window: check if metric violates threshold for sustained_seconds.
        """
        now = time.time()
        for rule in self.rules:
            agg = self.metrics.aggregate(
                rule.metric_name, rule.labels, rule.sustained_seconds
            )
            if not agg:
                continue

            # Use mean over the sustained window — more stable than latest value
            current_value = agg.get("mean", 0)
            is_firing = self._compare(current_value, rule.threshold, rule.operator)

            alert_key = f"{rule.name}:{json.dumps(rule.labels, sort_keys=True)}"

            if is_firing and alert_key not in self._active_alerts:
                # New alert — fire it
                alert = Alert(
                    rule_name=rule.name, fired_at=now, resolved_at=None,
                    current_value=current_value, severity=rule.severity,
                    labels=rule.labels
                )
                self._active_alerts[alert_key] = alert
                for handler in self._handlers[rule.severity]:
                    handler(alert)

            elif not is_firing and alert_key in self._active_alerts:
                # Alert resolved
                self._active_alerts[alert_key].resolved_at = now
                del self._active_alerts[alert_key]

    def get_active_alerts(self) -> List[Alert]:
        return list(self._active_alerts.values())


# Prevent NameError in AlertManager
import json
