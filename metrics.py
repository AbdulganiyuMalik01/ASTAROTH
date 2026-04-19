"""
Metrics Module
Prometheus-compatible metrics for monitoring webhook system.
"""
import time
from typing import Dict, Any
from collections import defaultdict
from dataclasses import dataclass, field
import threading


@dataclass
class Counter:
    """Simple counter metric."""
    name: str
    help_text: str
    value: int = 0
    labels: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def inc(self, amount: int = 1, **labels):
        """Increment counter."""
        if labels:
            label_key = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            self.labels[label_key] += amount
        else:
            self.value += amount

    def get(self, **labels) -> int:
        """Get counter value."""
        if labels:
            label_key = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            return self.labels.get(label_key, 0)
        return self.value

    def format_prometheus(self) -> str:
        """Format for Prometheus scraping."""
        lines = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} counter",
        ]
        if self.labels:
            for label_key, value in self.labels.items():
                lines.append(f"{self.name}{{{label_key}}} {value}")
        else:
            lines.append(f"{self.name} {self.value}")
        return "\n".join(lines)


@dataclass
class Gauge:
    """Simple gauge metric."""
    name: str
    help_text: str
    value: float = 0.0
    labels: Dict[str, float] = field(default_factory=lambda: defaultdict(float))

    def set(self, value: float, **labels):
        if labels:
            label_key = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            self.labels[label_key] = value
        else:
            self.value = value

    def inc(self, amount: float = 1.0, **labels):
        if labels:
            label_key = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            self.labels[label_key] += amount
        else:
            self.value += amount

    def dec(self, amount: float = 1.0, **labels):
        self.inc(-amount, **labels)

    def get(self, **labels) -> float:
        if labels:
            label_key = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            return self.labels.get(label_key, 0.0)
        return self.value

    def format_prometheus(self) -> str:
        lines = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} gauge",
        ]
        if self.labels:
            for label_key, value in self.labels.items():
                lines.append(f"{self.name}{{{label_key}}} {value}")
        else:
            lines.append(f"{self.name} {self.value}")
        return "\n".join(lines)


@dataclass
class Histogram:
    """Simple histogram metric."""
    name: str
    help_text: str
    buckets: list = field(default_factory=lambda: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    observations: list = field(default_factory=list)
    sum_value: float = 0.0
    count: int = 0

    def observe(self, value: float):
        self.observations.append(value)
        self.sum_value += value
        self.count += 1

    def format_prometheus(self) -> str:
        lines = [
            f"# HELP {self.name} {self.help_text}",
            f"# TYPE {self.name} histogram",
        ]
        bucket_counts = defaultdict(int)
        for obs in self.observations:
            for bucket in self.buckets:
                if obs <= bucket:
                    bucket_counts[bucket] += 1
        cumulative = 0
        for bucket in self.buckets:
            cumulative += bucket_counts.get(bucket, 0)
            lines.append(f'{self.name}_bucket{{le="{bucket}"}} {cumulative}')
        lines.append(f'{self.name}_bucket{{le="+Inf"}} {self.count}')
        lines.append(f'{self.name}_sum {self.sum_value}')
        lines.append(f'{self.name}_count {self.count}')
        return "\n".join(lines)


class MetricsRegistry:
    """Registry for all metrics."""

    def __init__(self):
        self.metrics: Dict[str, Any] = {}
        self.lock = threading.Lock()

    def register_counter(self, name: str, help_text: str) -> Counter:
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = Counter(name, help_text)
            return self.metrics[name]

    def register_gauge(self, name: str, help_text: str) -> Gauge:
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = Gauge(name, help_text)
            return self.metrics[name]

    def register_histogram(self, name: str, help_text: str, buckets: list = None) -> Histogram:
        with self.lock:
            if name not in self.metrics:
                hist = Histogram(name, help_text)
                if buckets:
                    hist.buckets = buckets
                self.metrics[name] = hist
            return self.metrics[name]

    def format_all_prometheus(self) -> str:
        with self.lock:
            lines = []
            for metric in self.metrics.values():
                lines.append(metric.format_prometheus())
            return "\n\n".join(lines)


# Global registry
_registry = MetricsRegistry()

webhook_requests_total = _registry.register_counter("webhook_requests_total", "Total number of webhook requests received")
webhook_processing_duration_seconds = _registry.register_histogram("webhook_processing_duration_seconds", "Time spent processing webhook requests")
tokens_tracked_total = _registry.register_gauge("tokens_tracked_total", "Total number of tokens currently being tracked")
alerts_sent_total = _registry.register_counter("alerts_sent_total", "Total number of alerts sent to Telegram")
api_calls_total = _registry.register_counter("api_calls_total", "Total number of external API calls")
api_errors_total = _registry.register_counter("api_errors_total", "Total number of API call errors")
circuit_breaker_state = _registry.register_gauge("circuit_breaker_state", "Circuit breaker state (0=closed, 1=half_open, 2=open)")
smart_volume_detections_total = _registry.register_counter("smart_volume_detections_total", "Total number of smart volume detections")
defi_deployments_detected_total = _registry.register_counter("defi_deployments_detected_total", "Total number of DeFi deployments detected")


def get_registry() -> MetricsRegistry:
    return _registry


def format_metrics_for_prometheus() -> str:
    return _registry.format_all_prometheus()
