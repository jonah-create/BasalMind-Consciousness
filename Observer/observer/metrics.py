"""
Observer Metrics - Lightweight in-memory tracking.

Collects performance metrics without Langfuse overhead per event.
"""

import time
from dataclasses import dataclass, field
from typing import List, Dict
from datetime import datetime


@dataclass
class ObserverMetrics:
    """
    Lightweight in-memory metrics collector.
    
    NO Langfuse calls here - just append to lists.
    Langfuse reporting happens separately (1/min aggregates).
    """
    
    # Counters
    events_observed: int = 0
    events_deduplicated: int = 0
    timescale_writes_success: int = 0
    timescale_writes_failed: int = 0
    redis_cache_hits: int = 0
    redis_cache_misses: int = 0
    neo4j_type_registrations: int = 0
    
    # Latency tracking (rolling window, last 1000)
    dedup_latencies_ms: List[float] = field(default_factory=list)
    timescale_write_latencies_ms: List[float] = field(default_factory=list)
    redis_cache_latencies_ms: List[float] = field(default_factory=list)
    
    # Errors (last 10 only)
    recent_errors: List[Dict] = field(default_factory=list)
    
    # Tracking
    start_time: float = field(default_factory=time.time)
    last_reset: float = field(default_factory=time.time)
    
    def record_event_observed(self, source: str):
        """Lightweight increment."""
        self.events_observed += 1
    
    def record_dedup_check(self, duration_ms: float, was_duplicate: bool):
        """Record dedup performance."""
        self.dedup_latencies_ms.append(duration_ms)
        if len(self.dedup_latencies_ms) > 1000:
            self.dedup_latencies_ms.pop(0)
        
        if was_duplicate:
            self.events_deduplicated += 1
    
    def record_timescale_write(self, duration_ms: float, success: bool):
        """Record TimescaleDB write performance."""
        if success:
            self.timescale_writes_success += 1
            self.timescale_write_latencies_ms.append(duration_ms)
            if len(self.timescale_write_latencies_ms) > 1000:
                self.timescale_write_latencies_ms.pop(0)
        else:
            self.timescale_writes_failed += 1
    
    def record_redis_cache(self, duration_ms: float, hit: bool):
        """Record Redis cache performance."""
        if hit:
            self.redis_cache_hits += 1
        else:
            self.redis_cache_misses += 1
        
        self.redis_cache_latencies_ms.append(duration_ms)
        if len(self.redis_cache_latencies_ms) > 1000:
            self.redis_cache_latencies_ms.pop(0)
    
    def record_neo4j_registration(self):
        """Record signal type registration."""
        self.neo4j_type_registrations += 1
    
    def record_error(self, error_type: str, message: str, event_id: str = None):
        """Record error (keep last 10)."""
        error = {
            "time": datetime.utcnow().isoformat(),
            "type": error_type,
            "message": message,
            "event_id": event_id
        }
        self.recent_errors.append(error)
        if len(self.recent_errors) > 10:
            self.recent_errors.pop(0)
    
    def get_summary(self) -> Dict:
        """
        Get metrics summary for Langfuse reporting.
        
        This is called every 60 seconds by the Langfuse reporter.
        """
        elapsed = time.time() - self.last_reset
        
        summary = {
            "period_seconds": elapsed,
            "events_observed": self.events_observed,
            "events_deduplicated": self.events_deduplicated,
            "dedup_rate": self.events_deduplicated / max(self.events_observed, 1),
            
            "timescale_writes_success": self.timescale_writes_success,
            "timescale_writes_failed": self.timescale_writes_failed,
            "write_success_rate": self.timescale_writes_success / max(
                self.timescale_writes_success + self.timescale_writes_failed, 1
            ),
            
            "redis_cache_hits": self.redis_cache_hits,
            "redis_cache_misses": self.redis_cache_misses,
            "cache_hit_rate": self.redis_cache_hits / max(
                self.redis_cache_hits + self.redis_cache_misses, 1
            ),
            
            "neo4j_type_registrations": self.neo4j_type_registrations,
            
            "events_per_second": self.events_observed / max(elapsed, 1),
            
            "error_count": len(self.recent_errors),
            "recent_errors": self.recent_errors.copy()
        }
        
        # Latency percentiles
        if self.timescale_write_latencies_ms:
            sorted_latencies = sorted(self.timescale_write_latencies_ms)
            n = len(sorted_latencies)
            summary["avg_write_latency_ms"] = sum(sorted_latencies) / n
            summary["p50_write_latency_ms"] = sorted_latencies[n // 2]
            summary["p95_write_latency_ms"] = sorted_latencies[int(n * 0.95)] if n > 20 else 0
            summary["p99_write_latency_ms"] = sorted_latencies[int(n * 0.99)] if n > 100 else 0
        else:
            summary["avg_write_latency_ms"] = 0
            summary["p50_write_latency_ms"] = 0
            summary["p95_write_latency_ms"] = 0
            summary["p99_write_latency_ms"] = 0
        
        return summary
    
    def reset(self):
        """Reset counters for next period."""
        self.events_observed = 0
        self.events_deduplicated = 0
        self.timescale_writes_success = 0
        self.timescale_writes_failed = 0
        self.redis_cache_hits = 0
        self.redis_cache_misses = 0
        self.neo4j_type_registrations = 0
        
        # Keep latencies and errors (rolling windows)
        
        self.last_reset = time.time()


# Global singleton
_observer_metrics = ObserverMetrics()


def get_metrics() -> ObserverMetrics:
    """Get global metrics instance."""
    return _observer_metrics
