"""
Circuit Breaker Pattern for External Service Resilience.

Prevents cascade failures when external services (Neo4j, etc.) are slow or down.

States:
- CLOSED: Normal operation, requests pass through
- OPEN: Service is failing, requests fail fast
- HALF_OPEN: Testing if service has recovered

"""

import asyncio
import logging
from enum import Enum
from datetime import datetime, timedelta
from typing import Callable, Any, Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker to prevent cascade failures.

    Usage:
        breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

        async with breaker:
            result = await call_external_service()
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception
    ):
        """
        Args:
            name: Circuit breaker name for logging
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type to catch
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.success_count = 0

        # Metrics
        self.total_calls = 0
        self.total_failures = 0
        self.total_rejections = 0  # Calls rejected when circuit open

    async def __aenter__(self):
        """Enter context manager."""
        self._check_state()
        if self.state == CircuitState.OPEN:
            self.total_rejections += 1
            raise CircuitBreakerOpen(f"Circuit breaker '{self.name}' is OPEN")

        self.total_calls += 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager, handle success/failure."""
        if exc_type is None:
            # Success
            self._on_success()
            return False
        elif isinstance(exc_val, self.expected_exception):
            # Expected failure
            self._on_failure()
            return False  # Don't suppress exception
        else:
            # Unexpected exception, don't count as circuit failure
            return False

    def _check_state(self):
        """Check if circuit should transition state."""
        if self.state == CircuitState.OPEN:
            # Check if recovery timeout has passed
            if self.last_failure_time:
                time_since_failure = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if time_since_failure >= self.recovery_timeout:
                    self._transition_to_half_open()

    def _on_success(self):
        """Handle successful call."""
        if self.state == CircuitState.HALF_OPEN:
            # Recovery successful
            self._transition_to_closed()
        elif self.state == CircuitState.CLOSED:
            # Reset failure count on success
            self.failure_count = 0

    def _on_failure(self):
        """Handle failed call."""
        self.failure_count += 1
        self.total_failures += 1
        self.last_failure_time = datetime.utcnow()

        if self.state == CircuitState.HALF_OPEN:
            # Recovery failed
            self._transition_to_open()
        elif self.state == CircuitState.CLOSED:
            if self.failure_count >= self.failure_threshold:
                self._transition_to_open()

    def _transition_to_open(self):
        """Transition circuit to OPEN state."""
        old_state = self.state
        self.state = CircuitState.OPEN
        logger.warning(
            f"🔴 Circuit breaker '{self.name}' transitioned {old_state.value} → OPEN "
            f"(failures: {self.failure_count}/{self.failure_threshold})"
        )

    def _transition_to_half_open(self):
        """Transition circuit to HALF_OPEN state."""
        old_state = self.state
        self.state = CircuitState.HALF_OPEN
        self.failure_count = 0
        logger.info(
            f"🟡 Circuit breaker '{self.name}' transitioned {old_state.value} → HALF_OPEN "
            f"(testing recovery after {self.recovery_timeout}s)"
        )

    def _transition_to_closed(self):
        """Transition circuit to CLOSED state."""
        old_state = self.state
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        logger.info(f"🟢 Circuit breaker '{self.name}' transitioned {old_state.value} → CLOSED (recovered)")

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Call a function through the circuit breaker.

        Alternative to context manager usage:
            result = await breaker.call(my_async_function, arg1, arg2)
        """
        async with self:
            return await func(*args, **kwargs)

    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "failure_threshold": self.failure_threshold,
            "total_calls": self.total_calls,
            "total_failures": self.total_failures,
            "total_rejections": self.total_rejections,
            "success_rate": (
                (self.total_calls - self.total_failures) / self.total_calls * 100
                if self.total_calls > 0 else 100.0
            ),
            "last_failure": self.last_failure_time.isoformat() if self.last_failure_time else None
        }

    def reset(self):
        """Manually reset circuit breaker to CLOSED state."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        logger.info(f"🔄 Circuit breaker '{self.name}' manually reset to CLOSED")


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is OPEN."""
    pass


# Example usage:
"""
# Create circuit breaker
neo4j_breaker = CircuitBreaker(
    name="neo4j",
    failure_threshold=5,
    recovery_timeout=60
)

# Use with context manager
async def get_enrichment(event_type):
    try:
        async with neo4j_breaker:
            return await neo4j_query(event_type)
    except CircuitBreakerOpen:
        # Circuit is open, return default value
        return {"enriched": False, "reason": "circuit_open"}
    except Exception as e:
        # Other errors
        return {"enriched": False, "error": str(e)}

# Or use with call method
result = await neo4j_breaker.call(neo4j_query, event_type)
"""
