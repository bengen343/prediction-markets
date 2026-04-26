import os
from collections import defaultdict


class BudgetGuard:
    """Tracks cumulative spend and signals soft warning + hard cap."""

    def __init__(self, limit_usd: float | None = None, warn_fraction: float = 0.7):
        if limit_usd is None:
            limit_usd = float(os.environ.get("DEBATE_BUDGET_USD", "0.50"))
        self.limit_usd = limit_usd
        self.warn_fraction = warn_fraction
        self._total = 0.0
        self._by_provider: dict[str, float] = defaultdict(float)

    def charge(self, provider: str, cost_usd: float) -> None:
        self._total += cost_usd
        self._by_provider[provider] += cost_usd

    @property
    def total(self) -> float:
        return self._total

    @property
    def by_provider(self) -> dict[str, float]:
        return dict(self._by_provider)

    @property
    def exhausted(self) -> bool:
        return self._total >= self.limit_usd

    @property
    def near_exhaustion(self) -> bool:
        return self._total >= self.limit_usd * self.warn_fraction
