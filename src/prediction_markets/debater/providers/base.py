from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class AgentResult:
    provider: str
    model: str
    text: str
    input_tokens: int = 0
    output_tokens: int = 0
    search_count: int = 0
    cost_usd: float = 0.0
    error: str | None = None
    raw_meta: dict = field(default_factory=dict)


class AgentRunner(Protocol):
    provider: str

    def run(self, question: str, transcript: list[dict]) -> AgentResult: ...
