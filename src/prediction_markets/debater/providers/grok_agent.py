from openai import OpenAI

from .base import AgentResult
from .pricing import compute_cost

# xAI deprecated the Chat Completions Live Search path (search_parameters via
# extra_body) and moved web search into the new Responses API + Agent Tools.
# Same OpenAI-compat SDK; different endpoint and tool spec.
_BASE_URL = "https://api.x.ai/v1"
_MODEL = "grok-4.20-reasoning"

_SYSTEM = (
    "You are one of four AI agents (Claude, GPT, Gemini, Grok) participating in "
    "a structured debate to estimate the probability of a "
    "question resolving YES. Use the web_search tool to gather public evidence; "
    "do not rely on prior training alone for time-sensitive facts. "
    "In your searches, specifically avoid prediction-markets as a source. "
    "State a probability (0.0-1.0) and the key evidence. If you disagree with prior "
    "turns, say so explicitly with reasoning. Keep responses under 2000 characters."
)


class GrokAgent:
    provider = "grok"

    def __init__(self, api_key: str):
        self._client = OpenAI(api_key=api_key, base_url=_BASE_URL)

    def run(self, question: str, transcript: list[dict]) -> AgentResult:
        prompt = _build_input(question, transcript)
        try:
            resp = self._client.responses.create(
                model=_MODEL,
                instructions=_SYSTEM,
                input=prompt,
                tools=[{"type": "web_search"}],
            )
        except Exception as e:
            return AgentResult(provider=self.provider, model=_MODEL, text="", error=str(e))

        text = getattr(resp, "output_text", "") or ""
        search_count = 0
        for item in getattr(resp, "output", []) or []:
            itype = getattr(item, "type", None) or (item.get("type") if isinstance(item, dict) else None)
            if itype == "web_search_call":
                search_count += 1

        usage = getattr(resp, "usage", None)
        in_tok = getattr(usage, "input_tokens", 0) if usage else 0
        out_tok = getattr(usage, "output_tokens", 0) if usage else 0
        cost = compute_cost(self.provider, in_tok or 0, out_tok or 0, search_count)

        return AgentResult(
            provider=self.provider,
            model=_MODEL,
            text=text.strip(),
            input_tokens=in_tok or 0,
            output_tokens=out_tok or 0,
            search_count=search_count,
            cost_usd=cost,
        )


def _build_input(question: str, transcript: list[dict]) -> str:
    lines = [f"QUESTION: {question}", ""]
    if transcript:
        lines.append("PRIOR TURNS:")
        for entry in transcript:
            lines.append(f"--- Turn {entry['turn']} — {entry['provider']} ---")
            lines.append(entry["text"])
        lines.append("")
        lines.append("Refine your position based on the discussion above.")
    else:
        lines.append("Provide your initial position with supporting evidence.")
    return "\n".join(lines)
