import anthropic

from .base import AgentResult
from .pricing import compute_cost

_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS = 2000

_SYSTEM = (
    "You are one of four AI agents (Claude, GPT, Gemini, Grok) participating in "
    "a structured debate to estimate the probability of a "
    "question resolving YES. Use the web_search tool to gather public evidence; "
    "do not rely on prior training alone for time-sensitive facts."
    "In your searches, specifically avoid prediction-markets as a source. "
    "State a probability (0.0-1.0) and the key evidence. If you disagree with prior "
    "turns, say so explicitly with reasoning. Keep responses under 250 words."
)


class AnthropicAgent:
    provider = "anthropic"

    def __init__(self, api_key: str):
        self._client = anthropic.Anthropic(api_key=api_key)

    def run(self, question: str, transcript: list[dict]) -> AgentResult:
        user_msg = _build_user_message(question, transcript)
        try:
            resp = self._client.messages.create(
                model=_MODEL,
                max_tokens=_MAX_TOKENS,
                system=_SYSTEM,
                messages=[{"role": "user", "content": user_msg}],
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
            )
        except Exception as e:
            return AgentResult(provider=self.provider, model=_MODEL, text="", error=str(e))

        text_parts: list[str] = []
        search_count = 0
        for block in resp.content:
            btype = getattr(block, "type", None)
            if btype == "text":
                text_parts.append(block.text)
            elif btype == "server_tool_use" and getattr(block, "name", "") == "web_search":
                search_count += 1

        in_tok = getattr(resp.usage, "input_tokens", 0) or 0
        out_tok = getattr(resp.usage, "output_tokens", 0) or 0
        cost = compute_cost(self.provider, in_tok, out_tok, search_count)

        return AgentResult(
            provider=self.provider,
            model=_MODEL,
            text="\n".join(text_parts).strip(),
            input_tokens=in_tok,
            output_tokens=out_tok,
            search_count=search_count,
            cost_usd=cost,
        )


def _build_user_message(question: str, transcript: list[dict]) -> str:
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
