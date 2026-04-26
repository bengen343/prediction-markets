from openai import OpenAI

from .base import AgentResult
from .pricing import compute_cost

# xAI exposes an OpenAI-compatible Chat Completions endpoint. Live Search rides
# on a non-standard `search_parameters` field, which we pass via `extra_body`.
_BASE_URL = "https://api.x.ai/v1"
_MODEL = "grok-4"

_SYSTEM = (
    "You are one of four AI agents (Claude, GPT, Gemini, Grok) participating in "
    "a structured debate to estimate the probability of a prediction-market "
    "question resolving YES. Use Live Search to gather public evidence; do not "
    "rely on prior training alone for time-sensitive facts. State a probability "
    "(0.0-1.0) and the key evidence. If you disagree with prior turns, say so "
    "explicitly with reasoning. Keep responses under 250 words."
)


class GrokAgent:
    provider = "grok"

    def __init__(self, api_key: str):
        self._client = OpenAI(api_key=api_key, base_url=_BASE_URL)

    def run(self, question: str, transcript: list[dict]) -> AgentResult:
        user_msg = _build_user_message(question, transcript)
        try:
            resp = self._client.chat.completions.create(
                model=_MODEL,
                messages=[
                    {"role": "system", "content": _SYSTEM},
                    {"role": "user", "content": user_msg},
                ],
                extra_body={"search_parameters": {"mode": "auto"}},
            )
        except Exception as e:
            return AgentResult(provider=self.provider, model=_MODEL, text="", error=str(e))

        choice = resp.choices[0] if resp.choices else None
        text = (choice.message.content if choice and choice.message else "") or ""

        usage = getattr(resp, "usage", None)
        in_tok = getattr(usage, "prompt_tokens", 0) if usage else 0
        out_tok = getattr(usage, "completion_tokens", 0) if usage else 0

        # xAI returns a `num_sources_used` field on Live Search responses; the
        # exact location varies. Best-effort lookup; fall back to 0.
        search_count = 0
        usage_dict = getattr(usage, "model_dump", lambda: {})() if usage else {}
        search_count = (
            usage_dict.get("num_sources_used")
            or (usage_dict.get("search_usage") or {}).get("num_sources_used")
            or 0
        )

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
