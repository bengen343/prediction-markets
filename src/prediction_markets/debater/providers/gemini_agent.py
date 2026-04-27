from google import genai
from google.genai import types

from .base import AgentResult
from .pricing import compute_cost

_MODEL = "gemini-2.5-pro"

_SYSTEM = (
    "You are one of four AI agents (Claude, GPT, Gemini, Grok) participating in "
    "a structured debate to estimate the probability of a "
    "question resolving YES. Use Google Search grounding to gather public "
    "evidence; do not rely on prior training alone for time-sensitive facts. "
    "In your searches, specifically avoid prediction-markets as a source. "
    "State a probability (0.0-1.0) and the key evidence. If you disagree with "
    "prior turns, say so explicitly with reasoning. Keep responses under 2000 characters."
    "words."
)


class GeminiAgent:
    provider = "gemini"

    def __init__(self, api_key: str):
        self._client = genai.Client(api_key=api_key)

    def run(self, question: str, transcript: list[dict]) -> AgentResult:
        prompt = _build_prompt(question, transcript)
        try:
            resp = self._client.models.generate_content(
                model=_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=_SYSTEM,
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                ),
            )
        except Exception as e:
            return AgentResult(provider=self.provider, model=_MODEL, text="", error=str(e))

        text = getattr(resp, "text", "") or ""

        # Search count: count grounding chunks in the first candidate's metadata.
        search_count = 0
        candidates = getattr(resp, "candidates", None) or []
        if candidates:
            grounding = getattr(candidates[0], "grounding_metadata", None)
            if grounding:
                queries = getattr(grounding, "web_search_queries", None) or []
                search_count = len(queries)

        usage = getattr(resp, "usage_metadata", None)
        in_tok = getattr(usage, "prompt_token_count", 0) if usage else 0
        out_tok = getattr(usage, "candidates_token_count", 0) if usage else 0
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


def _build_prompt(question: str, transcript: list[dict]) -> str:
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
