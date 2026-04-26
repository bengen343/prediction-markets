import json
import re

import anthropic

from .providers.base import AgentResult
from .providers.pricing import compute_cost

_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS = 1500

_SYSTEM = (
    "You are the moderator of a four-agent AI debate (Claude, GPT, Gemini, Grok) "
    "estimating the probability of a prediction-market question. After each "
    "round you must judge whether to: (a) declare consensus, (b) declare "
    "deadlock, or (c) continue debate. Be strict about consensus — require "
    "alignment on probability (within 0.10) AND substantive agreement on "
    "reasoning. Declare deadlock only when further debate is unlikely to move "
    "positions. Respond ONLY with a single JSON object using this schema:\n"
    '{"decision": "consensus|deadlock|continue", "rationale": "...", '
    '"verdict": {"probability": 0.NN, "summary": "...", '
    '"key_evidence": ["..."], "dissenters": [{"agent": "...", "view": "..."}]}}\n'
    'The "verdict" key is required for "consensus" and may be a best-effort '
    'summary for "deadlock"; for "continue" it can be omitted or null.'
)


class Moderator:
    provider = "moderator"

    def __init__(self, api_key: str):
        self._client = anthropic.Anthropic(api_key=api_key)

    def evaluate(
        self,
        question: str,
        transcript: list[dict],
        turn: int,
        budget_used: float,
        budget_limit: float,
        wrap_up: bool,
    ) -> tuple[dict, AgentResult]:
        user_msg = _build_user_message(question, transcript, turn, budget_used, budget_limit, wrap_up)
        try:
            resp = self._client.messages.create(
                model=_MODEL,
                max_tokens=_MAX_TOKENS,
                system=_SYSTEM,
                messages=[{"role": "user", "content": user_msg}],
            )
        except Exception as e:
            err_result = AgentResult(provider="moderator", model=_MODEL, text="", error=str(e))
            return ({"decision": "continue", "rationale": f"moderator error: {e}"}, err_result)

        text_parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        text = "\n".join(text_parts).strip()
        in_tok = getattr(resp.usage, "input_tokens", 0) or 0
        out_tok = getattr(resp.usage, "output_tokens", 0) or 0
        cost = compute_cost("anthropic", in_tok, out_tok, 0)

        result = AgentResult(
            provider="moderator",
            model=_MODEL,
            text=text,
            input_tokens=in_tok,
            output_tokens=out_tok,
            cost_usd=cost,
        )

        decision = _parse_decision(text)
        return (decision, result)


def _parse_decision(text: str) -> dict:
    # Tolerate prose around the JSON: extract the first {...} block.
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return {"decision": "continue", "rationale": "moderator did not return JSON"}
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {"decision": "continue", "rationale": "moderator JSON parse failed"}
    if parsed.get("decision") not in {"consensus", "deadlock", "continue"}:
        parsed["decision"] = "continue"
    return parsed


def _build_user_message(
    question: str,
    transcript: list[dict],
    turn: int,
    budget_used: float,
    budget_limit: float,
    wrap_up: bool,
) -> str:
    lines = [
        f"QUESTION: {question}",
        f"TURN: {turn}",
        f"BUDGET: ${budget_used:.3f} / ${budget_limit:.2f} used",
    ]
    if wrap_up:
        lines.append(
            "WRAP-UP SIGNAL: budget is nearing exhaustion. If consensus is "
            "plausible this round, declare it; otherwise declare deadlock. "
            "Do not return 'continue' unless absolutely necessary."
        )
    lines.append("")
    lines.append("DEBATE TRANSCRIPT:")
    for entry in transcript:
        lines.append(f"--- Turn {entry['turn']} — {entry['provider']} ---")
        lines.append(entry["text"])
    lines.append("")
    lines.append("Return your decision as a single JSON object per the system schema.")
    return "\n".join(lines)
