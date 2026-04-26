import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from ..shared.log import get_logger
from .budget import BudgetGuard
from .moderator import Moderator
from .providers.anthropic_agent import AnthropicAgent
from .providers.gemini_agent import GeminiAgent
from .providers.grok_agent import GrokAgent
from .providers.openai_agent import OpenAIAgent
from .transcript import TranscriptWriter

log = get_logger(__name__)


@dataclass
class DebateOutput:
    debate_id: str
    outcome: str  # consensus | deadlock | budget_exhausted | error
    verdict: dict | None
    turn_count: int
    total_cost_usd: float
    cost_by_provider: dict[str, float]
    transcript_gcs_uri: str | None
    # Full sequence of agent + moderator entries in order. The agent-only
    # `transcript` used inside run_debate is intentionally separate (we don't
    # want agents to see the moderator's reasoning when forming next-turn
    # responses, otherwise they'd play to the moderator).
    full_transcript: list[dict]


def run_debate(
    *,
    question: str,
    bucket: str,
    api_keys: dict[str, str],
) -> DebateOutput:
    debate_id = uuid.uuid4().hex
    transcript_writer = TranscriptWriter(bucket=bucket, debate_id=debate_id)
    budget = BudgetGuard()

    agents = [
        AnthropicAgent(api_keys["anthropic"]),
        OpenAIAgent(api_keys["openai"]),
        GeminiAgent(api_keys["gemini"]),
        GrokAgent(api_keys["grok"]),
    ]
    moderator = Moderator(api_keys["anthropic"])

    transcript: list[dict] = []
    full_transcript: list[dict] = []
    turn = 0
    outcome = "error"
    verdict: dict | None = None

    try:
        while True:
            turn += 1
            log.info("debate.turn_start", debate_id=debate_id, turn=turn)

            # Run all four agents in parallel. They share the same transcript
            # snapshot, so order-of-response doesn't influence each agent's
            # input — we don't want the second agent to see the first agent's
            # response from this same turn.
            with ThreadPoolExecutor(max_workers=len(agents)) as ex:
                futures = [ex.submit(a.run, question, transcript) for a in agents]
                results = [f.result() for f in futures]

            new_entries: list[dict] = []
            for r in results:
                budget.charge(r.provider, r.cost_usd)
                if r.error:
                    log.error(
                        "debate.agent_error",
                        debate_id=debate_id, turn=turn,
                        provider=r.provider, error=r.error[:500],
                    )
                entry = {
                    "turn": turn,
                    "provider": r.provider,
                    "model": r.model,
                    "text": r.text or (f"[error: {r.error}]" if r.error else ""),
                    "input_tokens": r.input_tokens,
                    "output_tokens": r.output_tokens,
                    "search_count": r.search_count,
                    "cost_usd": r.cost_usd,
                    "error": r.error,
                }
                new_entries.append(entry)
                transcript_writer.append(entry)
            transcript.extend(new_entries)
            full_transcript.extend(new_entries)

            # Bail if every agent failed this turn — without it, an outage
            # (or missing keys) would trap us in an unkillable loop since
            # zero-cost turns never trip the budget cap.
            if all(r.error for r in results):
                log.error(
                    "debate.all_agents_failed",
                    debate_id=debate_id, turn=turn,
                    errors=[r.error for r in results],
                )
                outcome = "error"
                break

            # Moderator evaluates after each round. Pass wrap-up signal once we
            # cross the soft warning threshold, but only flag the moderator —
            # we still rely on the hard cap to stop the loop.
            decision, mod_result = moderator.evaluate(
                question=question,
                transcript=transcript,
                turn=turn,
                budget_used=budget.total,
                budget_limit=budget.limit_usd,
                wrap_up=budget.near_exhaustion,
            )
            budget.charge("moderator", mod_result.cost_usd)
            if mod_result.error:
                log.error(
                    "debate.moderator_error",
                    debate_id=debate_id, turn=turn, error=mod_result.error[:500],
                )
            mod_entry = {
                "turn": turn,
                "moderator": True,
                "model": mod_result.model,
                "decision": decision.get("decision"),
                "rationale": decision.get("rationale"),
                "verdict": decision.get("verdict"),
                "input_tokens": mod_result.input_tokens,
                "output_tokens": mod_result.output_tokens,
                "cost_usd": mod_result.cost_usd,
                "error": mod_result.error,
            }
            transcript_writer.append(mod_entry)
            full_transcript.append(mod_entry)

            decision_kind = decision.get("decision")
            if decision_kind == "consensus":
                outcome = "consensus"
                verdict = decision.get("verdict")
                break
            if decision_kind == "deadlock":
                outcome = "deadlock"
                verdict = decision.get("verdict")
                break
            if budget.exhausted:
                outcome = "budget_exhausted"
                verdict = decision.get("verdict")  # last best-effort, may be None
                break
    except Exception as e:
        log.exception("debate.unhandled_error", debate_id=debate_id)
        transcript_writer.append({"event": "unhandled_error", "error": str(e)})
        outcome = "error"

    transcript_uri: str | None = None
    try:
        transcript_uri = transcript_writer.flush()
    except Exception:
        log.exception("debate.transcript_flush_failed", debate_id=debate_id)

    return DebateOutput(
        debate_id=debate_id,
        outcome=outcome,
        verdict=verdict,
        turn_count=turn,
        total_cost_usd=budget.total,
        cost_by_provider=budget.by_provider,
        transcript_gcs_uri=transcript_uri,
        full_transcript=full_transcript,
    )


def load_api_keys() -> dict[str, str]:
    """Load LLM API keys from Secret Manager (with env-var override for local testing)."""
    from ..shared.secrets import get_secret

    out = {}
    for provider, secret_name in [
        ("anthropic", "anthropic-api-key"),
        ("openai", "openai-api-key"),
        ("gemini", "gemini-api-key"),
        ("grok", "xai-api-key"),
    ]:
        env_name = f"{provider.upper()}_API_KEY"
        out[provider] = os.environ.get(env_name) or get_secret(secret_name)
    return out
