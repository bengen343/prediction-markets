# Per-million-token pricing in USD. Updated approximately 2026-01.
# These are best-effort defaults; tweak as providers change rates.
#
# Web-search-call pricing is rough — provider docs vary. We charge a flat
# per-search rate. Token usage on top is already counted via the standard
# input/output rates.
PRICING = {
    "anthropic": {
        # claude-sonnet-4-6
        "input_per_mtok": 3.00,
        "output_per_mtok": 15.00,
        "search_per_call": 0.01,
    },
    "openai": {
        # gpt-4o (Responses API)
        "input_per_mtok": 2.50,
        "output_per_mtok": 10.00,
        "search_per_call": 0.025,
    },
    "gemini": {
        # gemini-2.5-pro; google_search grounding is free under daily quotas,
        # so we don't add a per-search charge.
        "input_per_mtok": 1.25,
        "output_per_mtok": 5.00,
        "search_per_call": 0.0,
    },
    "grok": {
        # grok-4 with Live Search; xAI charges $0.025 per source returned.
        "input_per_mtok": 3.00,
        "output_per_mtok": 15.00,
        "search_per_call": 0.025,
    },
}


def compute_cost(provider: str, input_tokens: int, output_tokens: int, search_count: int) -> float:
    p = PRICING.get(provider)
    if not p:
        return 0.0
    return (
        input_tokens / 1_000_000 * p["input_per_mtok"]
        + output_tokens / 1_000_000 * p["output_per_mtok"]
        + search_count * p["search_per_call"]
    )
