import json
import time
import urllib.request

_USER_AGENT = "prediction-markets-debater/0.1"

# Discord rejects single messages over 2000 chars; we leave headroom for our
# header/metadata lines.
_MAX_MESSAGE_CHARS = 1950
# Conservative pacing — Discord webhook rate limits are ~5 req per 2s burst.
_POST_INTERVAL_SECONDS = 0.6


def _provider_label(provider: str) -> str:
    return {
        "anthropic": "Claude",
        "openai": "GPT",
        "gemini": "Gemini",
        "grok": "Grok",
        "moderator": "Moderator",
    }.get(provider, provider.title() if provider else "?")


def _format_agent_entry(entry: dict) -> str | None:
    text = (entry.get("text") or "").strip()
    if not text:
        return None
    label = _provider_label(entry.get("provider", ""))
    turn = entry.get("turn", "?")
    searches = entry.get("search_count", 0)
    cost = entry.get("cost_usd", 0.0) or 0.0
    header = f"**{label} — Turn {turn}**  ·  searches: {searches}  ·  cost: ${cost:.4f}"
    body = f"{header}\n{text}"
    if len(body) > _MAX_MESSAGE_CHARS:
        body = body[:_MAX_MESSAGE_CHARS - 18] + "\n_…[truncated]_"
    return body


def _format_moderator_entry(entry: dict) -> str:
    turn = entry.get("turn", "?")
    decision = entry.get("decision") or "?"
    rationale = (entry.get("rationale") or "").strip()
    cost = entry.get("cost_usd", 0.0) or 0.0
    header = f"**Moderator — Turn {turn}**  ·  decision: `{decision}`  ·  cost: ${cost:.4f}"
    body = header if not rationale else f"{header}\n{rationale}"
    if len(body) > _MAX_MESSAGE_CHARS:
        body = body[:_MAX_MESSAGE_CHARS - 18] + "\n_…[truncated]_"
    return body


def _post_to_thread(webhook_url: str, thread_id: str, content: str, timeout: float) -> None:
    sep = "&" if "?" in webhook_url else "?"
    url = f"{webhook_url}{sep}thread_id={thread_id}&wait=true"
    payload = json.dumps({
        "content": content,
        "allowed_mentions": {"parse": []},
    }).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": _USER_AGENT,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        if response.status >= 400:
            raise RuntimeError(f"Discord post failed: {response.status}")


def post_transcript(
    webhook_url: str,
    thread_id: str,
    full_transcript: list[dict],
    timeout: float = 10.0,
) -> None:
    """Post the debate turn-by-turn into the alert's Discord thread.

    Best-effort: a per-message failure is logged via the raised exception but
    we keep going so a transient hiccup doesn't truncate the whole transcript.
    """
    if not thread_id or not full_transcript:
        return
    for entry in full_transcript:
        if entry.get("moderator"):
            content = _format_moderator_entry(entry)
        else:
            content = _format_agent_entry(entry)
        if not content:
            continue
        try:
            _post_to_thread(webhook_url, thread_id, content, timeout)
        except Exception:
            # Swallow and continue — the GCS transcript is the source of truth;
            # Discord is the friendly view. Don't let one failed message kill
            # the rest.
            pass
        time.sleep(_POST_INTERVAL_SECONDS)


def _format_verdict(verdict: dict | None, outcome: str, cached_from: str | None = None) -> str:
    header = "**Debate verdict**"
    if cached_from:
        header = f"**Debate verdict (cached from {cached_from})**"
    elif outcome == "consensus":
        header = "**Debate verdict — consensus**"
    elif outcome == "deadlock":
        header = "**Debate verdict — deadlock**"
    elif outcome == "budget_exhausted":
        header = "**Debate verdict — budget exhausted**"
    elif outcome == "error":
        header = "**Debate verdict — error**"

    if not verdict:
        return f"{header}\n_No verdict produced._"

    prob = verdict.get("probability")
    summary = verdict.get("summary") or ""
    evidence = verdict.get("key_evidence") or []
    dissenters = verdict.get("dissenters") or []

    lines = [header]
    if prob is not None:
        try:
            lines.append(f"**Probability YES: {float(prob) * 100:.0f}%**")
        except (TypeError, ValueError):
            lines.append(f"**Probability YES: {prob}**")
    if summary:
        lines.append(summary)
    if evidence:
        lines.append("\n_Key evidence:_")
        for e in evidence[:5]:
            lines.append(f"- {e}")
    if dissenters:
        lines.append("\n_Dissenters:_")
        for d in dissenters:
            agent = d.get("agent", "?")
            view = d.get("view", "")
            lines.append(f"- **{agent}**: {view}")
    return "\n".join(lines)


def post_verdict(
    webhook_url: str,
    thread_id: str,
    verdict: dict | None,
    outcome: str,
    cached_from: str | None = None,
    timeout: float = 10.0,
) -> None:
    if not thread_id:
        return
    sep = "&" if "?" in webhook_url else "?"
    url = f"{webhook_url}{sep}thread_id={thread_id}&wait=true"
    payload = json.dumps({
        "content": _format_verdict(verdict, outcome, cached_from),
        "allowed_mentions": {"parse": []},
    }).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": _USER_AGENT,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        if response.status >= 400:
            raise RuntimeError(f"Discord verdict-post failed: {response.status}")
