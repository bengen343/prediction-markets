import json
import urllib.request

_USER_AGENT = "prediction-markets-debater/0.1"


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
