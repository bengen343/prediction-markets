from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey


def verify_signature(
    public_key_hex: str,
    signature_hex: str,
    timestamp: str,
    body: bytes,
) -> bool:
    """Verify the Ed25519 signature Discord attaches to every interaction.

    Discord rejects any application that doesn't enforce this — they actively
    probe with malformed signatures during the URL validation step in the
    Developer Portal, and a missing/incorrect check disqualifies the URL.
    """
    try:
        verify_key = VerifyKey(bytes.fromhex(public_key_hex))
        verify_key.verify(timestamp.encode() + body, bytes.fromhex(signature_hex))
        return True
    except (BadSignatureError, ValueError):
        return False
