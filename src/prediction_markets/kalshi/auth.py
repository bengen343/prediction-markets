import base64
import time

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def load_private_key(pem_bytes: bytes) -> rsa.RSAPrivateKey:
    key = serialization.load_pem_private_key(pem_bytes, password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("Expected an RSA private key")
    return key


def sign(private_key: rsa.RSAPrivateKey, message: str) -> str:
    signature = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def build_headers(
    private_key: rsa.RSAPrivateKey,
    access_key_id: str,
    method: str,
    path: str,
) -> dict[str, str]:
    # Kalshi spec: sign the path WITHOUT query string.
    path_without_query = path.split("?", 1)[0]
    timestamp = str(int(time.time() * 1000))
    message = f"{timestamp}{method.upper()}{path_without_query}"
    return {
        "KALSHI-ACCESS-KEY": access_key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": sign(private_key, message),
    }
