"""Helper for counting tokens using tiktoken.

Provides a single function `count_tokens(text, model="gpt-4") -> int` used by the codebase
for estimating token usage. The implementation prefers `encoding_for_model` and falls back to
`cl100k_base` encoding if the model is unknown.
"""
import tiktoken

from typing import Optional


def count_tokens(text: Optional[str], model: str = "gpt-5") -> int:
    """Return the number of tokens in `text` for the given `model`.

    Args:
        text: input text (None treated as empty)
        model: model name to choose encoding for (defaults to "gpt-4")

    Returns:
        Number of tokens (int)

    Raises:
        ImportError: if the `tiktoken` package is not installed.
    """
    if not text:
        return 0

    # Prefer a model-specific encoding; fall back to cl100k_base which is used by OpenAI gpt-* models.
    try:
        enc = tiktoken.encoding_for_model(model)
    except Exception:
        print("Failed to get encoding for model '{}'".format(model))
        enc = tiktoken.get_encoding("cl100k_base")

    # encode returns a list/array of token ids
    tokens = enc.encode(text)
    return len(tokens)

