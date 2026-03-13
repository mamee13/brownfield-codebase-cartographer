"""USD cost mapping for LLM models."""

# Prices per 1M tokens in USD (as of early 2026 estimate/placeholders)
MODEL_COSTS = {
    "qwen/qwen-2.5-7b-instruct:free": {"input": 0.0, "output": 0.0},
    "mistralai/mistral-small-24b-instruct-2501:free": {"input": 0.0, "output": 0.0},
    "openai/gpt-4o": {"input": 5.0, "output": 15.0},
    "openai/gpt-4o-mini": {"input": 0.15, "output": 0.6},
    "anthropic/claude-3-5-sonnet": {"input": 3.0, "output": 15.0},
    # Add more as needed
}


def calculate_cost(model_name: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate USD cost for a given model and token counts."""
    costs = MODEL_COSTS.get(model_name, {"input": 0.0, "output": 0.0})
    input_cost = (input_tokens / 1_000_000) * costs["input"]
    output_cost = (output_tokens / 1_000_000) * costs["output"]
    return input_cost + output_cost
