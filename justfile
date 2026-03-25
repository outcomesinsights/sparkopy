# Run the full test suite (matches CI)
test:
    uv run ruff check src/ tests/
    uv run ruff format --check src/ tests/
    uv run pytest tests/ -v
