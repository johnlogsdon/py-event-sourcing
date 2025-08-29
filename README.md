# Event Sourcing V2

This is a Python implementation of an event sourcing system (version 2), focusing on functional programming, async I/O, and dependency injection.

## Setup

This project uses `uv` for dependency management.

1.  **Create and activate the virtual environment:**
    ```bash
    uv venv
    source .venv/bin/activate
    ```

2.  **Install the package in editable mode with dev dependencies:**
    This command installs the project's dependencies and testing tools. The `-e` flag makes your local source code available on the Python path, resolving `ModuleNotFoundError`.
    ```bash
    uv pip install -e ".[dev]"
    ```

## Usage

Run the main script: `uv run python main.py`

## Testing

Run the test suite: `uv run pytest`

## Design

See [docs/DESIGN.md](docs/DESIGN.md) for architecture details.
