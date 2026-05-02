# Contributing

Thanks for helping improve HA Energy TUI.

## Development Setup

```bash
git clone https://github.com/rockyolsen/ha-energy-tui
cd ha-energy-tui
uv sync --all-extras
uv run pytest
uv run ruff check .
```

## Pull Requests

- Keep changes focused.
- Include tests for behavior that can be tested without a live Home Assistant instance.
- For TUI changes, describe the keyboard path you tested.
- Avoid direct writes to Home Assistant `.storage` files.

## Bug Reports

Please include:

- Home Assistant version
- Python version
- OS and terminal emulator
- Install method
- Command used to run the app
- Screenshot or copied status/error text when useful
