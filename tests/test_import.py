import importlib.util
import sys
from pathlib import Path


def test_script_loads() -> None:
    script_path = Path(__file__).parent.parent / "ha_energy_tui.py"
    spec = importlib.util.spec_from_file_location("ha_energy_tui_script", script_path)

    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    assert module.main
