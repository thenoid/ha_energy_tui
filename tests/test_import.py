import importlib.util
import sys
from pathlib import Path


def load_script_module():
    script_path = Path(__file__).parent.parent / "ha_energy_tui.py"
    spec = importlib.util.spec_from_file_location("ha_energy_tui_script", script_path)

    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_script_loads() -> None:
    module = load_script_module()
    assert module.main


def test_device_config_changes_records_old_and_new_values() -> None:
    module = load_script_module()

    assert module.device_config_changes(
        {
            "sensor.removed": {"stat_consumption": "sensor.removed"},
            "sensor.updated": {"stat_consumption": "sensor.updated", "name": "Old"},
            "sensor.same": {"stat_consumption": "sensor.same"},
        },
        {
            "sensor.added": {"stat_consumption": "sensor.added"},
            "sensor.updated": {"stat_consumption": "sensor.updated", "name": "New"},
            "sensor.same": {"stat_consumption": "sensor.same"},
        },
    ) == {
        "sensor.added": {
            "old": None,
            "new": {"stat_consumption": "sensor.added"},
        },
        "sensor.removed": {
            "old": {"stat_consumption": "sensor.removed"},
            "new": None,
        },
        "sensor.updated": {
            "old": {"stat_consumption": "sensor.updated", "name": "Old"},
            "new": {"stat_consumption": "sensor.updated", "name": "New"},
        },
    }


def test_write_audit_log_appends_json_fields(tmp_path, monkeypatch) -> None:
    module = load_script_module()
    log_path = tmp_path / "tui_ha.log"
    monkeypatch.setattr(module, "AUDIT_LOG_PATH", log_path)

    module.write_audit_log(
        "staged_enable_device",
        mode="electric",
        entity_id="sensor.example",
        old=None,
        new={"stat_consumption": "sensor.example"},
    )

    log_text = log_path.read_text(encoding="utf-8")
    assert " staged_enable_device " in log_text
    assert 'mode="electric"' in log_text
    assert 'entity_id="sensor.example"' in log_text
    assert '"stat_consumption": "sensor.example"' in log_text
