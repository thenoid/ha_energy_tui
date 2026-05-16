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


def make_app(module, consumption=None, entity_registry=None):
    prefs = {
        "data": {
            "device_consumption": consumption or [],
            "device_consumption_water": [],
        }
    }
    sensors = [
        module.EnergySensor(
            entity_id="sensor.child",
            name="Child Sensor",
            unit="kWh",
            state_class="total_increasing",
            current_state="1",
            configured=False,
            eligible=True,
        ),
        module.EnergySensor(
            entity_id="sensor.parent",
            name="Parent Sensor",
            unit="kWh",
            state_class="total_increasing",
            current_state="2",
            configured=False,
            eligible=True,
        ),
    ]
    return module.EnergyConfiguratorApp(
        client=object(),
        prefs=prefs,
        sensors=sensors,
        entity_registry=entity_registry or {},
        device_registry={},
    )


def test_enable_device_undo_redo_updates_dirty_state() -> None:
    module = load_script_module()
    app = make_app(module)
    command = module.EditCommand(
        action="staged_enable_device",
        mode="electric",
        entity_id="sensor.child",
        old_config=None,
        new_config={"stat_consumption": "sensor.child"},
    )

    assert app.stage_edit_command(command)
    assert app.device_configs_by_mode["electric"] == {
        "sensor.child": {"stat_consumption": "sensor.child"}
    }
    assert app.dirty_modes == {"electric"}

    assert app.undo_edit_command() == command
    assert app.device_configs_by_mode["electric"] == {}
    assert app.dirty_modes == set()

    assert app.redo_edit_command() == command
    assert app.device_configs_by_mode["electric"] == {
        "sensor.child": {"stat_consumption": "sensor.child"}
    }
    assert app.dirty_modes == {"electric"}


def test_disable_device_undo_restores_full_config() -> None:
    module = load_script_module()
    old_config = {
        "stat_consumption": "sensor.child",
        "name": "Child",
        "included_in_stat": "sensor.parent",
    }
    app = make_app(module, consumption=[old_config])
    command = module.EditCommand(
        action="staged_disable_device",
        mode="electric",
        entity_id="sensor.child",
        old_config=old_config,
        new_config=None,
    )

    assert app.stage_edit_command(command)
    assert app.device_configs_by_mode["electric"] == {}

    app.undo_edit_command()

    assert app.device_configs_by_mode["electric"] == {"sensor.child": old_config}
    assert app.dirty_modes == set()


def test_parent_set_and_clear_undo_redo() -> None:
    module = load_script_module()
    base_config = {"stat_consumption": "sensor.child"}
    parent_config = {
        "stat_consumption": "sensor.child",
        "included_in_stat": "sensor.parent",
    }
    app = make_app(module, consumption=[base_config])
    command = module.EditCommand(
        action="staged_set_parent",
        mode="electric",
        entity_id="sensor.child",
        old_config=base_config,
        new_config=parent_config,
    )

    app.stage_edit_command(command)
    assert app.device_configs_by_mode["electric"]["sensor.child"] == parent_config

    app.undo_edit_command()
    assert app.device_configs_by_mode["electric"]["sensor.child"] == base_config

    app.redo_edit_command()
    assert app.device_configs_by_mode["electric"]["sensor.child"] == parent_config


def test_rename_undo_redo_restores_config_and_entity_name_update() -> None:
    module = load_script_module()
    old_config = {"stat_consumption": "sensor.child", "name": "Old Energy"}
    new_config = {"stat_consumption": "sensor.child", "name": "New Energy"}
    app = make_app(
        module,
        consumption=[old_config],
        entity_registry={"sensor.child": {"name": "Old Entity"}},
    )
    command = module.EditCommand(
        action="staged_rename_device",
        mode="electric",
        entity_id="sensor.child",
        old_config=old_config,
        new_config=new_config,
        entity_name_changed=True,
        old_entity_name="Old Entity",
        new_entity_name="New Entity",
    )

    app.stage_edit_command(command)
    assert app.device_configs_by_mode["electric"]["sensor.child"] == new_config
    assert app.entity_name_updates == {"sensor.child": "New Entity"}

    app.undo_edit_command()
    assert app.device_configs_by_mode["electric"]["sensor.child"] == old_config
    assert app.entity_name_updates == {}

    app.redo_edit_command()
    assert app.device_configs_by_mode["electric"]["sensor.child"] == new_config
    assert app.entity_name_updates == {"sensor.child": "New Entity"}


def test_undo_after_save_stages_inverse_against_new_baseline() -> None:
    module = load_script_module()
    app = make_app(module)
    new_config = {"stat_consumption": "sensor.child"}
    command = module.EditCommand(
        action="staged_enable_device",
        mode="electric",
        entity_id="sensor.child",
        old_config=None,
        new_config=new_config,
    )
    app.stage_edit_command(command)

    app.prefs = {
        "data": {
            "device_consumption": [new_config],
            "device_consumption_water": [],
        }
    }
    app.device_configs_by_mode["electric"] = module.load_device_configs(
        app.prefs, "electric"
    )
    app.dirty_modes.clear()

    app.undo_edit_command()

    assert app.device_configs_by_mode["electric"] == {}
    assert app.dirty_modes == {"electric"}


def test_new_edit_after_undo_clears_redo_stack() -> None:
    module = load_script_module()
    app = make_app(module)
    first = module.EditCommand(
        action="staged_enable_device",
        mode="electric",
        entity_id="sensor.child",
        old_config=None,
        new_config={"stat_consumption": "sensor.child"},
    )
    second = module.EditCommand(
        action="staged_enable_device",
        mode="electric",
        entity_id="sensor.parent",
        old_config=None,
        new_config={"stat_consumption": "sensor.parent"},
    )

    app.stage_edit_command(first)
    app.undo_edit_command()
    assert app.redo_stack == [first]

    app.stage_edit_command(second)

    assert app.redo_stack == []
