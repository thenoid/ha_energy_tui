"""TUI editor for Home Assistant Energy dashboard device consumption.

Uses only the Home Assistant websocket API. No direct .storage edits.

Auth:
  export HASS_SERVER=https://ha.example:8123
  export HASS_TOKEN=...
  uv run ha_energy_tui.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp
from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    OptionList,
    Static,
    TabbedContent,
    TabPane,
    Tree,
)
from textual.widgets.option_list import Option

ENERGY_UNITS = {"Wh", "kWh", "MWh", "MJ", "GJ"}
WATER_UNITS = {"L", "mL", "m³", "ft³", "gal", "CCF"}
ENERGY_STATE_CLASSES = {"total", "total_increasing"}

MODES: dict[str, dict[str, Any]] = {
    "electric": {
        "label": "Electric",
        "pref_key": "device_consumption",
        "device_class": "energy",
        "units": ENERGY_UNITS,
        "tree_label": "Configured Electric Devices",
        "name_column": "Energy Name",
        "config_label": "Energy config",
    },
    "water": {
        "label": "Water",
        "pref_key": "device_consumption_water",
        "device_class": "water",
        "units": WATER_UNITS,
        "tree_label": "Configured Water Devices",
        "name_column": "Water Name",
        "config_label": "Water config",
    },
}
MODE_ORDER = ["electric", "water"]
TABLE_COLUMN_KEYS = [
    "enabled",
    "config_name",
    "entity_name",
    "parent",
    "entity",
    "value",
    "class",
]
FILTER_SCOPES: list[dict[str, Any]] = [
    {
        "label": "id/name",
        "entity_name": False,
        "parent": False,
    },
    {
        "label": "id/name/entity",
        "entity_name": True,
        "parent": False,
    },
    {
        "label": "id/name/parent",
        "entity_name": False,
        "parent": True,
    },
    {
        "label": "all",
        "entity_name": True,
        "parent": True,
    },
]


class WebSocketError(RuntimeError):
    pass


class HAClient:
    def __init__(self, server: str, token: str, verify_ssl: bool = False):
        self.server = server.rstrip("/")
        self.token = token
        self.verify_ssl = verify_ssl
        self.timeout = 30.0
        self.loop: asyncio.AbstractEventLoop | None = None
        self.loop_thread: threading.Thread | None = None
        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.reader_task: asyncio.Task[None] | None = None
        self.pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self.next_id = 1

    def __enter__(self) -> "HAClient":
        self._ensure_loop()
        self._run(self._connect())
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self.loop and self.loop.is_running():
            self._run(self._close_async())
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self.loop_thread:
            self.loop_thread.join(timeout=2)
        self.loop = None
        self.loop_thread = None

    def reconnect(self) -> None:
        self._run(self._reconnect_async())

    def command(self, msg_type: str, **payload: Any) -> Any:
        return self._run(self._command_async(msg_type, **payload))

    def command_with_reconnect(self, msg_type: str, **payload: Any) -> Any:
        try:
            return self.command(msg_type, **payload)
        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            BrokenPipeError,
            ConnectionError,
            OSError,
            WebSocketError,
        ):
            self.reconnect()
            return self.command(msg_type, **payload)

    def _ensure_loop(self) -> None:
        if self.loop and self.loop.is_running():
            return
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(
            target=self._run_loop,
            name="ha-aiohttp-loop",
            daemon=True,
        )
        self.loop_thread.start()

    def _run_loop(self) -> None:
        assert self.loop is not None
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _run(self, coro: Any) -> Any:
        self._ensure_loop()
        assert self.loop is not None
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=self.timeout + 5)

    def _ws_url(self) -> str:
        parsed = urlparse(self.server)
        if parsed.scheme not in {"http", "https"}:
            raise WebSocketError(f"Unsupported Home Assistant URL: {self.server}")
        scheme = "wss" if parsed.scheme == "https" else "ws"
        base = f"{scheme}://{parsed.netloc}"
        path = parsed.path.rstrip("/")
        return f"{base}{path}/api/websocket"

    async def _connect(self) -> None:
        await self._close_async()
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        connector = aiohttp.TCPConnector(ssl=self.verify_ssl)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        self.ws = await self.session.ws_connect(
            self._ws_url(),
            autoping=True,
            heartbeat=20,
            receive_timeout=None,
        )
        auth_required = await self._receive_json_direct()
        if auth_required.get("type") != "auth_required":
            raise WebSocketError(f"Unexpected auth message: {auth_required}")
        await self.ws.send_json({"type": "auth", "access_token": self.token})
        auth = await self._receive_json_direct()
        if auth.get("type") != "auth_ok":
            raise WebSocketError(f"Authentication failed: {auth}")
        self.reader_task = asyncio.create_task(self._reader_loop())

    async def _receive_json_direct(self) -> dict[str, Any]:
        if not self.ws:
            raise WebSocketError("Client is not connected")
        message = await self.ws.receive()
        if message.type == aiohttp.WSMsgType.TEXT:
            return json.loads(message.data)
        if message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED}:
            raise WebSocketError("Websocket closed")
        if message.type == aiohttp.WSMsgType.ERROR:
            raise WebSocketError(str(self.ws.exception()))
        raise WebSocketError(f"Unexpected websocket message type: {message.type}")

    async def _reader_loop(self) -> None:
        assert self.ws is not None
        try:
            async for message in self.ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    response = json.loads(message.data)
                    msg_id = response.get("id")
                    future = self.pending.pop(msg_id, None)
                    if future and not future.done():
                        future.set_result(response)
                elif message.type == aiohttp.WSMsgType.ERROR:
                    raise WebSocketError(str(self.ws.exception()))
        except Exception as err:  # noqa: BLE001 - fail any pending commands
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(err)
            self.pending.clear()

    async def _command_async(self, msg_type: str, **payload: Any) -> Any:
        if not self.ws or self.ws.closed:
            raise WebSocketError("Client is not connected")
        msg_id = self.next_id
        self.next_id += 1
        assert self.loop is not None
        future: asyncio.Future[dict[str, Any]] = self.loop.create_future()
        self.pending[msg_id] = future
        await self.ws.send_json({"id": msg_id, "type": msg_type, **payload})
        response = await asyncio.wait_for(future, timeout=self.timeout)
        if not response.get("success", False):
            raise WebSocketError(json.dumps(response.get("error", response)))
        return response.get("result")

    async def _reconnect_async(self) -> None:
        await self._connect()

    async def _close_async(self) -> None:
        if self.reader_task:
            self.reader_task.cancel()
            try:
                await self.reader_task
            except asyncio.CancelledError:
                pass
            self.reader_task = None
        for future in self.pending.values():
            if not future.done():
                future.cancel()
        self.pending.clear()
        if self.ws and not self.ws.closed:
            await self.ws.close()
        self.ws = None
        if self.session and not self.session.closed:
            await self.session.close()
        self.session = None


@dataclass
class EnergySensor:
    entity_id: str
    name: str
    unit: str
    state_class: str
    current_state: str
    configured: bool
    eligible: bool


@dataclass
class ParentChoice:
    label: str
    stat_consumption: str | None


@dataclass
class RenameValues:
    energy_name: str
    entity_name: str


CLEAR_PARENT = "__clear_parent__"
CANCEL_PARENT = "__cancel_parent__"
VISUAL_ROOT = "__visual_root__"
MISSING_PARENTS = "__missing_parents__"
AUDIT_LOG_PATH = Path("tui_ha.log")


class EnergyTree(Tree[str]):
    BINDINGS = [
        Binding("enter", "app.show_info", "Info"),
        Binding("space", "toggle_node", "Toggle"),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("h", "cursor_parent", "Parent", show=False),
        Binding("l", "toggle_node", "Toggle", show=False),
    ]


def env_default(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def mode_config(mode: str) -> dict[str, Any]:
    return MODES[mode]


def next_mode(mode: str) -> str:
    index = MODE_ORDER.index(mode)
    return MODE_ORDER[(index + 1) % len(MODE_ORDER)]


def prefs_data(prefs: dict[str, Any]) -> dict[str, Any]:
    return prefs.get("data", prefs)


def load_energy_sensors(
    states: list[dict[str, Any]], prefs: dict[str, Any], mode: str
) -> list[EnergySensor]:
    data = prefs_data(prefs)
    config = mode_config(mode)
    configured = {
        item.get("stat_consumption")
        for item in data.get(config["pref_key"], [])
        if item.get("stat_consumption")
    }
    sensors: list[EnergySensor] = []
    for state in states:
        entity_id = state.get("entity_id", "")
        if not entity_id.startswith("sensor."):
            continue
        attrs = state.get("attributes") or {}
        unit = attrs.get("unit_of_measurement") or ""
        state_class = attrs.get("state_class") or ""
        eligible = (
            attrs.get("device_class") == config["device_class"]
            and state_class in ENERGY_STATE_CLASSES
            and unit in config["units"]
        )
        if not eligible and entity_id not in configured:
            continue
        sensors.append(
            EnergySensor(
                entity_id=entity_id,
                name=attrs.get("friendly_name") or entity_id,
                unit=unit,
                state_class=state_class,
                current_state=state.get("state", ""),
                configured=entity_id in configured,
                eligible=eligible,
            )
        )
    return sorted(sensors, key=lambda sensor: (not sensor.configured, sensor.name.lower()))


def load_device_configs(prefs: dict[str, Any], mode: str) -> dict[str, dict[str, Any]]:
    data = prefs_data(prefs)
    return {
        item["stat_consumption"]: dict(item)
        for item in data.get(mode_config(mode)["pref_key"], [])
        if item.get("stat_consumption")
    }


def count_device_config_changes(
    saved_configs: dict[str, dict[str, Any]],
    staged_configs: dict[str, dict[str, Any]],
) -> int:
    return sum(
        1
        for entity_id in set(saved_configs) | set(staged_configs)
        if saved_configs.get(entity_id) != staged_configs.get(entity_id)
    )


def device_config_changes(
    saved_configs: dict[str, dict[str, Any]],
    staged_configs: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        entity_id: {
            "old": saved_configs.get(entity_id),
            "new": staged_configs.get(entity_id),
        }
        for entity_id in sorted(set(saved_configs) | set(staged_configs))
        if saved_configs.get(entity_id) != staged_configs.get(entity_id)
    }


def audit_log_line(action: str, **fields: Any) -> str:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    details = " ".join(
        f"{key}={json.dumps(value, sort_keys=True)}" for key, value in fields.items()
    )
    return f"{timestamp} {action}{' ' + details if details else ''}\n"


def write_audit_log(action: str, **fields: Any) -> None:
    try:
        with AUDIT_LOG_PATH.open("a", encoding="utf-8") as log_file:
            log_file.write(audit_log_line(action, **fields))
    except OSError:
        pass


def _load_registry(client: HAClient, command: str, key: str) -> dict[str, dict[str, Any]]:
    try:
        entries = client.command_with_reconnect(command)
    except Exception:
        return {}
    return {entry[key]: entry for entry in entries if entry.get(key)}


def load_entity_registry(client: HAClient) -> dict[str, dict[str, Any]]:
    return _load_registry(client, "config/entity_registry/list", "entity_id")


def load_device_registry(client: HAClient) -> dict[str, dict[str, Any]]:
    return _load_registry(client, "config/device_registry/list", "id")


def save_device_consumption(
    client: HAClient,
    prefs: dict[str, Any],
    device_configs: dict[str, dict[str, Any]],
    mode: str,
) -> Any:
    data = prefs_data(prefs)
    new_consumption = [device_configs[entity_id] for entity_id in sorted(device_configs)]
    payload = dict(data)
    payload[mode_config(mode)["pref_key"]] = new_consumption
    return client.command_with_reconnect("energy/save_prefs", **payload)


def sensor_label(entity_id: str, sensors_by_id: dict[str, EnergySensor]) -> str:
    sensor = sensors_by_id.get(entity_id)
    return sensor.name if sensor else entity_id


def entity_registry_name(
    entity_id: str,
    sensors_by_id: dict[str, EnergySensor],
    entity_registry: dict[str, dict[str, Any]],
) -> str:
    entry = entity_registry.get(entity_id, {})
    return entry.get("name") or sensor_label(entity_id, sensors_by_id)


def staged_entity_registry_name(
    entity_id: str,
    sensors_by_id: dict[str, EnergySensor],
    entity_registry: dict[str, dict[str, Any]],
    entity_name_updates: dict[str, str | None],
) -> str:
    if entity_id in entity_name_updates:
        return entity_name_updates[entity_id] or sensor_label(entity_id, sensors_by_id)
    return entity_registry_name(entity_id, sensors_by_id, entity_registry)


def device_registry_label(device: dict[str, Any] | None) -> str:
    if not device:
        return "<none>"
    name = (
        device.get("name_by_user")
        or device.get("name")
        or device.get("model")
        or device.get("id")
    )
    detail_parts = [
        part
        for part in [device.get("manufacturer"), device.get("model")]
        if part
    ]
    detail = f" ({' / '.join(detail_parts)})" if detail_parts else ""
    return f"{name}{detail}"


def device_label(
    entity_id: str,
    sensors_by_id: dict[str, EnergySensor],
    device_configs: dict[str, dict[str, Any]],
) -> str:
    config = device_configs.get(entity_id, {})
    name = config.get("name") or sensor_label(entity_id, sensors_by_id)
    return f"{name} ({entity_id})"


def build_device_tree_lines(
    sensors: list[EnergySensor],
    device_configs: dict[str, dict[str, Any]],
) -> list[str]:
    sensors_by_id = {sensor.entity_id: sensor for sensor in sensors}
    children: dict[str, list[str]] = {entity_id: [] for entity_id in device_configs}
    roots: list[str] = []
    for entity_id, config in device_configs.items():
        parent = config.get("included_in_stat")
        if parent and parent in device_configs and parent != entity_id:
            children.setdefault(parent, []).append(entity_id)
        else:
            roots.append(entity_id)

    def sort_key(entity_id: str) -> str:
        return device_label(entity_id, sensors_by_id, device_configs).lower()

    for child_list in children.values():
        child_list.sort(key=sort_key)
    roots.sort(key=sort_key)

    lines: list[str] = []
    visiting: set[str] = set()

    def walk(entity_id: str, prefix: str, is_last: bool) -> None:
        connector = "`-- " if is_last else "|-- "
        label = device_label(entity_id, sensors_by_id, device_configs)
        if entity_id in visiting:
            lines.append(f"{prefix}{connector}{label}  [cycle]")
            return
        lines.append(f"{prefix}{connector}{label}")
        visiting.add(entity_id)
        child_prefix = prefix + ("    " if is_last else "|   ")
        child_ids = children.get(entity_id, [])
        for index, child_id in enumerate(child_ids):
            walk(child_id, child_prefix, index == len(child_ids) - 1)
        visiting.remove(entity_id)

    for index, root in enumerate(roots):
        walk(root, "", index == len(roots) - 1)

    orphans = sorted(
        entity_id
        for entity_id, config in device_configs.items()
        if config.get("included_in_stat")
        and config["included_in_stat"] not in device_configs
    )
    if orphans:
        lines.append("")
        lines.append("Missing parent references:")
        for entity_id in orphans:
            parent = device_configs[entity_id]["included_in_stat"]
            lines.append(f"- {device_label(entity_id, sensors_by_id, device_configs)} -> {parent}")

    return lines or ["No configured device consumption sensors."]


def filter_sensors(
    sensors: list[EnergySensor],
    query: str,
    *,
    device_configs: dict[str, dict[str, Any]],
    entity_registry: dict[str, dict[str, Any]],
    entity_name_updates: dict[str, str | None],
    include_entity_name: bool,
    include_parent: bool,
) -> list[EnergySensor]:
    if not query:
        return sensors
    needle = query.lower()
    sensors_by_id = (
        {sensor.entity_id: sensor for sensor in sensors}
        if (include_entity_name or include_parent)
        else {}
    )
    matches: list[EnergySensor] = []
    for sensor in sensors:
        config = device_configs.get(sensor.entity_id, {})
        haystack = [sensor.entity_id, sensor.name, config.get("name", "")]
        if include_entity_name:
            haystack.append(
                staged_entity_registry_name(
                    sensor.entity_id,
                    sensors_by_id,
                    entity_registry,
                    entity_name_updates,
                )
            )
        if include_parent:
            parent = config.get("included_in_stat")
            if parent:
                haystack.append(device_label(parent, sensors_by_id, device_configs))
                haystack.append(parent)
        if any(needle in value.lower() for value in haystack if value):
            matches.append(sensor)
    return matches


def join_words(words: list[str]) -> str:
    if len(words) <= 2:
        return " and ".join(words)
    return f"{', '.join(words[:-1])}, and {words[-1]}"


class EnergyTable(DataTable):
    BINDINGS = [
        Binding("enter", "app.show_info", "Info"),
        Binding("space", "app.toggle_device", "Toggle"),
        Binding("o", "app.sort_selected_column", "Sort"),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("h", "cursor_left", "Left", show=False),
        Binding("l", "cursor_right", "Right", show=False),
    ]


class ParentPicker(ModalScreen[str]):
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("q", "cancel", "Cancel"),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("pagedown", "page_down", "Page Down", show=False),
        Binding("pageup", "page_up", "Page Up", show=False),
    ]

    CSS = """
    ParentPicker {
        align: center middle;
    }

    #parent-dialog {
        width: 88%;
        height: 80%;
        border: round $primary;
        background: $surface;
        padding: 1 2;
    }

    #parent-list {
        height: 1fr;
    }

    #parent-filter {
        margin: 1 0;
    }
    """

    def __init__(
        self, choices: list[ParentChoice], current_parent: str | None = None
    ) -> None:
        super().__init__()
        self.choices = choices
        self.current_parent = current_parent
        self.filter_text = ""

    def compose(self) -> ComposeResult:
        with Container(id="parent-dialog"):
            yield Static("Select Parent Device")
            yield Static("Type to filter. Enter selects. Choose <none> to clear parent.")
            yield Input(placeholder="Filter parent devices", id="parent-filter")
            yield OptionList(id="parent-list")

    def on_mount(self) -> None:
        self.refresh_options()
        self.query_one("#parent-filter", Input).focus()

    @on(Input.Changed, "#parent-filter")
    def filter_changed(self, event: Input.Changed) -> None:
        self.filter_text = event.value
        self.refresh_options()

    def refresh_options(self) -> None:
        option_list = self.query_one("#parent-list", OptionList)
        option_list.clear_options()
        needle = self.filter_text.lower()
        filtered_choices = [
            choice
            for choice in self.choices
            if choice.stat_consumption is None
            or not needle
            or needle in choice.label.lower()
            or needle in choice.stat_consumption.lower()
        ]
        option_list.add_options(
            [
                Option(
                    choice.label
                    if choice.stat_consumption is None
                    else f"{choice.label}  ({choice.stat_consumption})",
                    id=choice.stat_consumption or CLEAR_PARENT,
                )
                for choice in filtered_choices
            ]
        )
        wanted = self.current_parent or CLEAR_PARENT
        for index, option in enumerate(option_list.options):
            if option.id == wanted:
                option_list.highlighted = index
                break
        else:
            option_list.highlighted = 0 if option_list.option_count else None

    def action_cancel(self) -> None:
        self.dismiss(CANCEL_PARENT)

    @on(OptionList.OptionSelected)
    def option_selected(self, event: OptionList.OptionSelected) -> None:
        self.dismiss(event.option_id or CLEAR_PARENT)

    @on(Input.Submitted, "#parent-filter")
    def filter_submitted(self, _event: Input.Submitted) -> None:
        option_list = self.query_one("#parent-list", OptionList)
        option_list.focus()

    def action_page_down(self) -> None:
        option_list = self.query_one("#parent-list", OptionList)
        option_list.action_page_down()

    def action_page_up(self) -> None:
        option_list = self.query_one("#parent-list", OptionList)
        option_list.action_page_up()

    def action_cursor_down(self) -> None:
        option_list = self.query_one("#parent-list", OptionList)
        option_list.action_cursor_down()

    def action_cursor_up(self) -> None:
        option_list = self.query_one("#parent-list", OptionList)
        option_list.action_cursor_up()


class DeviceInfo(ModalScreen[None]):
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("q", "close", "Close"),
        Binding("enter", "close", "Close"),
    ]

    CSS = """
    DeviceInfo {
        align: center middle;
    }

    #info-dialog {
        width: 86%;
        max-width: 120;
        height: auto;
        max-height: 80%;
        border: round $primary;
        background: $surface;
        padding: 1 2;
    }

    #info-body {
        width: 1fr;
    }
    """

    def __init__(self, title: str, body: str) -> None:
        super().__init__()
        self.info_title = title
        self.body = body

    def compose(self) -> ComposeResult:
        with Container(id="info-dialog"):
            yield Static(self.info_title)
            yield Static(self.body, id="info-body")
            yield Static("Enter, q, or Esc closes")

    def action_close(self) -> None:
        self.dismiss()


class Notice(ModalScreen[None]):
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("q", "close", "Close"),
        Binding("enter", "close", "Close"),
    ]

    CSS = """
    Notice {
        align: center middle;
    }

    #notice-dialog {
        width: 72%;
        max-width: 96;
        height: auto;
        border: round $primary;
        background: $surface;
        padding: 1 2;
    }
    """

    def __init__(self, message: str) -> None:
        super().__init__()
        self.message = message

    def compose(self) -> ComposeResult:
        with Container(id="notice-dialog"):
            yield Static(self.message)
            yield Static("Enter, q, or Esc closes")

    def action_close(self) -> None:
        self.dismiss()


class ConfirmRefresh(ModalScreen[bool]):
    BINDINGS = [
        Binding("escape", "cancel", "No"),
        Binding("n", "cancel", "No"),
        Binding("y", "confirm", "Yes"),
    ]

    CSS = """
    ConfirmRefresh {
        align: center middle;
    }

    #confirm-refresh-dialog {
        width: 72%;
        max-width: 88;
        height: auto;
        border: round $warning;
        background: $surface;
        padding: 1 2;
    }

    #confirm-refresh-actions {
        margin-top: 1;
        height: auto;
    }

    #confirm-refresh-actions Button {
        margin-right: 1;
    }
    """

    def compose(self) -> ComposeResult:
        with Container(id="confirm-refresh-dialog"):
            yield Static(
                "You have unstaged changes. Refreshing will wipe them out."
            )
            with Horizontal(id="confirm-refresh-actions"):
                yield Button("Yes", id="refresh-yes", variant="warning")
                yield Button("No", id="refresh-no", variant="primary")

    def on_mount(self) -> None:
        self.query_one("#refresh-no", Button).focus()

    @on(Button.Pressed, "#refresh-yes")
    def yes_pressed(self, _event: Button.Pressed) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#refresh-no")
    def no_pressed(self, _event: Button.Pressed) -> None:
        self.dismiss(False)

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


class DirtyQuit(ModalScreen[str]):
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("c", "cancel", "Cancel"),
        Binding("s", "save", "Save"),
        Binding("y", "save", "Save"),
        Binding("d", "discard", "Discard"),
        Binding("n", "discard", "Discard"),
    ]

    CSS = """
    DirtyQuit {
        align: center middle;
    }

    #dirty-quit-dialog {
        width: 72%;
        max-width: 88;
        height: auto;
        border: round $warning;
        background: $surface;
        padding: 1 2;
    }

    #dirty-quit-actions {
        margin-top: 1;
        height: auto;
    }

    #dirty-quit-actions Button {
        margin-right: 1;
    }
    """

    def compose(self) -> ComposeResult:
        with Container(id="dirty-quit-dialog"):
            yield Static("You have unsaved changes. Save before quitting?")
            with Horizontal(id="dirty-quit-actions"):
                yield Button("Save", id="quit-save", variant="success")
                yield Button("Discard", id="quit-discard", variant="warning")
                yield Button("Cancel", id="quit-cancel", variant="primary")

    def on_mount(self) -> None:
        self.query_one("#quit-cancel", Button).focus()

    @on(Button.Pressed, "#quit-save")
    def save_pressed(self, _event: Button.Pressed) -> None:
        self.dismiss("save")

    @on(Button.Pressed, "#quit-discard")
    def discard_pressed(self, _event: Button.Pressed) -> None:
        self.dismiss("discard")

    @on(Button.Pressed, "#quit-cancel")
    def cancel_pressed(self, _event: Button.Pressed) -> None:
        self.dismiss("cancel")

    def action_save(self) -> None:
        self.dismiss("save")

    def action_discard(self) -> None:
        self.dismiss("discard")

    def action_cancel(self) -> None:
        self.dismiss("cancel")


class TextPrompt(ModalScreen[str | None]):
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+c", "cancel", "Cancel"),
    ]

    CSS = """
    TextPrompt {
        align: center middle;
    }

    #prompt-dialog {
        width: 72%;
        max-width: 96;
        height: auto;
        border: round $primary;
        background: $surface;
        padding: 1 2;
    }

    #prompt-input {
        margin-top: 1;
    }
    """

    def __init__(self, title: str, value: str = "") -> None:
        super().__init__()
        self.prompt_title = title
        self.value = value

    def compose(self) -> ComposeResult:
        with Container(id="prompt-dialog"):
            yield Static(self.prompt_title)
            yield Input(value=self.value, id="prompt-input")
            yield Static("Enter saves. Esc cancels. Empty clears custom name.")

    def on_mount(self) -> None:
        prompt_input = self.query_one("#prompt-input", Input)
        prompt_input.focus()

    @on(Input.Submitted, "#prompt-input")
    def input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value)

    def action_cancel(self) -> None:
        self.dismiss(None)


class RenamePrompt(ModalScreen[RenameValues | None]):
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+c", "cancel", "Cancel"),
    ]

    CSS = """
    RenamePrompt {
        align: center middle;
    }

    #rename-dialog {
        width: 82%;
        max-width: 112;
        height: auto;
        border: round $primary;
        background: $surface;
        padding: 1 2;
    }

    .rename-input {
        margin-bottom: 1;
    }
    """

    def __init__(
        self,
        entity_id: str,
        energy_name: str,
        entity_name: str,
    ) -> None:
        super().__init__()
        self.entity_id = entity_id
        self.energy_name = energy_name
        self.entity_name = entity_name

    def compose(self) -> ComposeResult:
        with Container(id="rename-dialog"):
            yield Static("Rename")
            yield Static(f"Entity id: {self.entity_id}")
            yield Static("Energy dashboard label")
            yield Input(
                value=self.energy_name,
                id="energy-name-input",
                classes="rename-input",
            )
            yield Static("Home Assistant entity display name")
            yield Input(
                value=self.entity_name,
                id="entity-name-input",
                classes="rename-input",
            )
            yield Static("Enter saves staged names. Esc cancels. Empty clears that custom name.")

    def on_mount(self) -> None:
        self.query_one("#energy-name-input", Input).focus()

    @on(Input.Submitted)
    def input_submitted(self, _event: Input.Submitted) -> None:
        self.dismiss(
            RenameValues(
                energy_name=self.query_one("#energy-name-input", Input).value,
                entity_name=self.query_one("#entity-name-input", Input).value,
            )
        )

    def action_cancel(self) -> None:
        self.dismiss(None)


class EnergyConfiguratorApp(App[None]):
    TITLE = "HA Energy Device Configurator"
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("p", "pick_parent", "Parent"),
        Binding("n", "rename_device", "Rename"),
        Binding("m", "switch_mode", "Mode"),
        Binding("a", "toggle_autosave", "Autosave"),
        Binding("s", "save", "Save"),
        Binding("v", "validate", "Validate"),
        Binding("r", "refresh", "Refresh"),
        Binding("i", "show_info", "Info"),
        Binding("u", "toggle_parentless_filter", "Parentless"),
        Binding("f", "cycle_filter_scope", "Filter Scope"),
        Binding("/", "focus_filter", "Filter"),
        Binding("[", "previous_tab", "Prev Tab"),
        Binding("]", "next_tab", "Next Tab"),
        Binding("pagedown", "page_down", "Page Down", show=False),
        Binding("pageup", "page_up", "Page Up", show=False),
    ]

    CSS = """
    Screen {
        layout: vertical;
    }

    #autosave-indicator {
        dock: right;
        width: auto;
        height: 1;
        padding: 0 1;
        color: $text;
    }

    #filter {
        width: 1fr;
    }

    #filter-bar {
        dock: top;
        height: 3;
    }

    #filter-feedback {
        width: auto;
        min-width: 26;
        height: 3;
        content-align: right middle;
        padding: 0 1;
        background: $panel;
        color: $text-muted;
    }

    #table {
        height: 1fr;
    }

    #status {
        dock: bottom;
        min-height: 1;
        padding: 0 1;
        background: $panel;
    }

    #dirty-indicator {
        dock: bottom;
        min-height: 1;
        padding: 0 1;
        background: $warning;
        color: $text;
        text-style: bold;
    }

    #visual {
        height: 1fr;
        padding: 1 2;
    }
    """

    def __init__(
        self,
        client: HAClient,
        prefs: dict[str, Any],
        sensors: list[EnergySensor],
        entity_registry: dict[str, dict[str, Any]],
        device_registry: dict[str, dict[str, Any]],
        mode: str = "electric",
    ) -> None:
        super().__init__()
        self.client = client
        self.prefs = prefs
        self.mode = mode
        self.sensors = sensors
        self.entity_registry = entity_registry
        self.device_registry = device_registry
        self.device_configs_by_mode = {
            configured_mode: load_device_configs(prefs, configured_mode)
            for configured_mode in MODE_ORDER
        }
        self.entity_name_updates: dict[str, str | None] = {}
        self.dirty_modes: set[str] = set()
        self.autosave = False
        self.autosave_timer: Any = None
        self.saving = False
        self.filtered: list[EnergySensor] = []
        self.filter_text = ""
        self.filter_scope_index = 0
        self.sort_column_key: str | None = None
        self.sort_desc = False
        self.parentless_only = False
        self.status = ""

    @property
    def device_configs(self) -> dict[str, dict[str, Any]]:
        return self.device_configs_by_mode[self.mode]

    @device_configs.setter
    def device_configs(self, value: dict[str, dict[str, Any]]) -> None:
        self.device_configs_by_mode[self.mode] = value

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(initial="config"):
            with TabPane("Configure", id="config"):
                with Horizontal(id="filter-bar"):
                    yield Input(placeholder="Filter sensors by name or entity id", id="filter")
                    yield Static(id="filter-feedback")
                yield EnergyTable(id="table")
            with TabPane("Visual", id="visual-tab"):
                yield EnergyTree("Configured Energy Devices", id="visual")
        yield Static("UNSAVED CHANGES", id="dirty-indicator")
        yield Static(id="status")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(Header).mount(Static("", id="autosave-indicator"))
        table = self.table
        table.cursor_type = "cell"
        table.zebra_stripes = True
        table.add_columns(*self.table_columns())
        self.dirty_indicator.display = self.has_unsaved_changes()
        self.status = (
            f"{self.mode_status_prefix()} | Loaded {len(self.sensors)} sensors; "
            f"{len(self.device_configs)} configured."
        )
        self.refresh_table()
        table.focus()

    def _modal_active(self) -> bool:
        return isinstance(
            self.screen,
            (DeviceInfo, Notice, ConfirmRefresh, DirtyQuit, ParentPicker, TextPrompt, RenamePrompt),
        )

    @property
    def _save_hint(self) -> str:
        return "Autosaving." if self.autosave else "Press s to save."

    def _ensure_configured(self, entity_id: str) -> None:
        if entity_id not in self.device_configs:
            self.device_configs[entity_id] = {"stat_consumption": entity_id}

    @property
    def table(self) -> DataTable:
        return self.query_one("#table", EnergyTable)

    @property
    def status_widget(self) -> Static:
        return self.query_one("#status", Static)

    @property
    def filter_feedback(self) -> Static:
        return self.query_one("#filter-feedback", Static)

    @property
    def dirty_indicator(self) -> Static:
        return self.query_one("#dirty-indicator", Static)

    @property
    def autosave_indicator(self) -> Static:
        return self.query_one("#autosave-indicator", Static)

    @property
    def visual_widget(self) -> EnergyTree:
        return self.query_one("#visual", EnergyTree)

    def table_columns(self) -> list[tuple[str, str]]:
        labels = [
            "On",
            mode_config(self.mode)["name_column"],
            "HA Entity Name",
            "Parent",
            "Entity",
            "Value",
            "Class",
        ]
        if self.sort_column_key in TABLE_COLUMN_KEYS:
            index = TABLE_COLUMN_KEYS.index(self.sort_column_key)
            labels[index] = f"{labels[index]} {'desc' if self.sort_desc else 'asc'}"
        return list(zip(labels, TABLE_COLUMN_KEYS))

    def has_unsaved_changes(self) -> bool:
        return bool(self.dirty_modes or self.entity_name_updates)

    @on(TabbedContent.TabActivated)
    def tab_activated(self, event: TabbedContent.TabActivated) -> None:
        if event.pane.id == "visual-tab":
            self.refresh_visual()
            self.visual_widget.focus()
        elif event.pane.id == "config":
            self.table.focus()

    def switch_tab(self, direction: int) -> None:
        if self._modal_active():
            return
        tabs = ["config", "visual-tab"]
        tabbed_content = self.query_one(TabbedContent)
        current_index = tabs.index(tabbed_content.active or "config")
        tabbed_content.active = tabs[(current_index + direction) % len(tabs)]

    def action_previous_tab(self) -> None:
        self.switch_tab(-1)

    def action_next_tab(self) -> None:
        self.switch_tab(1)

    def action_quit(self) -> None:
        if self._modal_active():
            return
        if self.has_unsaved_changes():
            self.push_screen(DirtyQuit(), self.handle_dirty_quit)
        else:
            self.exit()

    def handle_dirty_quit(self, choice: str) -> None:
        if choice == "save":
            self.save_all_dirty()
            if not self.has_unsaved_changes():
                self.exit()
        elif choice == "discard":
            self.exit()
        else:
            self.status = f"{self.mode_status_prefix()} | Quit cancelled."
            self.refresh_table()

    def save_all_dirty(self) -> None:
        modes = list(self.dirty_modes)
        if self.mode in modes:
            modes.remove(self.mode)
            modes.insert(0, self.mode)
        if not modes and self.entity_name_updates:
            modes = [self.mode]
        for mode in modes:
            self.save_mode(mode, show_notice=False)
            if mode in self.dirty_modes:
                break

    @on(Input.Changed, "#filter")
    def filter_changed(self, event: Input.Changed) -> None:
        self.filter_text = event.value
        self.refresh_table()

    @property
    def filter_scope(self) -> dict[str, Any]:
        return FILTER_SCOPES[self.filter_scope_index]

    def filter_feedback_text(self) -> str:
        filters = ["id/name"]
        if self.filter_scope["entity_name"]:
            filters.append("HA entity name")
        if self.filter_scope["parent"]:
            filters.append("parent")
        if self.parentless_only:
            filters.append("parentless only")
        return f"Filtering on {join_words(filters)}"

    def status_text(self) -> Text:
        text = Text(self.status)
        start = self.status.find("Autosave")
        if start >= 0:
            end = start + len("Autosave")
            if self.autosave:
                text.stylize("bold green", start, end)
            else:
                text.stylize("dim", start, end)
        return text

    def refresh_table(self) -> None:
        table = self.table
        current_entity = self.current_entity_id()
        current_column = max(0, min(table.cursor_column, len(TABLE_COLUMN_KEYS) - 1))
        table.clear()
        scope = self.filter_scope
        self.filtered = filter_sensors(
            self.sensors,
            self.filter_text,
            device_configs=self.device_configs,
            entity_registry=self.entity_registry,
            entity_name_updates=self.entity_name_updates,
            include_entity_name=scope["entity_name"],
            include_parent=scope["parent"],
        )
        if self.parentless_only:
            self.filtered = [
                sensor
                for sensor in self.filtered
                if sensor.entity_id in self.device_configs
                and not self.device_configs[sensor.entity_id].get("included_in_stat")
            ]
        sensors_by_id = {sensor.entity_id: sensor for sensor in self.sensors}
        if self.sort_column_key:
            self.filtered.sort(
                key=lambda sensor: self.sort_value(sensor, sensors_by_id),
                reverse=self.sort_desc,
            )
        for sensor in self.filtered:
            config = self.device_configs.get(sensor.entity_id)
            parent = ""
            if config and config.get("included_in_stat"):
                parent = device_label(
                    config["included_in_stat"], sensors_by_id, self.device_configs
                )
            table.add_row(
                "x" if config else "",
                config.get("name", "") if config else "",
                staged_entity_registry_name(
                    sensor.entity_id,
                    sensors_by_id,
                    self.entity_registry,
                    self.entity_name_updates,
                ),
                parent,
                sensor.entity_id,
                f"{sensor.current_state} {sensor.unit}".strip(),
                sensor.state_class if sensor.eligible else f"{sensor.state_class} !",
                key=sensor.entity_id,
            )
        if current_entity:
            for index, sensor in enumerate(self.filtered):
                if sensor.entity_id == current_entity:
                    table.move_cursor(row=index, column=current_column)
                    break
        else:
            table.move_cursor(column=current_column)
        self.status_widget.update(self.status_text())
        self.filter_feedback.update(self.filter_feedback_text())
        self.dirty_indicator.display = self.has_unsaved_changes()
        if self.query_one(TabbedContent).active == "visual-tab":
            self.refresh_visual()

    def sort_value(
        self,
        sensor: EnergySensor,
        sensors_by_id: dict[str, EnergySensor],
    ) -> tuple[int, Any]:
        config = self.device_configs.get(sensor.entity_id)
        value = ""
        if self.sort_column_key == "enabled":
            value = "x" if config else ""
        elif self.sort_column_key == "config_name":
            value = config.get("name", "") if config else ""
        elif self.sort_column_key == "entity_name":
            value = staged_entity_registry_name(
                sensor.entity_id,
                sensors_by_id,
                self.entity_registry,
                self.entity_name_updates,
            )
        elif self.sort_column_key == "parent":
            parent = config.get("included_in_stat") if config else None
            value = device_label(parent, sensors_by_id, self.device_configs) if parent else ""
        elif self.sort_column_key == "entity":
            value = sensor.entity_id
        elif self.sort_column_key == "value":
            try:
                return (0, float(sensor.current_state))
            except ValueError:
                value = sensor.current_state
        elif self.sort_column_key == "class":
            value = sensor.state_class
        return (1, str(value).lower())

    def mode_status_prefix(self) -> str:
        suffix = " | Parentless only" if self.parentless_only else ""
        autosave = "on" if self.autosave else "off"
        dirty = " | Unsaved" if self.mode in self.dirty_modes else ""
        return (
            f"Mode: {mode_config(self.mode)['label']} | Autosave: {autosave}"
            f"{dirty} | Filter: {self.filter_scope['label']}{suffix}"
        )

    def mark_dirty(self, mode: str | None = None) -> None:
        dirty_mode = mode or self.mode
        self.dirty_modes.add(dirty_mode)
        if self.autosave:
            self.schedule_autosave()

    def schedule_autosave(self) -> None:
        if self.autosave_timer is not None:
            self.autosave_timer.stop()
        self.autosave_timer = self.set_timer(0.75, self.autosave_now)

    def autosave_now(self) -> None:
        if not self.autosave or self.saving or not self.dirty_modes:
            return
        mode = self.mode if self.mode in self.dirty_modes else sorted(self.dirty_modes)[0]
        self.save_mode(mode, show_notice=False)

    def action_toggle_autosave(self) -> None:
        if self._modal_active():
            return
        self.autosave = not self.autosave
        self.autosave_indicator.update("Autosave" if self.autosave else "")
        self.status = f"{self.mode_status_prefix()} | Autosave {'enabled' if self.autosave else 'disabled'}."
        if self.autosave and self.dirty_modes:
            self.schedule_autosave()
        self.refresh_table()

    def refresh_visual(self) -> None:
        tree = self.visual_widget
        tree.root.set_label(mode_config(self.mode)["tree_label"])
        tree.root.data = VISUAL_ROOT
        tree.root.remove_children()
        tree.root.expand()

        sensors_by_id = {sensor.entity_id: sensor for sensor in self.sensors}
        children: dict[str, list[str]] = {
            entity_id: [] for entity_id in self.device_configs
        }
        roots: list[str] = []
        missing_parents: list[str] = []
        for entity_id, config in self.device_configs.items():
            parent = config.get("included_in_stat")
            if parent and parent in self.device_configs and parent != entity_id:
                children.setdefault(parent, []).append(entity_id)
            else:
                roots.append(entity_id)
                if parent and parent not in self.device_configs:
                    missing_parents.append(entity_id)

        def sort_key(entity_id: str) -> str:
            return device_label(entity_id, sensors_by_id, self.device_configs).lower()

        for child_list in children.values():
            child_list.sort(key=sort_key)
        roots.sort(key=sort_key)
        missing_parents.sort(key=sort_key)

        visiting: set[str] = set()

        def add_node(parent_node: Any, entity_id: str) -> None:
            label = device_label(entity_id, sensors_by_id, self.device_configs)
            if entity_id in visiting:
                parent_node.add_leaf(f"{label} [cycle]", data=entity_id)
                return
            visiting.add(entity_id)
            child_ids = children.get(entity_id, [])
            node = parent_node.add(label, data=entity_id, expand=True)
            if not child_ids:
                node.allow_expand = False
            for child_id in child_ids:
                add_node(node, child_id)
            visiting.remove(entity_id)

        for root in roots:
            add_node(tree.root, root)

        if missing_parents:
            missing_node = tree.root.add(
                "Missing parent references", data=MISSING_PARENTS, expand=True
            )
            for entity_id in missing_parents:
                parent = self.device_configs[entity_id]["included_in_stat"]
                missing_node.add_leaf(
                    f"{device_label(entity_id, sensors_by_id, self.device_configs)} -> {parent}",
                    data=entity_id,
                )

    def current_entity_id(self) -> str | None:
        if not self.filtered:
            return None
        row = max(0, min(self.table.cursor_row, len(self.filtered) - 1))
        return self.filtered[row].entity_id

    def selected_entity_id(self) -> str | None:
        if self.query_one(TabbedContent).active == "visual-tab":
            entity_id = self.visual_widget.cursor_node.data
            if entity_id in {VISUAL_ROOT, MISSING_PARENTS}:
                return None
            return entity_id
        return self.current_entity_id()

    def action_focus_filter(self) -> None:
        self.query_one("#filter", Input).focus()

    def action_cycle_filter_scope(self) -> None:
        if self._modal_active():
            return
        self.filter_scope_index = (self.filter_scope_index + 1) % len(FILTER_SCOPES)
        self.status = f"{self.mode_status_prefix()} | Filter scope changed."
        self.refresh_table()

    def action_sort_selected_column(self) -> None:
        if self._modal_active():
            return
        if self.query_one(TabbedContent).active == "visual-tab":
            return
        table = self.table
        column_index = max(0, min(table.cursor_column, len(TABLE_COLUMN_KEYS) - 1))
        column_key = TABLE_COLUMN_KEYS[column_index]
        if self.sort_column_key == column_key:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_column_key = column_key
            self.sort_desc = False
        table.clear(columns=True)
        columns = self.table_columns()
        table.add_columns(*columns)
        direction = "desc" if self.sort_desc else "asc"
        column_label = columns[column_index][0].rsplit(" ", 1)[0]
        self.status = f"{self.mode_status_prefix()} | Sorted by {column_label} {direction}."
        self.refresh_table()
        self.table.move_cursor(column=column_index)

    def action_toggle_parentless_filter(self) -> None:
        if self._modal_active():
            return
        self.parentless_only = not self.parentless_only
        self.status = (
            f"{self.mode_status_prefix()} | "
            f"Parentless filter {'on' if self.parentless_only else 'off'}."
        )
        self.refresh_table()

    def action_switch_mode(self) -> None:
        if self._modal_active():
            return
        self.mode = next_mode(self.mode)
        self.sensors = load_energy_sensors(
            self.client.command_with_reconnect("get_states"), self.prefs, self.mode
        )
        table = self.table
        table.clear(columns=True)
        table.add_columns(*self.table_columns())
        self.status = (
            f"{self.mode_status_prefix()} | Loaded {len(self.sensors)} sensors; "
            f"{len(self.device_configs)} configured."
        )
        self.refresh_table()

    def action_page_down(self) -> None:
        if self.query_one(TabbedContent).active == "visual-tab":
            self.visual_widget.action_page_down()
        else:
            self.table.action_page_down()

    def action_page_up(self) -> None:
        if self.query_one(TabbedContent).active == "visual-tab":
            self.visual_widget.action_page_up()
        else:
            self.table.action_page_up()

    @on(Tree.NodeSelected, "#visual")
    def visual_node_selected(self, event: Tree.NodeSelected[str]) -> None:
        entity_id = event.node.data
        if not entity_id or entity_id in {VISUAL_ROOT, MISSING_PARENTS}:
            return
        self.show_device_info(entity_id)

    def action_toggle_device(self) -> None:
        if self._modal_active():
            return
        if self.query_one(TabbedContent).active == "visual-tab":
            node = self.visual_widget.cursor_node
            if node and node.children:
                node.toggle()
            return
        entity_id = self.current_entity_id()
        if not entity_id:
            return
        if entity_id in self.device_configs:
            previous_config = dict(self.device_configs[entity_id])
            del self.device_configs[entity_id]
            write_audit_log(
                "staged_disable_device",
                mode=self.mode,
                entity_id=entity_id,
                old=previous_config,
                new=None,
            )
        else:
            new_config = {"stat_consumption": entity_id}
            self.device_configs[entity_id] = new_config
            write_audit_log(
                "staged_enable_device",
                mode=self.mode,
                entity_id=entity_id,
                old=None,
                new=new_config,
            )
        self.mark_dirty()
        self.status = (
            f"{self.mode_status_prefix()} | Staged {len(self.device_configs)} device consumption sensors. "
            f"{self._save_hint}"
        )
        self.refresh_table()

    def action_show_info(self) -> None:
        if isinstance(self.screen, (DeviceInfo, Notice)):
            self.screen.dismiss()
            return
        if self._modal_active():
            return
        if self.query_one(TabbedContent).active == "visual-tab":
            entity_id = self.visual_widget.cursor_node.data
        else:
            entity_id = self.current_entity_id()
        if entity_id and entity_id not in {VISUAL_ROOT, MISSING_PARENTS}:
            self.show_device_info(entity_id)

    def show_device_info(self, entity_id: str) -> None:
        sensors_by_id = {sensor.entity_id: sensor for sensor in self.sensors}
        sensor = sensors_by_id.get(entity_id)
        config = self.device_configs.get(entity_id, {})
        entity_entry = self.entity_registry.get(entity_id, {})
        device_id = entity_entry.get("device_id")
        device_entry = self.device_registry.get(device_id) if device_id else None
        children = sorted(
            child_id
            for child_id, child_config in self.device_configs.items()
            if child_config.get("included_in_stat") == entity_id
        )
        parent = config.get("included_in_stat")
        lines = [
            f"Name: {config.get('name') or sensor_label(entity_id, sensors_by_id)}",
            f"Entity: {entity_id}",
            "HA entity name: "
            + staged_entity_registry_name(
                entity_id,
                sensors_by_id,
                self.entity_registry,
                self.entity_name_updates,
            ),
        ]
        if entity_entry:
            lines.append(f"Entity registry id: {entity_entry.get('id') or '<none>'}")
            lines.append(f"Platform: {entity_entry.get('platform') or '<none>'}")
            lines.append(f"Unique id: {entity_entry.get('unique_id') or '<none>'}")
            lines.append(f"Area id: {entity_entry.get('area_id') or '<none>'}")
            lines.append(f"Device id: {device_id or '<none>'}")
            lines.append(f"HA device: {device_registry_label(device_entry)}")
            if device_entry:
                lines.append(
                    f"Device area id: {device_entry.get('area_id') or '<none>'}"
                )
        if parent:
            lines.append(f"Parent: {device_label(parent, sensors_by_id, self.device_configs)}")
        else:
            lines.append("Parent: <none>")
        if config.get("stat_rate"):
            lines.append(f"Power/rate entity: {config['stat_rate']}")
        if sensor:
            lines.extend(
                [
                    f"Current value: {sensor.current_state} {sensor.unit}".strip(),
                    f"State class: {sensor.state_class}",
                    f"Eligible: {'yes' if sensor.eligible else 'no'}",
                ]
            )
        if children:
            lines.append("Children:")
            for child_id in children:
                lines.append(f"  - {device_label(child_id, sensors_by_id, self.device_configs)}")
        else:
            lines.append("Children: <none>")
        if parent:
            parent_sensor = sensors_by_id.get(parent)
            parent_config = self.device_configs.get(parent, {})
            parent_entity_entry = self.entity_registry.get(parent, {})
            parent_device_id = parent_entity_entry.get("device_id")
            parent_device_entry = (
                self.device_registry.get(parent_device_id) if parent_device_id else None
            )
            parent_parent = parent_config.get("included_in_stat")
            lines.append("")
            lines.append("Parent device details:")
            lines.append(
                f"  Energy name: {parent_config.get('name') or sensor_label(parent, sensors_by_id)}"
            )
            lines.append(
                "  HA entity name: "
                + staged_entity_registry_name(
                    parent,
                    sensors_by_id,
                    self.entity_registry,
                    self.entity_name_updates,
                )
            )
            lines.append(f"  Entity: {parent}")
            if parent_entity_entry:
                lines.append(
                    f"  Entity registry id: {parent_entity_entry.get('id') or '<none>'}"
                )
                lines.append(
                    f"  Platform: {parent_entity_entry.get('platform') or '<none>'}"
                )
                lines.append(
                    f"  Unique id: {parent_entity_entry.get('unique_id') or '<none>'}"
                )
                lines.append(
                    f"  Area id: {parent_entity_entry.get('area_id') or '<none>'}"
                )
                lines.append(f"  Device id: {parent_device_id or '<none>'}")
                lines.append(f"  HA device: {device_registry_label(parent_device_entry)}")
                if parent_device_entry:
                    lines.append(
                        f"  Device area id: {parent_device_entry.get('area_id') or '<none>'}"
                    )
            if parent_parent:
                lines.append(
                    f"  Parent: {device_label(parent_parent, sensors_by_id, self.device_configs)}"
                )
            else:
                lines.append("  Parent: <none>")
            if parent_config.get("stat_rate"):
                lines.append(f"  Power/rate entity: {parent_config['stat_rate']}")
            if parent_sensor:
                lines.append(
                    f"  Current value: {parent_sensor.current_state} {parent_sensor.unit}".strip()
                )
                lines.append(f"  State class: {parent_sensor.state_class}")
                lines.append(f"  Eligible: {'yes' if parent_sensor.eligible else 'no'}")
            if parent_config:
                lines.append(f"  {mode_config(self.mode)['config_label']}:")
                parent_config_text = json.dumps(parent_config, indent=2, sort_keys=True)
                lines.extend(f"    {line}" for line in parent_config_text.splitlines())
        if config:
            lines.append("")
            lines.append(f"Saved/staged {mode_config(self.mode)['config_label']}:")
            lines.append(json.dumps(config, indent=2, sort_keys=True))
        self.push_screen(DeviceInfo(device_label(entity_id, sensors_by_id, self.device_configs), "\n".join(lines)))

    def action_pick_parent(self) -> None:
        entity_id = self.selected_entity_id()
        if not entity_id:
            return
        self._ensure_configured(entity_id)
        sensors_by_id = {sensor.entity_id: sensor for sensor in self.sensors}
        choices = [ParentChoice("<none>", None)]
        choices.extend(
            ParentChoice(
                device_label(parent_entity, sensors_by_id, self.device_configs),
                parent_entity,
            )
            for parent_entity in sorted(
                self.device_configs,
                key=lambda item: device_label(
                    item, sensors_by_id, self.device_configs
                ).lower(),
            )
            if parent_entity != entity_id
        )
        current_parent = self.device_configs[entity_id].get("included_in_stat")
        self.push_screen(
            ParentPicker(choices, current_parent=current_parent),
            lambda parent: self.set_parent(entity_id, parent),
        )

    def action_rename_device(self) -> None:
        entity_id = self.selected_entity_id()
        if not entity_id:
            return
        self._ensure_configured(entity_id)
        sensors_by_id = {sensor.entity_id: sensor for sensor in self.sensors}
        energy_name = self.device_configs[entity_id].get("name", "")
        entity_name = self.entity_name_updates.get(
            entity_id,
            entity_registry_name(entity_id, sensors_by_id, self.entity_registry),
        )
        self.push_screen(
            RenamePrompt(entity_id, energy_name, entity_name or ""),
            lambda value: self.set_device_names(entity_id, value),
        )

    def set_device_names(self, entity_id: str, value: RenameValues | None) -> None:
        if value is None:
            self.status = "Rename cancelled."
            self.refresh_table()
            return

        energy_name = value.energy_name.strip()
        entity_name = value.entity_name.strip()
        previous_config = dict(self.device_configs[entity_id])
        if energy_name:
            self.device_configs[entity_id]["name"] = energy_name
        else:
            self.device_configs[entity_id].pop("name", None)

        registry_entry = self.entity_registry.get(entity_id, {})
        current_custom_entity_name = registry_entry.get("name") or ""
        current_display_entity_name = entity_registry_name(
            entity_id,
            {sensor.entity_id: sensor for sensor in self.sensors},
            self.entity_registry,
        )
        if entity_name == current_custom_entity_name or (
            not current_custom_entity_name and entity_name == current_display_entity_name
        ):
            self.entity_name_updates.pop(entity_id, None)
        else:
            self.entity_name_updates[entity_id] = entity_name or None

        write_audit_log(
            "staged_rename_device",
            mode=self.mode,
            entity_id=entity_id,
            old_config=previous_config,
            new_config=dict(self.device_configs[entity_id]),
            old_entity_name=current_custom_entity_name or current_display_entity_name,
            new_entity_name=entity_name or None,
        )
        self.mark_dirty()
        self.status = f"Renamed {entity_id}. {self._save_hint}"
        self.refresh_table()

    def set_parent(self, entity_id: str, parent: str) -> None:
        if parent == CANCEL_PARENT:
            self.status = "Parent selection cancelled."
        elif parent == CLEAR_PARENT:
            previous_parent = self.device_configs[entity_id].get("included_in_stat")
            self.device_configs[entity_id].pop("included_in_stat", None)
            write_audit_log(
                "staged_clear_parent",
                mode=self.mode,
                entity_id=entity_id,
                old_parent=previous_parent,
                new_parent=None,
            )
            self.mark_dirty()
            self.status = f"Cleared parent for {entity_id}. {self._save_hint}"
        else:
            previous_parent = self.device_configs[entity_id].get("included_in_stat")
            self.device_configs[entity_id]["included_in_stat"] = parent
            write_audit_log(
                "staged_set_parent",
                mode=self.mode,
                entity_id=entity_id,
                old_parent=previous_parent,
                new_parent=parent,
            )
            self.mark_dirty()
            self.status = f"Set parent for {entity_id}. {self._save_hint}"
        self.refresh_table()

    def action_refresh(self) -> None:
        if self._modal_active():
            return
        if self.dirty_modes or self.entity_name_updates:
            self.push_screen(ConfirmRefresh(), self.confirm_refresh)
            return
        self.perform_refresh()

    def confirm_refresh(self, confirmed: bool) -> None:
        if confirmed:
            self.perform_refresh()
        else:
            self.status = f"{self.mode_status_prefix()} | Refresh cancelled."
            self.refresh_table()

    def perform_refresh(self) -> None:
        try:
            self.prefs = self.client.command_with_reconnect("energy/get_prefs")
            self.sensors = load_energy_sensors(
                self.client.command_with_reconnect("get_states"), self.prefs, self.mode
            )
            self.entity_registry = load_entity_registry(self.client)
            self.device_registry = load_device_registry(self.client)
            self.device_configs_by_mode = {
                configured_mode: load_device_configs(self.prefs, configured_mode)
                for configured_mode in MODE_ORDER
            }
            self.entity_name_updates.clear()
            self.dirty_modes.clear()
            self.status = (
                f"{self.mode_status_prefix()} | Reloaded {len(self.sensors)} sensors; "
                f"{len(self.device_configs)} configured."
            )
        except Exception as err:  # noqa: BLE001 - show errors in status line
            self.status = f"Refresh failed: {err}"
        self.refresh_table()

    def action_validate(self) -> None:
        try:
            result = self.client.command_with_reconnect("energy/validate")
            self.status = (
                "Validation OK"
                if not result
                else f"Validation: {json.dumps(result)[:240]}"
            )
        except Exception as err:  # noqa: BLE001 - show errors in status line
            self.status = f"Validation failed: {err}"
        self.refresh_table()

    def save_mode(self, mode: str, show_notice: bool) -> None:
        previous_mode = self.mode
        self.saving = True
        saved_configs = load_device_configs(self.prefs, mode)
        staged_configs = self.device_configs_by_mode[mode]
        config_changes = device_config_changes(saved_configs, staged_configs)
        entity_name_changes = dict(sorted(self.entity_name_updates.items()))
        change_count = len(config_changes) + len(entity_name_changes)
        change_word = "change" if change_count == 1 else "changes"
        try:
            for entity_id, name in entity_name_changes.items():
                self.client.command_with_reconnect(
                    "config/entity_registry/update",
                    entity_id=entity_id,
                    name=name,
                )
            save_device_consumption(
                self.client,
                self.prefs,
                self.device_configs_by_mode[mode],
                mode,
            )
            self.prefs = self.client.command_with_reconnect("energy/get_prefs")
            self.sensors = load_energy_sensors(
                self.client.command_with_reconnect("get_states"), self.prefs, self.mode
            )
            self.entity_registry = load_entity_registry(self.client)
            self.device_registry = load_device_registry(self.client)
            self.device_configs_by_mode[mode] = load_device_configs(self.prefs, mode)
            self.entity_name_updates.clear()
            self.dirty_modes.discard(mode)
            saved_prefix = "Saved" if show_notice else "Autosaved"
            self.status = (
                f"{saved_prefix} {mode_config(mode)['label']}: "
                f"{change_count} {change_word}; "
                f"{len(self.device_configs_by_mode[mode])} configured."
            )
            write_audit_log(
                "saved_changes",
                mode=mode,
                autosave=not show_notice,
                config_changes=config_changes,
                entity_name_changes=entity_name_changes,
            )
            notice = self.status
        except Exception as err:  # noqa: BLE001 - show errors in status line
            self.dirty_modes.add(mode)
            self.status = f"{'Autosave' if not show_notice else 'Save'} failed: {err}"
            write_audit_log(
                "save_failed",
                mode=mode,
                autosave=not show_notice,
                error=str(err),
                config_changes=config_changes,
                entity_name_changes=entity_name_changes,
            )
            notice = self.status
        finally:
            self.saving = False
            self.mode = previous_mode
        self.refresh_table()
        if show_notice:
            self.push_screen(Notice(notice))

    def action_save(self) -> None:
        self.save_mode(self.mode, show_notice=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--server",
        default=env_default("HASS_SERVER", "HA_SERVER", "HA_URL", "HASS_URL")
        or "https://localhost:8123",
        help="Home Assistant base URL. Defaults to HASS_SERVER or https://localhost:8123.",
    )
    parser.add_argument(
        "--token",
        default=env_default("HASS_TOKEN", "HA_TOKEN"),
        help="Long-lived access token. Defaults to HASS_TOKEN or HA_TOKEN.",
    )
    parser.add_argument(
        "--verify-ssl",
        action="store_true",
        help="Verify TLS certificates. By default this is disabled for self-signed HA installs.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print eligible/configured energy sensors and exit.",
    )
    parser.add_argument(
        "--prefs-summary",
        action="store_true",
        help="Print a summary of HA Energy preference keys and exit.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run HA Energy validation and exit.",
    )
    args = parser.parse_args()

    if not args.token:
        print("Missing token. Set HASS_TOKEN or pass --token.", file=sys.stderr)
        return 2

    with HAClient(args.server, args.token, verify_ssl=args.verify_ssl) as client:
        if args.prefs_summary:
            prefs = client.command_with_reconnect("energy/get_prefs")
            data = prefs_data(prefs)
            print(
                json.dumps(
                    {
                        "top_keys": sorted(prefs.keys()),
                        "data_keys": sorted(data.keys()),
                        "device_consumption_count": len(data.get("device_consumption", [])),
                        "device_consumption_water_count": len(
                            data.get("device_consumption_water", [])
                        ),
                        "energy_sources_count": len(data.get("energy_sources", [])),
                    },
                    indent=2,
                )
            )
            return 0
        if args.validate:
            print(json.dumps(client.command_with_reconnect("energy/validate"), indent=2))
            return 0
        if args.list:
            prefs = client.command_with_reconnect("energy/get_prefs")
            sensors = load_energy_sensors(
                client.command_with_reconnect("get_states"), prefs, "electric"
            )
            for sensor in sensors:
                mark = "configured" if sensor.configured else "available"
                print(f"{mark:10} {sensor.entity_id:60} {sensor.name}")
            return 0
        prefs = client.command_with_reconnect("energy/get_prefs")
        sensors = load_energy_sensors(
            client.command_with_reconnect("get_states"), prefs, "electric"
        )
        entity_registry = load_entity_registry(client)
        device_registry = load_device_registry(client)
        EnergyConfiguratorApp(client, prefs, sensors, entity_registry, device_registry).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
