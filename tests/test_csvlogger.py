# Copyright (c) 2026 Vladimir Kozlov <vkozloff@gmail.com>
# SPDX-License-Identifier: MIT

"""Tests for CSVLogger configuration and reload behavior."""

import asyncio
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
import voluptuous as vol

from homeassistant import data_entry_flow
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from csvlogger import (
    CONFIG_SCHEMA,
    async_reload,
    async_setup,
    async_update_listener,
    async_unload_entry,
)
from csvlogger.config_flow import CSVLoggerConfigFlowHandler
from csvlogger.const import (
    CONF_COLUMNS,
    CONF_FILE_PATH,
    CONF_FILE_PATTERN,
    CONF_TABLES,
    CONF_TIME_INTERVAL,
    DOMAIN,
    CSVLOGGER_GATEWAY,
)
from csvlogger.gateway import CSVLoggerService, Gateway


VALID_CONFIG: dict[str, Any] = {
    DOMAIN: {
        CONF_TABLES: [
            {
                CONF_FILE_PATH: "/tmp/csvlogger",
                CONF_FILE_PATTERN: "%Y-%m-%d.csv",
                CONF_COLUMNS: [{"name": "sample", "template": "{{ 42 }}"}],
            }
        ]
    }
}


def test_config_schema_accepts_yaml_and_applies_defaults():
    validated = cast(dict[str, Any], CONFIG_SCHEMA(VALID_CONFIG))
    validated_domain = validated[DOMAIN]
    assert isinstance(validated_domain, dict)
    tables = validated_domain[CONF_TABLES]
    assert isinstance(tables, list)
    columns = tables[0][CONF_COLUMNS]
    assert isinstance(columns, list)
    column = columns[0]
    assert isinstance(column, dict)

    assert validated_domain[CONF_TIME_INTERVAL] == 60
    assert column["name"] == "sample"


@pytest.mark.parametrize(
    "config",
    [
        {DOMAIN: {CONF_TABLES: [{"file_pattern": "%Y.csv"}]}},
        {
            DOMAIN: {
                CONF_TABLES: [
                    {
                        CONF_FILE_PATH: "/tmp/csvlogger",
                        CONF_FILE_PATTERN: "%Y.csv",
                        CONF_COLUMNS: [{"name": "missing template"}],
                    }
                ]
            }
        },
        {DOMAIN: {CONF_TIME_INTERVAL: 0}},
    ],
)
def test_config_schema_rejects_malformed_yaml(config):
    with pytest.raises(vol.Invalid):
        CONFIG_SCHEMA(config)


@pytest.mark.asyncio
async def test_csvlogger_instances_own_their_processors(monkeypatch):
    first_processor = AsyncMock()
    second_processor = AsyncMock()
    processor_factory = Mock(side_effect=[first_processor, second_processor])
    monkeypatch.setattr("csvlogger.gateway.CSVFileProcessor", processor_factory)

    first = CSVLoggerService([VALID_CONFIG[DOMAIN][CONF_TABLES][0]], None)
    second = CSVLoggerService([VALID_CONFIG[DOMAIN][CONF_TABLES][0]], None)

    await first.execute()

    assert first._file_processors is not second._file_processors
    first_processor.execute.assert_awaited_once()
    second_processor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reload_reimports_validated_yaml(monkeypatch):
    config_entries = SimpleNamespace(
        flow=SimpleNamespace(async_init=AsyncMock())
    )
    hass = cast(
        HomeAssistant, SimpleNamespace(config_entries=config_entries)
    )
    read_config = AsyncMock(return_value=VALID_CONFIG)
    monkeypatch.setattr("csvlogger.async_integration_yaml_config", read_config)

    await async_reload(hass)

    read_config.assert_awaited_once_with(
        hass, DOMAIN, raise_on_failure=True
    )
    config_entries.flow.async_init.assert_awaited_once_with(
        DOMAIN,
        context={"source": "import"},
        data=VALID_CONFIG[DOMAIN],
    )


@pytest.mark.asyncio
async def test_setup_registers_reload_and_flush_handlers(monkeypatch):
    hass = cast(HomeAssistant, SimpleNamespace(services=Mock()))
    reload = AsyncMock()
    flush = AsyncMock()
    register_service = Mock()
    monkeypatch.setattr("csvlogger.async_reload", reload)
    monkeypatch.setattr("csvlogger.async_flush", flush)
    monkeypatch.setattr(
        "csvlogger.async_register_admin_service", register_service
    )
    await async_setup(hass, {})

    handlers = [
        call.args[3]
        for call in register_service.call_args_list
    ]
    assert len(handlers) == 2
    await handlers[0](Mock())
    await handlers[1](Mock())
    reload.assert_awaited_once_with(hass)
    flush.assert_awaited_once_with(hass)


@pytest.mark.asyncio
async def test_yaml_import_updates_existing_entry():
    existing_entry = SimpleNamespace(data={"old": True})
    config_entries = SimpleNamespace(
        async_update_entry=Mock(),
    )
    flow = Mock()
    flow.hass = cast(
        HomeAssistant, SimpleNamespace(config_entries=config_entries)
    )
    flow._async_current_entries.return_value = [existing_entry]
    new_data = VALID_CONFIG[DOMAIN]

    with pytest.raises(data_entry_flow.AbortFlow):
        await CSVLoggerConfigFlowHandler.async_step_import(flow, new_data)

    config_entries.async_update_entry.assert_called_once_with(
        existing_entry, data=new_data
    )


@pytest.mark.asyncio
async def test_entry_update_reloads_integration():
    config_entries = SimpleNamespace(async_reload=AsyncMock())
    hass = cast(
        HomeAssistant, SimpleNamespace(config_entries=config_entries)
    )
    entry = cast(ConfigEntry, SimpleNamespace(entry_id="csvlogger-entry"))

    await async_update_listener(hass, entry)

    config_entries.async_reload.assert_awaited_once_with(entry.entry_id)


@pytest.mark.asyncio
async def test_gateway_shutdown_waits_for_task_and_flushes():
    gateway = Gateway(None, SimpleNamespace())
    csv_file_service = AsyncMock()
    gateway._csv_file_service = csv_file_service
    gateway._data_logging_task = asyncio.create_task(
        gateway.data_logging_loop(60, csv_file_service)
    )
    await asyncio.sleep(0)

    await gateway.async_shutdown()

    assert gateway._data_logging_task is None
    csv_file_service.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_unload_stops_and_removes_gateway():
    entry = cast(ConfigEntry, SimpleNamespace(entry_id="csvlogger-entry"))
    gateway = SimpleNamespace(
        _config_entry=entry,
        async_shutdown=AsyncMock(),
    )
    hass = cast(
        HomeAssistant, SimpleNamespace(data={DOMAIN: {CSVLOGGER_GATEWAY: gateway}})
    )

    assert await async_unload_entry(hass, entry)

    gateway.async_shutdown.assert_awaited_once()
    assert CSVLOGGER_GATEWAY not in hass.data[DOMAIN]
