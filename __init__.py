"""The CSV Logger component."""

import logging

import voluptuous as vol

from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.reload import async_integration_yaml_config
from homeassistant.helpers.service import async_register_admin_service
from homeassistant.helpers.typing import ConfigType

from .const import (
    DOMAIN,
    CSVLOGGER_GATEWAY,
    CONF_TIME_INTERVAL,
    CONF_TABLES,
    CONF_FILE_PATH,
    CONF_FILE_PATTERN,
    CONF_COLUMNS
)

from .gateway import create_csvlogger_gateway, get_csv_file_service


COLUMN_SCHEMA = vol.Schema(
    {
        vol.Required('name'): cv.string,
        vol.Required('template'): cv.string,
    }
)

TABLE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_FILE_PATH): cv.string,
        vol.Required(CONF_FILE_PATTERN): cv.string,
        vol.Optional(CONF_COLUMNS, default=[]): vol.All(cv.ensure_list,
                                                        [COLUMN_SCHEMA]),
    }
)

CSVLOGGER_CONFIG_SCHEMA = vol.Schema(
    {
        vol.Optional(CONF_TIME_INTERVAL, default=60): vol.All(
            cv.positive_int, vol.Range(min=1)
        ),
        vol.Optional(CONF_TABLES, default=[]): vol.All(cv.ensure_list,
                                                       [TABLE_SCHEMA]),
    }
)

CONFIG_SCHEMA = vol.Schema(
    {vol.Optional(DOMAIN): CSVLOGGER_CONFIG_SCHEMA},
    extra=vol.ALLOW_EXTRA,
)


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the integration."""
    if DOMAIN in config:
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN, context={"source": SOURCE_IMPORT},
                data=config[DOMAIN]
            )
        )

    async def handle_reload(call: ServiceCall) -> None:
        await async_reload(hass)

    async def handle_flush(call: ServiceCall) -> None:
        await async_flush(hass)

    async_register_admin_service(hass, DOMAIN, "reload", handle_reload)
    async_register_admin_service(hass, DOMAIN, "flush", handle_flush)

    return True


async def async_reload(hass: HomeAssistant) -> None:
    """Reload CSV Logger configuration from configuration.yaml."""
    config = await async_integration_yaml_config(
        hass, DOMAIN, raise_on_failure=True
    )
    if config is None or DOMAIN not in config:
        raise HomeAssistantError(
            "CSV Logger configuration was not found in configuration.yaml"
        )

    await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": SOURCE_IMPORT},
        data=config[DOMAIN],
    )


async def async_flush(hass: HomeAssistant) -> None:
    """Flush the active CSV file handles."""
    await get_csv_file_service(hass).flush()


async def async_setup_entry(
    hass: HomeAssistant, config_entry: ConfigEntry
) -> bool:
    """Set up the CSV Logger component."""
    hass.data.setdefault(DOMAIN, {})

    _LOGGER.debug("Before create_csvlogger_gateway")

    gateway = create_csvlogger_gateway(config_entry, hass)

    _LOGGER.debug("After create_csvlogger_gateway")

    if not gateway:
        return False

    hass.data[DOMAIN][CSVLOGGER_GATEWAY] = gateway
    config_entry.async_on_unload(
        config_entry.add_update_listener(async_update_listener)
    )

    await gateway.async_added_to_hass()

    config_entry.async_on_unload(
        hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP,
            gateway.stop_processing,
        )
    )

    return True


async def async_update_listener(
    hass: HomeAssistant, config_entry: ConfigEntry
) -> None:
    """Reload the integration after its imported YAML configuration changes."""
    await hass.config_entries.async_reload(config_entry.entry_id)


async def async_unload_entry(
    hass: HomeAssistant, config_entry: ConfigEntry
) -> bool:
    """Unload the CSV Logger component."""
    gateway = hass.data.get(DOMAIN, {}).get(CSVLOGGER_GATEWAY)
    if gateway is not None:
        await gateway.async_shutdown()
        hass.data[DOMAIN].pop(CSVLOGGER_GATEWAY, None)

    return True
