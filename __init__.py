"""The CSV Logger component."""

import logging

import voluptuous as vol

from homeassistant.core import callback

from homeassistant.config_entries import SOURCE_IMPORT

import homeassistant.helpers.config_validation as cv
from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import EVENT_HOMEASSISTANT_STOP

from .const import (
    DOMAIN, 
    CSVLOGGER_GATEWAY, 
    CONF_TIME_INTERVAL, 
    CONF_FILE_PATH, 
    CONF_FILE_PATTERN, 
    CONF_COLUMNS
)

from .gateway import create_csvlogger_gateway


COLUMN_SCHEMA = vol.Schema(
    {
        vol.Required('name'): cv.string,
        vol.Required('template'): cv.template,
    }
)

PLATFORM_SCHEMA = [].extend(
    {
        vol.Optional(CONF_TIME_INTERVAL, default=60): cv.positive_int,
        vol.Required(CONF_FILE_PATH): cv.string,
        vol.Required(CONF_FILE_PATTERN): cv.string,
        vol.Optional(CONF_COLUMNS, default=[]): vol.All(cv.ensure_list, [COLUMN_SCHEMA]),
    }
)


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


async def async_setup(hass, config):
    """Set up the integration."""
    if DOMAIN in config:
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN, context={"source": SOURCE_IMPORT},
                data=config[DOMAIN]
            )
        )

    return True


async def async_setup_entry(hass, config_entry):
    """Set up the CSV Logget component."""

    hass.data.setdefault(DOMAIN, {})

    _LOGGER.debug("Before create_csvlogger_gateway")

    gateway = create_csvlogger_gateway(config_entry, hass)

    _LOGGER.debug("After create_csvlogger_gateway")

    if not gateway:
        return False

    hass.data[DOMAIN][CSVLOGGER_GATEWAY] = gateway

    await gateway.async_added_to_hass()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP,
                               gateway.stop_processing)

    return True