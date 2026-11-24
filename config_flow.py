import logging
import voluptuous as vol

from homeassistant import config_entries, data_entry_flow
from .const import DOMAIN, CONF_TIME_INTERVAL

DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_TIME_INTERVAL): str
    }
)

_LOGGER = logging.getLogger(__name__)


class CSVLoggerConfigFlowHandler(config_entries.ConfigFlow, domain=DOMAIN):
    """CSV Logger config flow."""

    async def async_step_import(self, user_input=None):
        """Import a config entry."""
        entries = self._async_current_entries(include_ignore=True)
        if entries:
            entry = entries[0]
            if user_input is not None and entry.data != user_input:
                self.hass.config_entries.async_update_entry(
                    entry, data=user_input
                )
            raise data_entry_flow.AbortFlow("already_configured")

        return self.async_create_entry(
            title="CSV Logger Config",
            data=user_input,
        )
