"""The CSVLogger gateway."""
import logging
import asyncio
import csv
import os
import time

from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

from homeassistant.core import callback
from homeassistant.helpers import template

from .const import (
    DOMAIN, 
    CSVLOGGER_GATEWAY,
    CONF_TIME_INTERVAL, 
    CONF_FILE_PATH, 
    CONF_FILE_PATTERN, 
    CONF_COLUMNS
)

from .exceptions import CSVLoggerGatewayException


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

class Gateway():
    """CSVLogger Gateway."""
    _config_entry = None
    _data_logging_task = None
    _csv_file_service = None

    def __init__(self, config_entry, hass):
        """Initialize the CSVLogger gateway."""
        self._hass = hass
        self._config_entry = config_entry
        _LOGGER.debug('Initialized, config = [%s]', config_entry)

    async def async_added_to_hass(self):
        """Handle when an entity is about to be added to Home Assistant."""
        time_interval = self._config_entry.data[CONF_TIME_INTERVAL]
        file_path = self._config_entry.data[CONF_FILE_PATH]
        file_pattern = self._config_entry.data[CONF_FILE_PATTERN]
        columns = self._config_entry.data[CONF_COLUMNS]

        self._csv_file_service = CSVLoggerService(file_path, file_pattern, columns, self._hass)

        _LOGGER.debug('Starting csv logging task')

        self._data_logging_task = self._hass.loop.create_task(
            self.data_logging_loop(
                time_interval,
                self._csv_file_service
            )
        )

    async def data_logging_loop(
        self,
        time_interval,
        csv_file_service,
        **kwargs,
    ):
        logged_error = False
        file_handler = None
        timer_time = 0
        try:
            while True:
                await asyncio.sleep(1)
                if (time.time() - timer_time > time_interval):
                    await csv_file_service.execute()
                    timer_time = time.time()

        except asyncio.CancelledError:
            _LOGGER.info('cancelled!!')
            await csv_file_service.flush()
            raise

 
    @callback
    def stop_processing(self, event):
        """Close resources."""
        _LOGGER.warn("Stopping processing")
        if self._data_logging_task:
            self._data_logging_task.cancel()

@dataclass
class CSVColumn:
    name: str
    template: template.Template

class CSVLoggerService():
    """CSV Logger Service"""
    _hass = None
    _file_path = None
    _file_pattern = None
    _columns = []
    _current_file_name = None
    _file_handle = None

    def __init__(self, file_path, file_pattern, columns, hass):
        self._hass = hass
        self._file_path = file_path
        self._file_pattern = file_pattern
        self._columns = list(map(lambda col: CSVColumn(name=col['name'], 
                                                       template=template.Template(col['template'], hass)), 
                                 columns))
    async def prepare_file(self):
        """Checks whether we need to open a handle"""
        file_name = datetime.now().strftime(self._file_pattern)
        actual_file_name = os.path.join(self._file_path, file_name)
        if (actual_file_name != self._current_file_name):
            await self.flush()
            is_new = not Path(actual_file_name).exists()
            self._current_file_name = actual_file_name
            self._file_handle = open(actual_file_name, 'a')
            _LOGGER.debug('Opened file %s' % actual_file_name)
            return is_new
        return False

    async def render_data(self, is_new_file):
        fieldnames = list(map(lambda col: col.name, self._columns))
        writer = csv.DictWriter(self._file_handle, fieldnames=fieldnames)
        if (is_new_file):
            writer.writeheader()
        row = dict()
        for col in self._columns:
            row[col.name] = col.template.async_render()
        writer.writerow(row)

    async def execute(self):
        _LOGGER.debug('Ping')
        is_new = await self.prepare_file()
        await self.render_data(is_new)

    async def flush(self):
        _LOGGER.info('flushing...')
        if (self._file_handle is not None):
            self._file_handle.close()
            self._file_handle = None
            _LOGGER.info('closed file. %s' % self._current_file_name)
            self._current_file_name = None

def create_csvlogger_gateway(config_entry, hass):
    """Create the gateway."""
    gateway = Gateway(config_entry, hass)
    return gateway

def get_csv_file_service(hass):
    """Get the SMS notification service."""

    if CSVLOGGER_GATEWAY not in hass.data[DOMAIN]:
        _LOGGER.error("CSV Logger gateway not found, cannot initialize service")
        raise CSVLoggerGatewayException("CSV Logger gateway not found")

    gateway = hass.data[DOMAIN][CSVLOGGER_GATEWAY]

    return gateway._csv_file_service