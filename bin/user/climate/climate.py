#
#    Copyright (c) 2026 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Create and manage a climatological database for WeeWX.

Typical manager_dict looks like:
{'table_name': 'climate_data',
 'manager': 'user.climate.climate.StatsManager',
 'database_dict': {'database_name': 'climate.sdb', 'driver': 'weedb.sqlite',
                   'SQLITE_ROOT': '/Users/tkeffer/weewx-data/archive'}, 'schema': None}

"""
import datetime
import importlib
import logging
import threading
import time

import weedb
import weewx
import weewx.manager
import weewx.xtypes
from weeutil.weeutil import to_bool, to_int
from weewx.engine import StdService

import user.climate.clxtype

VERSION = '1.0'

log = logging.getLogger(__name__)

CREATE_CLIMATE_DATA = "CREATE TABLE %s " \
                      "(station_id TEXT NOT NULL, month INTEGER NOT NULL, day INTEGER NOT NULL, " \
                      "usUnits INTEGER NOT NULL, obsType TEXT NOT NULL, stat TEXT NOT NULL, " \
                      "reduction TEXT NOT NULL, value REAL, year INTEGER);"

CREATE_CLIMATE_INDEX = "CREATE INDEX %s_index ON climate_data (station_id, month, day);"

CREATE_STATION_METADATA = "CREATE TABLE station_metadata " \
                          "(station_id TEXT PRIMARY KEY, " \
                          "station_name TEXT, " \
                          "station_location TEXT, " \
                          "latitude REAL, longitude REAL, altitude REAL, " \
                          "last_download TEXT);"

default_binding_dict = {
    'database': 'climate_sqlite',
    'table_name': 'climate_data',
    'manager': 'user.climate.climate.StatsManager'
}

default_station_id = None


class StatsManager(weewx.manager.Manager):
    """Specialized manager for the climate database. The base class manager is designed to
    handle time-series type data, while we need to look things up by station_id and date."""

    @classmethod
    def open_with_create(cls, database_dict, table_name, schema=None):
        """Overrides base class method because we use a different kind of schema. """
        setup_climate_database(database_dict, table_name)
        connection = weedb.connect(database_dict)
        # Create an instance of the right class and return it:
        dbmanager = cls(connection, table_name=table_name, schema=None)
        return dbmanager

    def __init__(self, connection, table_name, schema=None):
        self.connection = connection
        self.table_name = table_name


class Climate(StdService):

    def __init__(self, engine, config_dict):
        # Initialize my base class:
        super().__init__(engine, config_dict)

        # Extract our configuration stanza out of the main configuration dictionary:
        try:
            climate_dict = config_dict['Climate']
        except KeyError:
            log.error("Missing [Climate] stanza in weewx.conf. Extension disabled.")
            return

        if not to_bool(climate_dict.get('enabled', True)):
            log.info("weewx-climate extension is disabled.")
            return

        # How long to wait before launching a new thread if one is already running:
        self.max_wait = to_int(climate_dict.get('max_wait', 600))

        self.stations = {}

        # Iterate through the stations
        for station_id in climate_dict.sections:
            log.debug(f"Processing station: {station_id}")
            # Get the downloader name for this station, then import it.
            try:
                downloader = importlib.import_module(climate_dict[station_id]['downloader'])
            except (ImportError, KeyError):
                log.error(f"Missing downloader for station {station_id}. Skipped.")
                continue

            # Stuff to remember:
            self.stations[station_id] = {
                'thread': None,
                'launch_time': None,
                'downloader': downloader,
            }

            with weewx.manager.open_manager_with_config(
                    config_dict,
                    data_binding='climate_binding',
                    initialize=True,
                    default_binding_dict=default_binding_dict) as db_manager:

                # Fetch initial data for this station
                self.fetch_data(db_manager, station_id, datetime.date.today())

                # Set the default station ID to the first station
                global default_station_id
                if not default_station_id:
                    default_station_id = station_id

        # Register the XType
        self.xt = user.climate.clxtype.ClimateXType()
        weewx.xtypes.xtypes.append(self.xt)

        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    def new_archive_record(self, event):
        """Called when a new archive record is generated. Check to see if it's time for a new
        download of ACIS data. If so, launch a thread to do it."""

        # Get the date from the current record:
        current_date = datetime.datetime.fromtimestamp(event.record['dateTime']).date()

        for station_id in self.stations:
            with weewx.manager.open_manager_with_config(
                    self.config_dict,
                    data_binding='climate_binding',
                    initialize=True,
                    default_binding_dict=default_binding_dict) as db_manager:
                # Update data if necessary.
                self.fetch_data(db_manager, station_id, current_date)

    def fetch_data(self, db_manager, station_id, current_date):
        """If the data is not current, launch a thread to download it."""

        # Determine the last download time.
        results = db_manager.getSql("SELECT last_download "
                                    "FROM station_metadata "
                                    "WHERE station_id = ?;", (station_id,))
        download_date = results[0] if results else None
        if download_date and download_date >= current_date.isoformat():
            log.debug("Climate data is current.")
            return

        log.debug("Climate data is not current. Updating...")

        # Do not launch the update thread if an old one is still alive.
        # To guard against a zombie thread (alive, but doing nothing) launch
        # anyway if enough time has passed.
        if self.stations[station_id]['thread'] and self.stations[station_id]['thread'].is_alive():
            thread_age = time.time() - self.stations[station_id]['launch_time']
            if thread_age < self.max_wait:
                log.info("Launch of download thread aborted: existing thread is still running")
                return
            else:
                log.warning("Previous download thread has been running %s seconds. "
                            "Launching a new thread anyway.", thread_age)

        try:
            self.stations[station_id]['thread'] = threading.Thread(
                target=self.stations[station_id]['downloader'].fetch_station_data,
                args=(self.config_dict, station_id, current_date))
            self.stations[station_id]['thread'].start()
            self.stations[station_id]['launch_time'] = time.time()
        except threading.ThreadError:
            log.error("Unable to launch update thread.")
            self.stations[station_id]['thread'] = None

    def shutDown(self):
        # Engine is shutting down. Remove the XType registration
        weewx.xtypes.xtypes.remove(self.xt)


def setup_climate_database(database_dict, table_name):
    try:
        # This will raise exception weedb.DatabaseExistsError if the database already exists.
        weedb.create(database_dict)
        log.debug("Climate database created.")
    except weedb.DatabaseExistsError:
        log.debug("Climate database already exists.")
        return
    # Create the tables and indexes:
    with weedb.connect(database_dict) as db_conn:
        with weedb.Transaction(db_conn) as cursor:
            cursor.execute(CREATE_CLIMATE_DATA % table_name)
            cursor.execute(CREATE_CLIMATE_INDEX % table_name)
            cursor.execute(CREATE_STATION_METADATA)
        log.debug("Climate database table initialized.")

if __name__ == "__main__":
    import weecfg
    from weewx.engine import DummyEngine

    config_path, config_dict = weecfg.read_config(None)

    climate = Climate(DummyEngine(config_dict), config_dict)
