#
#    Copyright (c) 2026 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""WeeWX XTypes extensions that downloads climatological data from the Applied Climatology
Instrument System (ACIS) servers into a local SQL database.

See https://www.rcc-acis.org/docs_webservices.html for a description of the ACIS API.

Typical manager_dict looksl like:
{'table_name': 'acis_data',
 'manager': 'weewx.manager.Manager',
 'database_dict': {'database_name': 'climate.sdb', 'driver': 'weedb.sqlite',
                   'SQLITE_ROOT': '/Users/tkeffer/weewx-data/archive'}, 'schema': None}

"""
import datetime
import json
import logging
import threading
import time
import urllib.request

import weedb
import weewx
import weewx.manager
from weeutil.weeutil import to_bool, to_int, to_float
from weewx.engine import StdService

from user.climate.climate_data import create_climate_database

log = logging.getLogger(__name__)

VERSION = "0.1"

ACIS_URL = "https://data.rcc-acis.org/StnData"

default_binding_dict = {
    'database': 'climate_sqlite',
    'table_name': 'acis_data',
    'manager': 'weewx.manager.Manager'  # Not actually used
}


def acis_element(mint_maxt, reduce_method):
    """Return a dictionary representing an ACIS query element.
    
    Args:
        mint_maxt (str): Either 'mint' (minimum temperature) or 'maxt' (maximum temperature).
        reduce_method (str): The reduction method to apply to the data.
            Either 'min', 'max', or 'mean'.

    Example:
    acis_element('mint', 'max') would return an ACIS query element that
        would ask for the max daily low temperature (i.e., the "high low" for the day)
    """
    return {
        'name': mint_maxt,
        'interval': 'dly',
        'duration': 1,
        'smry': {'reduce': reduce_method, 'add': 'date'},
        'smry_only': 1,
        'groupby': 'year'
    }


def acis_struct(station_id):
    """Return a dictionary representing an entire ACIS query structure."""
    return {
        'sid': station_id,
        'sdate': 'por',
        'edate': 'por',
        'meta': ['name', 'state'],
        'elems': [
            acis_element('maxt', 'mean'),
            acis_element('maxt', 'max'),
            acis_element('maxt', 'min'),
            acis_element('mint', 'mean'),
            acis_element('mint', 'max'),
            acis_element('mint', 'min')
        ]
    }


class ACIS(StdService):

    def __init__(self, engine, config_dict):
        # Initialize my base class:
        super().__init__(engine, config_dict)

        self.thread = None
        self.launch_time = None

        # Extract our configuration stanza out of the main configuration dictionary:
        try:
            acis_dict = config_dict['ACIS']
        except KeyError:
            log.error("Missing ACIS stanza in weewx.conf. Extension disabled.")
            return

        if not to_bool(acis_dict.get('enabled', True)):
            log.info("weewx-acis extension is disabled.")
            return

        self.station_id = acis_dict.get('station_id')
        if not self.station_id:
            log.error("Missing station_id in ACIS section of weewx.conf. Extension disabled.")
            return

        # How long to wait before launching a new thread if one is already running:
        self.max_wait = to_int(acis_dict.get('max_wait', 600))

        self.manager_dict \
            = weewx.manager.get_manager_dict_from_config(config_dict,
                                                         data_binding='acis_binding',
                                                         default_binding_dict=default_binding_dict)
        create_climate_database(self.manager_dict['database_dict'],
                                self.manager_dict['table_name'])

        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    def new_archive_record(self, event):
        """Called when a new archive record is generated. Check to see if it's time for a new
        download of ACIS data. If so, launch a thread to do it."""

        # Get the date from the current record:
        current_date = datetime.datetime.fromtimestamp(event.record['dateTime']).date()

        table_name = self.manager_dict['table_name']

        # Get the last download date from the database metadata:
        with weedb.connect(self.manager_dict['database_dict']) as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute("SELECT value "
                               "FROM %s_metadata "
                               "WHERE name = 'download_date';" % table_name)
                results = cursor.fetchone()
                download_date = results[0] if results else None

        if download_date and download_date >= current_date.isoformat():
            log.debug("Climate data is current.")
        else:
            log.debug("Climate data is not current. Updating...")
            # Do not launch the update thread if an old one is still alive.
            # To guard against a zombie thread (alive, but doing nothing) launch
            # anyway if enough time has passed.
            if self.thread and self.thread.is_alive():
                thread_age = time.time() - self.launch_time
                if time.time() - thread_age < self.max_wait:
                    log.info("Launch of ACIS download thread aborted: "
                             "existing thread is still running")
                    return
                else:
                    log.warning("Previous ACIS download thread has been running"
                                " %s seconds.  Launching new thread anyway.", thread_age)

            try:
                self.thread = threading.Thread(target=fetch_data,
                                               args=(self.manager_dict['database_dict'],
                                                     table_name,
                                                     self.station_id,
                                                     current_date))
                self.thread.start()
                self.launch_time = time.time()
            except threading.ThreadError:
                log.error("Unable to launch ACIS update thread.")
                self.thread = None


def fetch_data(database_dict, table_name, station_id, current_date):
    """Worker thread to fetch ACIS data and store it in the database."""

    # Construct JSON payload:
    payload = acis_struct(station_id)

    try:
        request = urllib.request.Request(
            ACIS_URL,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        start = time.time()
        with urllib.request.urlopen(request) as response:
            results = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        log.error(f"Error fetching ACIS data: {e}")
        return
    else:
        stop = time.time()
        log.debug(f"Fetched ACIS data for station {station_id} in {stop - start:.2f} seconds")

    with weedb.connect(database_dict) as db_conn:
        with weedb.Transaction(db_conn) as cursor:
            # 1. Clear the table. This very fast in SQLite and does not require rebuilding indexes.
            cursor.execute(f"DELETE FROM {table_name};")

            # 2. Now batch insert the new data.
            for rec in gen_acis_records(results):
                # rec[2:] matches the columns in CREATE_CLIMATE_DATA
                cursor.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                               rec[2:])
            # 3. Update the download date in the metadata table.
            cursor.execute(f"INSERT OR REPLACE INTO {table_name}_metadata (name, value) "
                           "VALUES ('download_date', ?);", (current_date.isoformat(),))


def gen_acis_records(results):
    """
    Parse the returned JSON structure from the ACIS server. Break it down to individual statistics,
    reduction method, and record values. Yield them one-by-one as 9-way tuples.

    Example:

    ('HOOD RIVER EXPERIMENT STN', 'OR', 1, 1, 1, 'outTemp', 'high', 'avg', 38.5, None)
    ('HOOD RIVER EXPERIMENT STN', 'OR', 1, 2, 1, 'outTemp', 'high', 'avg', 38.7, None)
    ('HOOD RIVER EXPERIMENT STN', 'OR', 1, 3, 1, 'outTemp', 'high', 'avg', 38.5, None)
    ('HOOD RIVER EXPERIMENT STN', 'OR', 1, 4, 1, 'outTemp', 'high', 'avg', 38.7, None)
    ...
    ('HOOD RIVER EXPERIMENT STN', 'OR', 1, 1, 1, 'outTemp', 'high', 'max', 58.0, 1997)
    ('HOOD RIVER EXPERIMENT STN', 'OR', 1, 2, 1, 'outTemp', 'high', 'max', 56.0, 1913)
    ('HOOD RIVER EXPERIMENT STN', 'OR', 1, 3, 1, 'outTemp', 'high', 'max', 57.0, 1996)
    ('HOOD RIVER EXPERIMENT STN', 'OR', 1, 4, 1, 'outTemp', 'high', 'max', 58.0, 1902)
    ...

     In order, the tuple elements are
     1. Station name
     2. Station state
     3. Month (1-12)
     4. Day of month (1-31)
     5. Unit system (1==US)
     6. Observation type (e.g., 'outTemp')
     7. Statistics. Typically, 'high' or 'low'
     8. Reduction method. Typically, 'avg', 'max', or 'min'
     9. Record value in the unit given by usUnits
    10. Year of the record value, or None in the case of a reduction method of 'avg'.
    """
    # Start with the station metadata:
    station_name = results['meta']['name']
    station_state = results['meta']['state']

    # Now process the actual summary data. The order the results will appear in is set by
    # function acis_struct() above.
    ordering = [('high', 'avg'), ('high', 'max'), ('high', 'min'),
                ('low', 'avg'), ('low', 'max'), ('low', 'min')]

    # Scan through the 6 different statistics and reduction methods returned from the server
    for stat_tuple, element_list in zip(ordering, results['smry']):
        stat, reduction = stat_tuple
        for day_tuple in element_list:
            # The value is in the first element. It's a string, so convert it to a float. Watch
            # out for missing values (marked with 'M'):
            if day_tuple[0].strip().upper() == 'M':
                value = None
            else:
                value = to_float(day_tuple[0])
            # The second element holds the date in the form 'YYYY-MM-DD'.
            year, month, day_tuple = [to_int(e) for e in day_tuple[1].split('-')]
            # There is no record year for reduction methods of 'avg':
            if reduction == 'avg':
                year = None
            # In this version, everything is in US units.
            usUnits = 1
            obs_type = 'outTemp'
            yield (station_name, station_state, month, day_tuple, usUnits, obs_type,
                   stat, reduction, value, year)


if __name__ == "__main__":
    # this_dir = os.path.dirname(__file__)
    # with open(os.path.join(this_dir, '../../..', 'results.json'), mode='r') as fd:
    #     results = json.load(fd)
    #     for d in gen_acis_records(results):
    #         print(d)

    database_dict = {'database_name': 'climate.sdb',
                     'driver': 'weedb.sqlite',
                     'SQLITE_ROOT': '/Users/tkeffer/weewx-data/archive'}

    create_climate_database(database_dict, 'acis_data')
    fetch_data(database_dict, 'acis_data', 'USC00354003', datetime.date.today())
