#
#    Copyright (c) 2026 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""WeeWX XTypes extensions that downloads climatological data from NOAA's Applied Climatology
Instrument System (ACIS) servers into a local SQL database.

See https://www.rcc-acis.org/docs_webservices.html for a description of the ACIS API.
"""
import json
import logging
import time
import urllib.request

import weedb
import weewx.manager
from weeutil.weeutil import to_int, to_float
from user.climate.climate import default_binding_dict, setup_climate_database

log = logging.getLogger(__name__)

ACIS_URL = "https://data.rcc-acis.org/StnData"
ACIS_METADATA_URL = "https://data.rcc-acis.org/StnMeta"


def acis_element(stat, reduce_method):
    """Return a dictionary representing an ACIS query element.
    
    Args:
        stat (str): The statistical type to be summarized. Typically, mint, maxt,
            or pcpn (precipitation).
        reduce_method (str): The reduction method to apply to the data.
            Either 'min', 'max', or 'mean'.

    Example:
    acis_element('mint', 'max') would return an ACIS query element that
        would ask for the max daily low temperature (i.e., the "high low" for the day)
    """
    return {
        'name': stat,
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
            acis_element('mint', 'min'),
            acis_element('pcpn', 'max'),
        ]
    }


def fetch_station_data(config_dict, station_id, current_date):
    """Worker thread to fetch ACIS station metadata and historical data, then store
     in the database."""

    # Update both the station_metadata and the station_data tables as one transaction.
    with weewx.manager.open_manager_with_config(
            config_dict,
            data_binding='climate_binding',
            initialize=True,
            default_binding_dict=default_binding_dict) as db_manager:
        with weedb.Transaction(db_manager.connection) as cursor:
            # First get the metadata for the station...
            get_metadata(cursor, station_id)
            # ...then the data itself:
            get_data(cursor, station_id, db_manager.table_name, current_date)


def get_metadata(cursor, station_id):
    """Insert metadata about the given station into the database. """

    # Construct JSON payload:
    payload = json.loads('{"sids":"%s"}' % station_id)
    results = do_fetch(payload, ACIS_METADATA_URL)

    if not results:
        return

    data = results['meta'][0]
    meta = {'station_id': station_id,
            'station_name': data['name'],
            'station_location': data['state'],
            'latitude': data['ll'][1],
            'longitude': data['ll'][0],
            'altitude': data['elev'],
            'last_download': None,
            }
    # Insert it into the station_metadata table
    cursor.execute("INSERT OR REPLACE INTO station_metadata VALUES (?, ?, ?, ?, ?, ?, ?);",
                   (meta['station_id'],
                    meta['station_name'],
                    meta['station_location'],
                    meta['latitude'], meta['longitude'],
                    meta['altitude'],
                    meta['last_download']))
    log.debug(f"Metadata for station {station_id} inserted into database.")


def get_data(cursor, station_id, table_name, current_date):
    """Get the historical data for the given station and store it in the database."""

    # Construct JSON query payload:
    payload = acis_struct(station_id)
    # Do the fetch, then look for results:
    results = do_fetch(payload, ACIS_URL)
    if not results:
        return

    # 1. Clear entries for this station ID
    cursor.execute(f"DELETE FROM %s WHERE station_id = ?;" % table_name, (station_id,))

    # 2. Now batch insert the new data.
    for rec in gen_acis_records(results, station_id):
        # The record has 9 elements. Match the 9 columns in CREATE_CLIMATE_DATA
        cursor.execute(f"INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);" % table_name, rec)

    # 3. Update the download date in the station metadata table.
    cursor.execute(f"UPDATE station_metadata SET last_download = ? WHERE station_id = ?;",
                   (current_date.isoformat(), station_id))
    log.debug(f"Climate data for station {station_id} inserted into database.")


def gen_acis_records(results, station_id):
    """
    Parse the returned JSON structure from the ACIS server. Break it down to individual statistics,
    reduction method, and record values. Yield them one-by-one as 9-way tuples.

    Sample output:

    (USC00354003, 1, 1, 1, 'outTemp', 'high', 'avg', 38.5, None)
    (USC00354003, 1, 2, 1, 'outTemp', 'high', 'avg', 38.7, None)
    (USC00354003, 1, 3, 1, 'outTemp', 'high', 'avg', 38.5, None)
    (USC00354003, 1, 4, 1, 'outTemp', 'high', 'avg', 38.7, None)
    ...
    (USC00354003, 1, 1, 1, 'outTemp', 'high', 'max', 58.0, 1997)
    (USC00354003, 1, 2, 1, 'outTemp', 'high', 'max', 56.0, 1913)
    (USC00354003, 1, 3, 1, 'outTemp', 'high', 'max', 57.0, 1996)
    (USC00354003, 1, 4, 1, 'outTemp', 'high', 'max', 58.0, 1902)
    ...

     In order, the tuple elements are
     1. Station ID
     2. Month (1-12)
     3. Day of month (1-31)
     4. Unit system (1==US)
     5. Observation type (e.g., 'outTemp')
     6. Statistic. Typically, 'high' or 'low'
     7. Reduction method. Typically, 'avg', 'max', or 'min'
     8. Value of the record, in the unit given by usUnits
     9. Year of the record value, or None in the case of a reduction method of 'avg'.
    """
    # Start with the station metadata:
    station_name = results['meta']['name']
    station_state = results['meta']['state']

    # Now process the actual summary data. The order the results will appear in is set by
    # function acis_struct() above.
    ordering = [('high', 'avg', 'outTemp'), ('high', 'max', 'outTemp'), ('high', 'min', 'outTemp'),
                ('low', 'avg', 'outTemp'), ('low', 'max', 'outTemp'), ('low', 'min', 'outTemp'),
                ('sum', 'max', 'precip')]

    # Scan through the 6 different statistics and reduction methods returned from the server
    for stat_tuple, element_list in zip(ordering, results['smry']):
        stat, reduction, obs_type = stat_tuple
        for day_tuple in element_list:
            # The value is in the first element. It's a string, so convert it to a float. Watch
            # out for missing values (marked with 'M') and "trace" (marked with 'T'):
            val = day_tuple[0].strip().upper()
            if val == 'M':
                value = None
            elif val == 'T':
                value = 0.0
            else:
                value = to_float(day_tuple[0])
            # The second element holds the date in the form 'YYYY-MM-DD'.
            year, month, day = [to_int(e) for e in day_tuple[1].split('-')]
            # There is no record year for reduction methods of 'avg':
            if reduction == 'avg':
                year = None
            # In this version, everything is in US units.
            usUnits = 1
            yield (station_id, month, day, usUnits, obs_type,
                   stat, reduction, value, year)


def do_fetch(payload, url):
    """Generic fetch"""
    try:
        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        start = time.time()
        with urllib.request.urlopen(request) as response:
            results = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        log.error(f"Error fetching JSON data from URL {url}: {e}")
        return None
    else:
        stop = time.time()
        log.debug(f"Fetched JSON data from {url} in {stop - start:.2f} seconds")
        return results
