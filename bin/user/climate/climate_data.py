#
#    Copyright (c) 2026 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Create and manage a climatological database for WeeWX."""
import logging

import weedb

log = logging.getLogger(__name__)

VERSION = '1.0'

CREATE_CLIMATE_DATA = ("CREATE TABLE IF NOT EXISTS %s "
                       "(month INTEGER NOT NULL, "
                       "day INTEGER NOT NULL, "
                       "usUnits INTEGER NOT NULL, "
                       "obsType TEXT NOT NULL, "
                       "stat TEXT NOT NULL, "
                       "reduction TEXT NOT NULL, "
                       "value REAL,"
                       "year INTEGER);")


def create_climate_database(database_dict, table_name):
    try:
        # This will raise exception weedb.DatabaseExistsError if the database already exists.
        # Otherwise, it will create it.
        weedb.create(database_dict)
        # Now create the internal tables.
        with weedb.connect(database_dict) as db_conn:
            with weedb.Transaction(db_conn) as cursor:
                cursor.execute(CREATE_CLIMATE_DATA % table_name)
                cursor.execute("CREATE INDEX %s_idx ON %s (month, day);"
                               % (table_name, table_name))
                cursor.execute("CREATE TABLE IF NOT EXISTS %s_metadata "
                               "(name CHAR(20) NOT NULL PRIMARY KEY, value TEXT);" % table_name)
                cursor.execute("INSERT INTO %s_metadata (name, value) "
                               "VALUES ('version', ?);" % table_name, (VERSION,))
        log.debug("Climate database table %s initialized.", table_name)
    except weedb.DatabaseExistsError:
        log.debug("Climate database table %s already exists.", table_name)
        pass
