#
#    Copyright (c) 2026 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Installer for the WeeWX ACIS downloader"""

from io import StringIO

import configobj
from weecfg.extension import ExtensionInstaller

CONFIG = """
[DataBindings]
    
    [[acis_binding]]
        # The database must match one of the sections in [Databases].
        database = climate_sqlite
        # The name of the table within the database.
        table_name = acis_data
        # Not actually used by the ACIS downloader:
        manager = weewx.manager.Manager

[Databases]
    
    # The store for climatological data.
    [[climate_sqlite]]
        database_name = climate.sdb
        database_type = SQLite

##############################################################################

# The ACIS downloader, for downloading climatological data from ACIS.

[ACIS]

    station_id = USC00354003
    enabled = true
    binding = acis_binding

"""

acis_dict = configobj.ConfigObj(StringIO(CONFIG))


def loader():
    return ACISInstaller()


class ACISInstaller(ExtensionInstaller):
    def __init__(self):
        super(ACISInstaller, self).__init__(
            version="0.1",
            name='ACIS downloader',
            description='WeeWX extension to download climatological data from ACIS',
            author="Thomas Keffer",
            author_email="tkeffer@gmail.com",
            config=acis_dict,
            files=[('bin/user', ['bin/user/acis/__init__.py',
                                 'bin/user/climate_data.py'])]
        )
