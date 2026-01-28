#
#    Copyright (c) 2026 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Installer for the WeeWX climatological data downloader"""

from io import StringIO

import configobj
from weecfg.extension import ExtensionInstaller

CONFIG = """
[DataBindings]
    
    [[acis_binding]]
        # The climate database must match one of the sections in [Databases].
        database = climate_sqlite
        # The name of the table within the database.
        table_name = acis_data
        # The following is not actually used
        manager = weewx.manager.Manager

[Databases]
    
    # The store for climatological data.
    [[climate_sqlite]]
        database_name = climate.sdb
        database_type = SQLite

##############################################################################

# The climate downloader, for downloading climatological data from ACIS and other sources.

[Climate]
    # Replace with the ID from the ACIS database of a nearby station:
    [[USC00040983]]
        enabled = true
        binding = acis_binding
        downloader = user.climate.acis

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
