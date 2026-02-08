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
[StdReport]

    [[ClimateReport]]
        # This is a report for demonstrating the tags in weewx-climate.
        skin = Climate
        enable = false
        HTML_ROOT = climate

[DataBindings]
    
    [[climate_binding]]
        # The climate database must match one of the sections in [Databases].
        database = climate_sqlite
        # The name of the table within the database.
        table_name = climate_data
        # Specialized databases manager used by the extension
        manager = user.climate.climate.StatsManager

[Databases]
    
    # The store for climatological data.
    [[climate_sqlite]]
        database_name = climate.sdb
        database_type = SQLite

##############################################################################

# The climate downloader, for downloading climatological data from ACIS and other sources.

[Climate]
    # Replace with the ACIS ID of a nearby station. See the README for more info.
    [[USC00040983]]
        enabled = true
        downloader = user.climate.acis

"""

climate_dict = configobj.ConfigObj(StringIO(CONFIG))


def loader():
    return ClimateInstaller()


class ClimateInstaller(ExtensionInstaller):
    def __init__(self):
        super(ClimateInstaller, self).__init__(
            version="1.3",
            name='weewx-climate',
            description='Download climatological data from ACIS',
            author="Thomas Keffer",
            author_email="tkeffer@gmail.com",
            data_services='user.climate.climate.Climate',
            config=climate_dict,
            files=[
                ('bin/user', [
                    'bin/user/climate/__init__.py',
                    'bin/user/climate/acis.py',
                    'bin/user/climate/climate.py',
                    'bin/user/climate/clsle.py'
                ]),
                ('skins/Climate', [
                    'skins/Climate/index.html.tmpl',
                    'skins/Climate/skin.conf'
                ]),
                ('skins/Seasons', [
                    'skins/Seasons/climate.inc',
                    'skins/Seasons/climate.html.tmpl',
                    'skins/Seasons/lang/lang-climate/cz.conf',
                    'skins/Seasons/lang/lang-climate/de.conf',
                    'skins/Seasons/lang/lang-climate/es.conf',
                    'skins/Seasons/lang/lang-climate/fi.conf',
                    'skins/Seasons/lang/lang-climate/fr.conf',
                    'skins/Seasons/lang/lang-climate/gr.conf',
                    'skins/Seasons/lang/lang-climate/it.conf',
                    'skins/Seasons/lang/lang-climate/nl.conf',
                    'skins/Seasons/lang/lang-climate/no.conf',
                ])
            ]
        )
