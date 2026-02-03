#
#    Copyright (c) 2026 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Search List Extensions for weewx-climate.

Typical tags:

$climate.day.precip.sum.max       <-- Max precip for this day
$climate.day.precip.sum.maxtime   <-- Year of the max precip
$climate.day.outTemp.high.max     <-- Max high (high-high) temperature for this day
$climate.day.outTemp.high.maxtime <-- Year of the max high temperature
$climate.day.outTemp.high.avg     <-- Average high temperature for this day
$climate.day.outTemp.low.avg      <-- Average low temperature for this day
$climate.day.outTemp.low.max      <-- Max low (high-low) temperature for this day
          │    │      │   │
          │    │      │   └─── reduction
          │    │      └─────── stat
          │    └────────────── obs_type
          └─────────────────── period
"""

import datetime

import weewx.units
from weewx.cheetahgenerator import SearchList
from weewx.units import ValueTuple, ValueHelper

from user.climate.climate import default_station_id

weewx.units.obs_group_dict.setdefault('precip', 'group_rain')

class Climate:
    def __init__(self,
                 db_lookup,
                 report_time,
                 formatter=None,
                 converter=None):
        """Initialize an instance of Climate.

        Args:
            db_lookup (function|None): A function with call signature db_lookup(data_binding),
                which returns a database manager and where data_binding is an optional binding
                name. If not given, then a default binding will be used.
            report_time(float): The time for which the report should be run.
            formatter (weewx.units.Formatter|None): An instance of weewx.units.Formatter() holding
                the formatting information to be used. [Optional. If not given, the default
                Formatter will be used.]
            converter (weewx.units.Converter|None): An instance of weewx.units.Converter() holding
                the target unit information to be used. [Optional. If not given, the default
                Converter will be used.]
        """
        # Collect all data in one structure. It will make what follows easier.
        self.params = {
            'db_lookup': db_lookup,
            'report_time': report_time,
            'formatter': formatter or weewx.units.Formatter(),
            'converter': converter or weewx.units.Converter(),
            'station_id': default_station_id,
            'data_binding': 'climate_binding',
        }

        # Until we hit the metadata database, we don't know anything about the station.
        self.name = self.location = self.latitude = self.longitude = self.altitude = None
        # Get the metadata for the station.
        self._get_metadata()

    def _get_metadata(self):
        db_manager = self.params['db_lookup'](self.params['data_binding'])
        results = db_manager.getSql("SELECT station_name, station_location, "
                                    "latitude, longitude, altitude "
                                    "FROM station_metadata "
                                    "WHERE station_id=?", (self.params['station_id'],))
        if results:
            self.name, self.location, self.latitude, self.longitude, self.altitude = results

    def __call__(self, station_id=None, data_binding=None):
        """Set a new station ID or data binding."""
        if station_id is not None:
            self.params['station_id'] = station_id
        if data_binding is not None:
            self.params['data_binding'] = data_binding
        # Refresh metadata
        self._get_metadata()
        return self

    def station_id(self):
        return self.params['station_id']

    def __getattr__(self, period):
        return ClimatePeriod(period, **self.params)

    def day(self):
        """Record statistics for a particular day."""
        return ClimatePeriod('day', **self.params, )


class ClimatePeriod:
    def __init__(self, period, **params):
        self.params = params
        self.params['period'] = period

    def __getattr__(self, obs_type):
        return ClimateObsType(obs_type, **self.params)


class ClimateObsType:
    def __init__(self, obs_type, **params):
        self.params = params
        self.params['obs_type'] = obs_type

    def __getattr__(self, stat):
        return ClimateStat(stat, **self.params)


class ClimateStat:
    def __init__(self, stat, **params):
        """Incorporate a statistic. This is typically either 'high' or 'low'."""
        self.params = params
        self.params['stat'] = stat

    def __getattr__(self, reduction):
        return ClimateReduction(reduction, **self.params)


class ClimateReduction:
    def __init__(self, reduction, **params):
        """Incorporate a reduction function. This is typically something like 'min', 'max',
        'avg', 'mintime', etc."""
        self.params = params
        self.params['reduction'] = reduction

    def _do_query(self):
        # For the purposes of a database query, a reduction such as 'mintime' becomes 'min'.
        reduct = self.params['reduction'].replace('time', '')
        db_manager = self.params['db_lookup'](self.params['data_binding'])
        # Form the SQL statement to be used:
        stats_sql_stmt = ("SELECT value, usUnits, year FROM %s "
                          "WHERE station_id = ? "
                          "AND month = ? "
                          "AND day = ? "
                          "AND obsType = ? "
                          "AND stat = ? "
                          "AND reduction = ?;") % db_manager.table_name

        report_d = datetime.date.fromtimestamp(self.params['report_time'])

        # Hit the database:
        result = db_manager.getSql(stats_sql_stmt, (self.params['station_id'],
                                                    report_d.month,
                                                    report_d.day,
                                                    self.params['obs_type'],
                                                    self.params['stat'],
                                                    reduct))
        # Did we get a result?
        if result:
            # Yes. Create a ValueTuple from it:
            value, us_units, year = result
            if 'time' in self.params['reduction']:
                vt = ValueTuple(year, 'count', 'group_count')
            else:
                # Figure out which group the observation type belongs to:
                g = weewx.units.obs_group_dict.get(self.params['obs_type'])
                # Get the standard unit group for this unit system:
                std_group = weewx.units.std_groups[us_units]
                # Which unit the type is in:
                unit = std_group[g]
                # Now we have what we need to create a ValueTuple:
                vt = ValueTuple(value, unit, g)
        else:
            vt = ValueTuple(None, None, None)

        vh = ValueHelper(vt,
                         formatter=self.params['formatter'],
                         converter=self.params['converter'])
        return vh

    def __str__(self):
        """Need a string representation. Force the query, return as string."""
        vh = self._do_query()
        return str(vh)


class ClimateSLE(SearchList):  # 1
    """Class that implements the climate-related search list extensions."""

    def __init__(self, generator):  # 2
        SearchList.__init__(self, generator)

    def get_extension_list(self, timespan, db_lookup):
        climate = Climate(
            db_lookup,
            timespan.stop,
            formatter=self.generator.formatter,
            converter=self.generator.converter)

        return [{'climate': climate}]
