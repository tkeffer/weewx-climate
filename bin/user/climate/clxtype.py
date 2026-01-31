#
#    Copyright (c) 2026 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Climate XType to support climate database aggregations"""
import datetime

import weewx.xtypes
from weewx.units import ValueTuple

class ClimateXType(weewx.xtypes.XType):
    # The set of all valid aggregation types:
    climate_aggs = {
        'high_avg', 'low_avg',
        'high_high', 'high_high_year',
        'low_high', 'low_high_year',
        'low_low', 'low_low_year',
        'high_low', 'high_low_year',
    }
    stats_sql_stmt = (
        "SELECT value, usUnits, year FROM %s "
        "WHERE station_id = ? "
        "AND month = ? AND day = ? "
        "AND stat = ? AND reduction = ?;")

    # year_sql_stmt = (
    #     "SELECT year FROM %s "
    #     "WHERE station_id = ? "
    #     "AND month = ? AND day = ? "
    #     "AND stats = ? AND reduction = ?;")

    def get_aggregate(self, obs_type, timespan, aggregate_type, db_manager, **option_dict):

        # Do we know how to calculate this kind of aggregation?
        if aggregate_type not in ClimateXType.climate_aggs:
            raise weewx.UnknownAggregation(aggregate_type)
        # Are we aware of this observation type?
        if obs_type != 'outTemp':
            raise weewx.UnknownObservationType(obs_type)

        from user.climate.climate import default_station_id
        station_id = option_dict.get('station_id', default_station_id)

        # We determine which day to use by the start of the timespan:
        day = datetime.date.fromtimestamp(timespan.start)

        # Figure out which stats and reduction to use:
        stats, reduction = aggregate_type.split('_', maxsplit=1)
        if reduction == 'high':
            reduction = 'max'
        elif reduction == 'low':
            reduction = 'min'

        val = db_manager.getSql(ClimateXType.stats_sql_stmt % db_manager.table_name,
                                (station_id, day.month, day.day, stats, reduction))
        if val:
            value, us_units, year = val
        else:
            value = us_units = year = None

        if 'year' in aggregate_type:
            return ValueTuple(year, None, None)

        std_group = weewx.units.std_groups[us_units]
        unit = std_group['group_temperature']
        return ValueTuple(value, unit, 'group_temperature')