# Climate downloader

## About

This extension downloads climatological data, stores them in a local database,
then makes them available as XType extensions.  While the architecture can
support multiple download sources, this first version supports only the
US-centric [ACIS database](https://www.rcc-acis.org/) database.

In the future, I hope to add support for other countries.

## Prerequisites

- Python 3.7+

## Installation

### Station ID

You will need a station ID to download data from the ACIS database. The database
accepts many different kinds of IDs, but one of the more accessible is the list
of [Global Historical Climatology Network daily
(GHCNd)](https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt)
stations. Scan the list and find something useful. Only USA stations will work.

A more graphical interface is available at the [NOAA National Center for
Environmental Information's](https://www.ncei.noaa.gov/access/past-weather/)
page. Zoom into a location of interest, then click on "Search this area". A
list of stations will appear. Note the station ID next to the station that
interests you.

You can find more information about your chosen station in the NOAA
[Historical Observing Metadata Repository](https://www.ncei.noaa.gov/access/homr/).

## Database schema

```
station_id  month day usUnits obsType stat reduction value year
USC00040983    01  01       1 outTemp high       min    41 2018
USC00040983    01  01       1 outTemp high       avg    38 null
USC00040983    01  01       1 outTemp high       max    55 2004
USC00040983    01  01       1    rain  sum       avg  0.12 null
USC00040983    01  01       1    rain  sum       max  3.24 2009
...
```

## Tags

```
$day.outTemp.high_min   <-- Record low high for this particular day from the ACIS database
$day.outTemp.high_avg   <-- The average high for this particular day from the ACIS database
$month.outTemp.low_min  <-- Record low low for this particular month from the ACIS database

$day.rain.sum_avg   <-- The average rainfall for this particular day from the ACIS database
$day.rain.sum_max   <-- The maximum rainfall for this particular day from the ACIS database
```

## Alternate tags

$climate.day.outTemp.avg.high
$climate.day.outTemp.min.high
$climate.day.outTemp.max.high
$climate.day.outTemp.max.high.year
$climate.day.precip.max
$climate.day.precip.max.year