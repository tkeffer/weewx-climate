# Climate downloader

## About

This extension downloads climatological data, stores them in a local database,
then makes them available as a search list extension.  While the architecture
can support multiple download sources, this first version supports only the
US-centric [ACIS database](https://www.rcc-acis.org/) database.

In the future, I hope to add support for other countries.

## Prerequisites

- Python 3.7+

## Installation

### Station ID

You will need to find a station ID of a nearby climatology station in order to
download data. The extension accepts many different kinds of IDs, but one of the
more accessible is the list of [Global Historical Climatology Network daily
(GHCNd)](https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt)
stations. Scan the list for a nearby station. Only USA stations will work.

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
USC00040983    01  01       1  precip  sum       avg  0.12 null
USC00040983    01  01       1  precip  sum       max  3.24 2009
...
```

## Tags

```
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
```

## Demonstration skin

A demonstration skin called "Climate" is included and will be installed when
you install the extension. It demonstrates how to use the tags.