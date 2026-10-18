# Dataset provenance

`details-1950.csv` is the course-provided milestone file.

`details-1996.csv` was generated from the National Centers for Environmental
Information (NCEI) official Storm Events bulk details file:

<https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d1996_c20260323.csv.gz>

Only the 14 fields required by the assignment are retained, in assignment
order: event id, state, year, month name, event type, cz type, cz name,
injuries direct, injuries indirect, deaths direct, deaths indirect, damage
property, damage crops, and tornado F scale.

The resulting file has 48,534 records. Of the four event ids required in the
full-project demo, `5536849` and `5539287` are present; `2402920` and
`55770111` are absent and exercise the specified not-found response.
