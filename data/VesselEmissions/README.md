

# Climate Hackathon 2022

The vessel emissions data provided by HUB Ocean and the Ocean Data Portal Consists of:

- Gridded vessel emissions per month and vessel type. Available as .zarr files in folder zarr/vessel_emissions_and_traffic/ in blob storage. See Tutorial 1 for how to use.
- Voyage emissions tables, consisting of from/to ports and total co2 emission per vessel. Available as .parquet files in folder parquet/voyage_tables/ in blob storage. See Tutorial 2.
- Hourly emissions data per vessel. Available in PostgreSQL database. See Tutorial 3.


To access the datasets, connection strings need to be set for these two environmental variables below. Connection strings are distributed through the organizer.
```
HACKATHON_DB_CONNECTION
```
```
HACKATHON_CONNECTION_STR
```
This is not needed if running the notebooks in the Ocean Data Connector (The jupyter lab environment in the Ocean Data Platform).


Install dependecies with
```pip install -r requirements.txt```



