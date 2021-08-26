# jdf2gtfs
## Convert Czech or Slovak bus timetable data from JDF format into GTFS format

Public transport timetable data are the base of analytics in public transport. In the Czech Republic there are data available in the JDF format. This data cannot be directly used for analysis and data-mining. We have created an application for automatic processing of public transport timetables in JDF format. Thanks to Python, PostgreSQL and PostGIS technologies, our application can convert data to spatial database and GTFS format, which is the world standard for timetables data. The application is an open source software and provide data for analysis and optimization of public transport – use of these data is in fields of identify places and times of delays, optimizing the capacity of vehicles and design network of routes.

```
usage: jdf2gtfs.py [-h] [--db_name DB_NAME] [--db_server DB_SERVER]
               [--db_user DB_USER] [--db_password DB_PASSWORD]
               [--store_db_tables] [--cisjr] [--zip] [--keep_history]
               [--stopids] [--stopnames] [--stoplocations STOPLOCATIONS]
               in_dir out_dir

...

positional arguments:
  in_dir                Path to input direcotry with JDF files or zip.
                        Eventually directory where files from CIS JR will be
                        unzipped and stored
  out_dir               Path to output direcotry with GTFS files

optional arguments:
  -h, --help            show this help message and exit
  --db_name DB_NAME     PostgreSQL database name
  --db_server DB_SERVER
                        PostgreSQL database server
  --db_user DB_USER     PostgreSQL database username
  --db_password DB_PASSWORD
                        PostgreSQL database password
  --store_db_tables     If database tables in PostgreSQL database will be
                        stored or deleted
  --cisjr               Download data from CISJR
  --zip                 Input JDF data are in zip files
  --keep_history        If processing more timetables from different times
  --stopids             Use stop IDs as a primary key
  --stopnames           Use stop names as a primary key
  --stoplocations STOPLOCATIONS
                        Path to csv file with stops locations. (ID,x,y) or
                        (name,x,y)
```
