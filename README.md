# PrimaryBid
Transform application Lifecycle Data and Design and ETL pipeline architecture for ingesting data from multiple sources to redshift
This project is composed of two parts: Part1 and Part2

# Part1
This part involves ingesting an application lifecycle raw data in .csv formats (“CC Application Lifecycle.csv”).
The data is transformed to return various Application stages as column names, and the time of stage completion, as values against each customer ID via python.

Files included in this section include:
- Solution Directory:
  - application_etl.py (Contains transformation class for application lifecycle raw data)
  - run_application_etl.py (Ingest and executes transformations for application lifecycle raw data)
- Test Directory:
  - test_application_etl.py (runs a series of test for objects in the transformation class)
  - Input Directory (Contains all the input test files)
  - Output Directory (Contains all the output test files)

Execution:
1. Execute run_application_etl.py to obtain output file for transformed application lifecycle data.

Modifications:
1. Extra transformation, bug fixes and other modification can be added in application_etl.py as an object. 
2. For new transformations (new functions), add a test for the function in test_application_etl.py and execute it with pytest -vv.
3. Call the object in run_application_etl.py after test passes to return desired output.

# Part2
This part presents an architectural design to ingest data from a MongoDB database - into a Redshift data platform. The solution accomodates the addition of more data sources in the near future. The DDL scripts which form part of the solution is resusable for ingesting and loading data into redshift.

Files included in this section establishes the creation of target tables for the data ingestion process:
- dwh.cfg (Infrastucture parameters and configuration)
- DDL_queries.py (DDL queries to drop, creat, copy/insert data into Redshift)
- table_setup_load.py (Class to manage the establish connection to database setup and teardown of tables in Redshift)
- execute_ddl_process.py (script to execute processes in table_setup_load class)
- test_execute_ddl_process.py (script to test the setup and teardown of resources.)
- requirement.txt (key libraries needed to execute .py scripts)
- makefile (file to automate process of installing and testing libraries and .py scripts respectively.)

Execution:
1. Execute execute_ddl_process.py to create and load data into target tables from S3.

Modifications:
1. Bucket file sources and other config paramters can be added in dwh.cfg
2. New DDl queries which includes ingesting data from multiple tables from aggregations/joins can be added in DDL_queries.py.
3. For other functions not captured in this section work, custom functions can be added in table_setup_load.py
4. Before executing scripts for production environments, test the modifications by executing test_execute_ddl_process.py

The architecture below highlights the processes involved in ingesting data from various data sources into redshift

- Architeture
<img width="1575" alt="Data Architecture" src="https://user-images.githubusercontent.com/24667667/150010563-da2914b6-4246-4fce-9645-58bd9134d87b.png">
