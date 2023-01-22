# databricks_formula1_race
- Data analysis of the formula1 motor racing sports dataset using PySpark & spark SQL in Azure Databricks. Azure data lake for storage. Executed pipelines & triggers in Azure Data Factory. Visualization in PowerBI.
- Worked with formula1 racing datasets in Azure Databricks
- Created a resource group
- Created a databricks workspace 
- Created all-purpose cluster 
- Created notebook files in formula1 folder in workspace
- Created a data lake storage Gen2
- Created a key vault
- Created three blob containers raw, processed and presentation in data lake
- Mounted data lake containers raw, processed and presentation in databricks
- worked full load for files circuits, races, constructors, drivers
- worked incremental load for files results, pitstops, laptimes, qualifying for dates 2021-03-21, 2021-03-28, 2021-04-18 
- Converted parquet files to delta files 
- Created Azure Data Factory
- Created three pipelines for ingest, transform and process. Process pipeline executed ingestion and transform pipeline.
- Run these pipeline in Azure Data Factory.
- Created trigger that executes three pipeline at a specified date and time 
- Connected Databricks and PowerBI 
- Integrated Databricks and github and pushed notebook files from Databricks to github 
---------------------------------------------------------------------------------------
DATABASES:
- 1]demo database   : 
pv_race_results, race_results_ext_py, race_results_ext_sql, race_results_python
- 2]f1_demo database :
drivers_convert_to_delta, drivers_merge, drivers_txn, results_external, results_managed, results_partitioned.
- 3]f1_presentation database:
calculated_race_results, constructor_standings, driver_standings, race_results
- 4]f1_processed database:
circuits, constructors, drivers, laptimes, pitstops, qualifying, races,results 
- 5]f1_raw database:
circuits, constructors, drivers, laptimes, pitstops, qualifying, races,results 
