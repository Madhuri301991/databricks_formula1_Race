-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create circuits table 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT, 
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE, 
lng DOUBLE,
alt INT,
url STRING
)
using csv 
options (path "/mnt/formula1projectdeltalake/raw/circuits.csv",header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT, 
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING, 
url STRING)
using csv 
options (path "/mnt/formula1projectdeltalake/raw/races.csv",header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Constructors table 
-- MAGIC 1. Single-line JSON
-- MAGIC 2. Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
using json 
options (path "/mnt/formula1projectdeltalake/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Drivers table 
-- MAGIC 1. Single-line JSON
-- MAGIC 2. Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename:STRING,surname:STRING>,
dob DATE,
nationality STRING,
url STRING
)
using json 
options (path "/mnt/formula1projectdeltalake/raw/drivers.json")


-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create results table
-- MAGIC 1. Single-line JSON
-- MAGIC 2. Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT, 
position INT, 
positionText STRING,
positionOrder INT, 
points INT, 
laps INT,
time STRING,
milliseconds INT, 
fastestLap INT, 
rank INT, 
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
options (path "/mnt/formula1projectdeltalake/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Create Pit stops tables
-- MAGIC 1. Multi-line JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT, 
time STRING)
USING json
OPTIONS (path "/mnt/formula1projectdeltalake/raw/pit_stops.json",multiLine True)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create table for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Lap times table 
-- MAGIC 1. CSV files
-- MAGIC 2. Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT, 
driverId INT,
lap INT, 
position INT, 
time STRING,
milliseconds INT
)
using csv
OPTIONS (path "/mnt/formula1projectdeltalake/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Qualifying table 
-- MAGIC 1. JSON file
-- MAGIC 2. MultiLine Json 
-- MAGIC 3. Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT, 
driverId INT,
number INT, 
position INT, 
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
using json
OPTIONS (path "/mnt/formula1projectdeltalake/raw/qualifying",multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;
