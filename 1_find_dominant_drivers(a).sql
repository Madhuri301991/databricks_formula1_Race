-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results;


-- COMMAND ----------

SELECT driver_name,sum(calculated_points) as total_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
ORDER BY total_points DESC;

-- COMMAND ----------

SELECT driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
ORDER BY total_points DESC

-- COMMAND ----------

SELECT driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1)>=50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN  2011 AND 2020
GROUP BY driver_name
HAVING count(1)>=50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN  2001 AND 2010
GROUP BY driver_name
HAVING count(1)>=50
ORDER BY avg_points DESC
