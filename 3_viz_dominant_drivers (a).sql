-- Databricks notebook source
-- MAGIC %python
-- MAGIC html="""<h1 style="color:Black;text-align:center;font-family:Ariel"> Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
as
SELECT driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points,
      RANK() OVER (ORDER BY AVG(calculated_points) desc) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1)>=50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, 
      driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers where driver_rank<=10)
GROUP BY race_year, driver_name
ORDER BY race_year,avg_points DESC

-- COMMAND ----------

SELECT race_year, 
      driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers where driver_rank<=10)
GROUP BY race_year, driver_name
ORDER BY race_year,avg_points DESC

-- COMMAND ----------

SELECT race_year, 
      driver_name,
      count(1) as total_races,
      sum(calculated_points) as total_points,
      avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers where driver_rank<=10)
GROUP BY race_year, driver_name
ORDER BY race_year,avg_points DESC

-- COMMAND ----------


