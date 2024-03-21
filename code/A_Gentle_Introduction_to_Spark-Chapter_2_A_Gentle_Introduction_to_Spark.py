import os
import sys
import pyspark as spark
#os.environ['PYSPARK_PYTHON'] = sys.executable
#os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#from pyspark.context import SparkContext
#from pyspark.sql.session import SparkSession
#sc = SparkContext('local')
#spark = SparkSession(sc)

myRange = spark.range(1000).toDF("number")


# COMMAND ----------

divisBy2 = myRange.where("number % 2 = 0")


# COMMAND ----------

flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("C:\\users\\amendenhall\\Desktop\\Spark-The-Definitive-Guide\\data\\flight-data\\csv\\2015-summary.csv")

# flightData2015.take(3) #Displays the first 3 rows
# flightData2015.sort("count").explain()  #Shows how the execution is planned to take place
# spark.conf.set("spark.sql.shuffle.partitions","5") #Set number of partitions data is processed over
# flightData2015.sort("count").take(2) #Sort by the count column and return the first 2
# flightData2015.createOrReplaceTempView("flight_data_2015") # Turns DataFrame into table/view
# COMMAND ----------
flightData2015.createOrReplaceTempView("flight_data_2015")
# COMMAND ----------
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")
dataFrameWay = flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .count()
sqlWay.explain()
dataFrameWay.explain()
# COMMAND ----------

from pyspark.sql.functions import max

flightData2015.select(max("count")).take(1)


# COMMAND ----------

maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()


# COMMAND ----------

from pyspark.sql.functions import desc

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .show()


# COMMAND ----------

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .explain()


# COMMAND ----------
