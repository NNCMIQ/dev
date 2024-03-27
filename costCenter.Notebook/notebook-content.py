# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f1b082c4-7874-4e1e-a299-4b0e85b794cd",
# META       "default_lakehouse_name": "CMIQ_Playhouse",
# META       "default_lakehouse_workspace_id": "052c4811-3061-4b7d-86f7-c86b7345b87c",
# META       "known_lakehouses": [
# META         {
# META           "id": "f1b082c4-7874-4e1e-a299-4b0e85b794cd"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************


# MARKDOWN ********************


# CELL ********************


# MARKDOWN ********************

# Initialize

# MARKDOWN ********************


# CELL ********************

from pyspark.sql.functions import avg
from pyspark.sql.functions import sum
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import regexp_replace,when, col, ltrim as f
from pyspark.sql import SparkSession

# CELL ********************

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('costCenter').getOrCreate()

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/CostCenterMasterData.csv")

# Select specific columns
selected_columns = ["costCenter", "costCenterDesc", "profitCenter","profitCenterDesc","department","companyCode","companyCodeDesc","category","categoryDesc","country","currency","actualRevenues", "actualPrimaryCosts", "actualSecondaryCosts", "commitmentUpdate", "planRevenues", "planPrimaryCosts", "planSecondaryCosts", "personResponsible", "userResponsible", "lvl1", "lvl1Desc", "lvl2", "lvl2Desc", "lvl3", "lvl3Desc", "lvl4", "lvl4Desc", "lvl5", "lvl5Desc", "lvl6", "lvl6Desc"]

df = df.select(selected_columns)

# df now is a Spark DataFrame containing CSV data from "Files/CostCenterMasterData.csv".

for col_name in df.columns:
    #Remove NOVO 
    df = df.withColumn(col_name, regexp_replace(col_name, "NOVO", ""))
    #Replace by Nulls & Not assigned
    df = df.withColumn(col_name, when(col(col_name).contains("#"), None).otherwise(col(col_name)))
    df = df.withColumn(col_name, when(col(col_name).contains("Not assigned"), None).otherwise(col(col_name)))
    #Remove leading 0s
    df = df.withColumn(col_name, when(col(col_name).contains("-") | col(col_name).contains("0000000000"), col(col_name)).otherwise(regexp_replace(col_name, r'^[0]*', '')))

#Identify duplicates
duplicates = df.groupBy("costCenter").agg(count("*").alias("count")).filter("count > 1")
display(duplicates)

#Drop duplicates
df = df.dropDuplicates(["costCenter"])

#write Delta table & Define PK and constraints
#df.write.format("delta").option("primaryKey", "costCenter").mode("overwrite").save("Tables/costCenter")


display(df)

# CELL ********************

#This code is identifying the changes between source and destination for SCD Type 2 Dimensions and performing the pertinet insert/updates/delete operation


from pyspark.sql.functions import sha2, concat_ws

#We read from the destination and from the source and compare both dataframes

destination = spark.read.format("delta").load("Tables/costCenter")

# Calculate hash of source
#df2 = df.filter(df.costCenter =='018-111205')

#df = df.select("costCenter", "costCenterDesc")
#df2 = df.withColumn("concat", concat_ws(",", *df.columns))
df2 = df.withColumn("hash", sha2(concat_ws(",", *df.columns), 256))

# Calculate hash of destination
#destination = destination.filter(destination.costCenter =='018-111205')
#destination = destination.select("costCenter", "costCenterDesc")
#destination = destination.withColumn("concat", concat_ws(",", *destination.columns))
destination = destination.withColumn("hash", sha2(concat_ws(",", *destination.columns), 256))

#display(destination)
#display(df2)
#df.select("concat","hash").show()
#destination.select("concat","hash").show()

# Find differences where there is a match when joining Cost Center & the Hash value (Update operation)
update = df2.join(destination, ["costCenter"], "inner").filter(destination.hash != df2.hash)

display(update)
display(update.count())

#Identify insert operations
insert = df2.join(destination, ["costCenter"], "left").filter(destination.costCenter.isNull())
display(insert)
display(insert.count())

#Identify delete operations
delete = destination.join(df2, ["costCenter"], "left").filter(df2.costCenter.isNull())
display(delete)
display(delete.count())


# CELL ********************

from delta.tables import *

destination2 = DeltaTable.forPath(spark, "Tables/costCenter")

destination2.alias('dest') \
  .merge(
    df2.alias('source'),
    'dest.costCenter = source.costCenter'
  ) \
  .whenMatchedUpdate(set =
    {
      "costCenter": "source.costCenter",
      "costCenterDesc": "source.costCenterDesc",
      "profitCenter": "source.profitCenter",
      "profitCenterDesc": "source.profitCenterDesc"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "costCenter": "source.costCenter",
      "costCenterDesc": "source.costCenterDesc",
      "profitCenter": "source.profitCenter",
      "profitCenterDesc": "source.profitCenterDesc"
    }
  ) \
  .whenNotMatchedBySourceDelete() \
  .execute()

# CELL ********************

df = spark.sql("SELECT * FROM CMIQ_Playhouse.costCenter LIMIT 1000")
display(df)

# CELL ********************

# describe the history of a Delta table
spark.sql("DESCRIBE HISTORY 'Tables/costCenter'").show(truncate=False)
