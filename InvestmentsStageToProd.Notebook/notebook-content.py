# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "f1b082c4-7874-4e1e-a299-4b0e85b794cd",
# META       "default_lakehouse_name": "CMIQ_Playhouse",
# META       "default_lakehouse_workspace_id": "052c4811-3061-4b7d-86f7-c86b7345b87c",
# META       "known_lakehouses": [
# META         {
# META           "id": "f1b082c4-7874-4e1e-a299-4b0e85b794cd"
# META         },
# META         {
# META           "id": "36172c68-b4e1-49e7-b5f6-5f8ece5c14a6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Start Spark Session**

# CELL ********************

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('appName').getOrCreate()

# MARKDOWN ********************

# **IMPORTS**

# CELL ********************

from pyspark.sql.functions import avg
from pyspark.sql.functions import sum
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit

# MARKDOWN ********************

# **LOAD AND CLEAN STAGING**

# CELL ********************

#IN SQL
#df = spark.sql("SELECT * FROM CMIQ_Staging.InvestmentSpendFact LIMIT 1000")

#different approach
incrementalLoad = spark.read.format("delta") \
.load("abfss://052c4811-3061-4b7d-86f7-c86b7345b87c@onelake.dfs.fabric.microsoft.com/36172c68-b4e1-49e7-b5f6-5f8ece5c14a6/Tables/InvestmentSpendFact").limit(100)


#Add Audit fields
incrementalLoad = incrementalLoad.withColumn("loadTime", current_timestamp()) \
       .withColumn("loadJob", lit("InvestmentSpendFact"))

#HERE WE SHOULD ADD SOME QUALITY CHECKS ON THE DATA
#display(incrementalLoad)

# MARKDOWN ********************

# **DELETE NEWEST DATA FROM PROD IF DATA QUALITY IS GOOD**

# CELL ********************

#Identify the date of what's going to be updated
maxDate = spark.sql("select Max(Date) as max_date FROM CMIQ_Staging.InvestmentSpendFact").collect()[0]["max_date"]

#override incremental load
query = "delete FROM CMIQ_Playhouse.InvestmentSpendFact where Date >= '" + str(maxDate) + "'"
spark.sql(query)


# MARKDOWN ********************

# **APPEND DATA TO PROD**

# CELL ********************

#write Delta table & Define PK and constraints
incrementalLoad.write.format("delta").mode("append").save("abfss://052c4811-3061-4b7d-86f7-c86b7345b87c@onelake.dfs.fabric.microsoft.com/f1b082c4-7874-4e1e-a299-4b0e85b794cd/Tables/InvestmentSpendFact")

