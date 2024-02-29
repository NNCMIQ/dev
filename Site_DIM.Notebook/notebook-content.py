# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "108fa7a0-fb80-4644-a34c-43a92e407a1c",
# META       "default_lakehouse_name": "OKRT_Playhouse",
# META       "default_lakehouse_workspace_id": "052c4811-3061-4b7d-86f7-c86b7345b87c",
# META       "known_lakehouses": [
# META         {
# META           "id": "108fa7a0-fb80-4644-a34c-43a92e407a1c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# read data from Excel
# df = spark.read.format("xlsx").option("header","true").load("Files/Capacity DB.xlsx")
import pyspark.pandas as ps
import pandas as pd
df = pd.read_excel("abfss://052c4811-3061-4b7d-86f7-c86b7345b87c@onelake.dfs.fabric.microsoft.com/108fa7a0-fb80-4644-a34c-43a92e407a1c/Files/Capacity_DB.xlsx")


# CELL ********************

dfs = ps.from_pandas(df).to_spark(index_col='index')

# iterate over all columns
for col in dfs.columns:
    # remove whitespaces and rename
    dfs = dfs.withColumnRenamed(col, col.replace('-', '').replace(' ',''))
    
    

display(dfs)        

dfs.write.format("delta").option("delta.columnMapping.mode", "name").option("overwriteSchema", "true").mode("Overwrite").saveAsTable("Site_Dim")

# CELL ********************

print(dfs.columns)

# CELL ********************

# MAGIC %%sql
# MAGIC select  
# MAGIC BuildingId ,  
# MAGIC BuildingStatus ,  
# MAGIC BuildingLocationtype ,  
# MAGIC BuildingOwner ,  
# MAGIC BuildingName, 
# MAGIC BuildingLocationtype, 
# MAGIC BuildingOwner,
# MAGIC BuildingName, 
# MAGIC BuildingCountry, 
# MAGIC BuildingCity, 
# MAGIC BuildingPostalcode, 
# MAGIC BuildingAddressline, 
# MAGIC BuildingComments, 
# MAGIC ProdLineId,
# MAGIC ProdLineStatus,
# MAGIC ProdLineSupplier,
# MAGIC FROM OKRT_Playhouse.site_dim

# CELL ********************

