# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "3eb89a7a-df5e-4af1-9c13-3a32194b1f1e",
# META       "default_lakehouse_name": "ShallowEnd",
# META       "default_lakehouse_workspace_id": "052c4811-3061-4b7d-86f7-c86b7345b87c",
# META       "known_lakehouses": [
# META         {
# META           "id": "3eb89a7a-df5e-4af1-9c13-3a32194b1f1e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Loading data from a lakehouse can be done quickly from either a table or file

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/ShowSales/sales.csv")
# df now is a Spark DataFrame containing CSV data from "Files/ShowSales/sales.csv".
display(df)

# MARKDOWN ********************

# ### Or remote connections

# CELL ********************

 # Azure Blob Storage access info
 blob_account_name = "azureopendatastorage"
 blob_container_name = "nyctlc"
 blob_relative_path = "yellow"
    
 # Construct connection path
 wasbs_path = f'wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}'
 print(wasbs_path)
    
 # Read parquet data from Azure Blob Storage path
 blob_df = spark.read.parquet(wasbs_path)

 display(blob_df.limit(10))

# MARKDOWN ********************

# #### Transformations

# CELL ********************

from pyspark.sql.functions import col, to_timestamp, current_timestamp, year, month
from pyspark.sql.types import DateType

# Specifies the datatype
df = df.withColumn('OrderDate', df['OrderDate'].cast(DateType()))

# add a new column 'Month' that extracts the month number from 'OrderDate'
df = df.withColumn('Month', month('OrderDate'))

display(df)

# MARKDOWN ********************

# ### Lets focus on a file already in the Lakehouse
# #### We can either create a managed table or an external by explicitly specifiying a file path

# CELL ********************

df.limit(1000).write.format("delta").saveAsTable("managed_table")

# CELL ********************

df.limit(1000).write.format("delta").saveAsTable("external_table", path="Files/externaltable")

# MARKDOWN ********************

# #### Note: For a notebook used in a datapipeline we would want to overwrite or update the current table as follows:

# CELL ********************

df.limit(1000).write.format("delta").mode("overwrite").saveAsTable("sales_data", path="Files/externaltable")

# MARKDOWN ********************

# #### We can also write the data as a parquet file to then later create a table

# CELL ********************

     # Declare file name    
     file_name = "ShowNotes"
    
     # Construct destination path
     output_parquet_path = f"abfss://052c4811-3061-4b7d-86f7-c86b7345b87c@onelake.dfs.fabric.microsoft.com/3eb89a7a-df5e-4af1-9c13-3a32194b1f1e/Files/RawData/{file_name}"
     print(output_parquet_path)
     
     # Load the first 1000 rows as a Parquet file
     df.limit(1000).write.mode("overwrite").parquet(output_parquet_path)

     # Write DataFrame to Delta table
     delta_table_name = 'sales_data'
     df.limit(1000).write.format("delta").mode("overwrite").saveAsTable(delta_table_name)
     print(f"DataFrame has been written to Delta table: {delta_table_name}")

# MARKDOWN ********************

# #### Once the table has been created, it can be accessed by loading it into a dataframe via SparkSQL, or queried using SQL

# CELL ********************

df = spark.sql("SELECT * FROM ShallowEnd.sales_data LIMIT 1000")
display(df)

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT YEAR(OrderDate) AS OrderYear,
# MAGIC        SUM((UnitPrice * Quantity) + TaxAmount) AS GrossRevenue
# MAGIC FROM sales_data
# MAGIC GROUP BY YEAR(OrderDate)
# MAGIC ORDER BY OrderYear;
