# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "23c7a180-b753-4476-9418-e7e9a3e7ad6e",
# META       "default_lakehouse_name": "Silver",
# META       "default_lakehouse_workspace_id": "052c4811-3061-4b7d-86f7-c86b7345b87c",
# META       "known_lakehouses": [
# META         {
# META           "id": "23c7a180-b753-4476-9418-e7e9a3e7ad6e"
# META         },
# META         {
# META           "id": "277cde3f-72c0-40b1-b159-f2fc33b811a3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Read from Excel file from Bronze Lakehouse and apply Table schema**

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pandas as pd

#URL to Bronze folder
path = 'abfss://052c4811-3061-4b7d-86f7-c86b7345b87c@onelake.dfs.fabric.microsoft.com/277cde3f-72c0-40b1-b159-f2fc33b811a3/Files/'
input_file = 'PEM Scorecard.xlsx'
url = path+input_file

 # Create the schema for the table
orderSchema = StructType([
    StructField("ProjectId", StringType()),
    StructField("VPArea", StringType()),
    StructField("Investment", StringType()),
    StructField("PMInitials", StringType()),
    StructField("ScopeStatus", StringType()),
    StructField("CostStatus", StringType()),
    StructField("ScheduleStatus", StringType()),
    StructField("RiskStatus", StringType()),
    StructField("PhaseStatus", StringType()),
    StructField("ProjectStart", StringType()),
    StructField("ProjectEnd", StringType()),
    StructField("Grant", StringType()),
    StructField("TotalProjectCost", StringType()),
    StructField("ActualCost", StringType()),
    StructField("StatusComment", StringType()),
    StructField("Goal", StringType()),
    StructField("SVPArea", StringType()),
    StructField("IAID", StringType()),
    StructField("TotalPlannedCost", FloatType()),
    StructField("ComplexityLevel", StringType()),
    StructField("LastUpdated", StringType()),
    StructField("CostCenter", StringType()),
    StructField("Active", StringType()),
    StructField("Tan/Intan", StringType()),
    StructField("PortfolioStatus", StringType()),
    StructField("SAPCostCenterId", StringType()),
    StructField("Location", StringType()),
    StructField("StatusReportDate", StringType()),
    StructField("DepartmentManager", StringType()),
    StructField("Quality", StringType())
     ])
    
# Read the Excel file using pandas
pd_df = pd.read_excel(url)

#Create Spark Data frame with new defined Schema
df = spark.createDataFrame(pd_df, schema=orderSchema)

 # Display the first 10 rows of the dataframe to preview your data
#display(df.head(100))

# MARKDOWN ********************

# # **Clean the dataframe**

# CELL ********************

 from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name,  to_date, upper

 # Cast to Date and Float and Add columns, CreatedTS and ModifiedTS

df_clean = df.withColumn("LastUpdated", to_date("LastUpdated", "MM/dd/yy")) \
.withColumn("StatusReportDate", to_date("StatusReportDate", "MM/dd/yy")) \
.withColumn("ProjectStart", to_date("StatusReportDate", "MM/dd/yy")) \
.withColumn("ProjectEnd", to_date("StatusReportDate", "MM/dd/yy")) \
.withColumn("StatusReportDate", to_date("StatusReportDate", "MM/dd/yy")) \
.withColumn("TotalProjectCost", col("TotalProjectCost").cast("float")) \
.withColumn("ActualCost", col("ActualCost").cast("float")) \
.withColumn("Grant", col("Grant").cast("float")) \
.withColumn("TotalPlannedCost", col("TotalPlannedCost").cast("float")) \
.withColumn("PMInitials", upper(col("PMInitials"))) \
.withColumn("DepartmentManager", upper(col("DepartmentManager"))) \
.withColumn("isIPG", when(col("TotalProjectCost") > 500000000,True).otherwise(False)) \
.withColumn("CreatedTS", current_timestamp()).withColumn("ModifiedTS", current_timestamp())

df_clean = df_clean.dropDuplicates(["ProjectId"])

#Rearrange colunms
df_clean = df_clean.select("ProjectId",
 "Investment",
 "CostCenter",
 "SAPCostCenterId",
 "Active",
 "Location",
 "Tan/Intan",
 "Goal",
 "PhaseStatus",
 "ComplexityLevel",
 "ProjectStart",
 "ProjectEnd",
 "TotalProjectCost",
 "Grant",
 "ActualCost",
 "isIPG",
 "LastUpdated",
 "DepartmentManager",
 "PMInitials",
 "StatusReportDate",
 "ScopeStatus",
 "CostStatus",
 "RiskStatus",
 "ScheduleStatus",
 "PortfolioStatus",
 "StatusComment",
 "Quality",
 "CreatedTS",
 "ModifiedTS")  # Specify the order of columns as needed

#display(df_clean)

# MARKDOWN ********************

# # **Create delta table if not exist**

# CELL ********************

 
 from pyspark.sql import SparkSession
 
 from pyspark.sql.types import *
 from delta.tables import *

 # Create Delta table

 DeltaTable.createIfNotExists(spark) \
    .tableName("ClarityProjects") \
	.addColumn("ProjectId", StringType()) \
	.addColumn("Investment", StringType()) \
	.addColumn("CostCenter", StringType()) \
	.addColumn("SAPCostCenterId", StringType()) \
	.addColumn("Active", StringType()) \
	.addColumn("Location", StringType()) \
	.addColumn("Tan/Intan", StringType()) \
	.addColumn("Goal", StringType()) \
	.addColumn("PhaseStatus", StringType()) \
	.addColumn("ComplexityLevel", StringType()) \
	.addColumn("ProjectStart", DateType()) \
	.addColumn("ProjectEnd", DateType()) \
	.addColumn("TotalProjectCost", FloatType()) \
	.addColumn("Grant", FloatType()) \
	.addColumn("ActualCost", FloatType()) \
	.addColumn("isIPG", BooleanType()) \
	.addColumn("LastUpdated", DateType()) \
	.addColumn("DepartmentManager", StringType()) \
	.addColumn("PMInitials", StringType()) \
	.addColumn("StatusReportDate", DateType()) \
	.addColumn("ScopeStatus", StringType()) \
	.addColumn("CostStatus", StringType()) \
	.addColumn("RiskStatus", StringType()) \
	.addColumn("ScheduleStatus", StringType()) \
	.addColumn("PortfolioStatus", StringType()) \
	.addColumn("StatusComment", StringType()) \
	.addColumn("Quality", StringType()) \
	.addColumn("CreatedTS", TimestampType()) \
	.addColumn("ModifiedTS", TimestampType()) \
    .execute()


# MARKDOWN ********************

# # ****Define records in scope for update and upsert Operation**

# CELL ********************

from delta.tables import *

#Import deltaTable to query lastUpdateDate
deltaTable = DeltaTable.forPath(spark, 'abfss://052c4811-3061-4b7d-86f7-c86b7345b87c@onelake.dfs.fabric.microsoft.com/23c7a180-b753-4476-9418-e7e9a3e7ad6e/Tables/clarityprojects')

#We create a dataframe only with the updated records for the upsert operation
lastUpdated = deltaTable.toDF().selectExpr("max(LastUpdated) as lastUpdated").collect()[0]["lastUpdated"]

display(lastUpdated)

df_update = df_clean.where(col("LastUpdated") > lastUpdated)

display(df_update.count())

deltaTable.alias('silver') \
  .merge(
    df_update.alias('updates'),
    'silver.ProjectId = updates.ProjectId'
  ) \
  .whenMatchedUpdate(set =
    {
      "Investment": "updates.Investment",
      "CostCenter": "updates.CostCenter",
      "SAPCostCenterId": "updates.SAPCostCenterId",
      "Active": "updates.Active",
      "Location": "updates.Location",
      "Goal": "updates.Goal",
      "PhaseStatus": "updates.PhaseStatus",
      "ComplexityLevel": "updates.ComplexityLevel",
      "ProjectStart": "updates.ProjectStart",
      "ProjectEnd": "updates.ProjectEnd",
      "TotalProjectCost": "updates.TotalProjectCost",
      "Grant": "updates.Grant",
      "ActualCost": "updates.ActualCost",
      "isIPG": "updates.isIPG",
      "LastUpdated": "updates.LastUpdated",
      "DepartmentManager": "updates.DepartmentManager",
      "PMInitials": "updates.PMInitials",
      "StatusReportDate": "updates.StatusReportDate",
      "ScopeStatus": "updates.ScopeStatus",
      "CostStatus": "updates.CostStatus",
      "RiskStatus": "updates.RiskStatus",
      "ScheduleStatus": "updates.ScheduleStatus",
      "PortfolioStatus": "updates.PortfolioStatus",
      "StatusComment": "updates.StatusComment",
      "Quality": "updates.Quality",
      "ModifiedTS": current_timestamp()
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "ProjectId": "updates.ProjectId",
      "Investment": "updates.Investment",
      "CostCenter": "updates.CostCenter",
      "SAPCostCenterId": "updates.SAPCostCenterId",
      "Active": "updates.Active",
      "Location": "updates.Location",
      "Goal": "updates.Goal",
      "PhaseStatus": "updates.PhaseStatus",
      "ComplexityLevel": "updates.ComplexityLevel",
      "ProjectStart": "updates.ProjectStart",
      "ProjectEnd": "updates.ProjectEnd",
      "TotalProjectCost": "updates.TotalProjectCost",
      "Grant": "updates.Grant",
      "ActualCost": "updates.ActualCost",
      "isIPG": "updates.isIPG",
      "LastUpdated": "updates.LastUpdated",
      "DepartmentManager": "updates.DepartmentManager",
      "PMInitials": "updates.PMInitials",
      "StatusReportDate": "updates.StatusReportDate",
      "ScopeStatus": "updates.ScopeStatus",
      "CostStatus": "updates.CostStatus",
      "RiskStatus": "updates.RiskStatus",
      "ScheduleStatus": "updates.ScheduleStatus",
      "PortfolioStatus": "updates.PortfolioStatus",
      "StatusComment": "updates.StatusComment",
      "Quality": "updates.Quality",
      "CreatedTS": current_timestamp(),
      "ModifiedTS": current_timestamp()
    }
  ) \
  .execute()
