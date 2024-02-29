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

# CELL ********************

# Load data to the dataframe from Lakehouse
df = spark.read.table("ShallowEnd.dsi_FTE_SAPBW")

# CELL ********************

display(df)

# MARKDOWN ********************

# #### **Note: Cells containing _display_ is freezed for performance reasons. Unfreeze them to follow along**

# CELL ********************

df_show = spark.sql("SELECT * FROM ShallowEnd.dsi_FTE_SAPBW LIMIT 1000")
display(df_show)

# MARKDOWN ********************

# #### Adding year and month to date

# CELL ********************

from pyspark.sql import functions as F

# assuming df is your DataFrame, "year_col" is the year column, and "date_col" is the date column
df = df.withColumn("Month", df["Month"].substr(2, 3))
df = df.withColumn("Date", F.to_date(F.concat(df["Year"], df["Month"]), 'yyyyMM'))

# CELL ********************

display(df.head(50))

# MARKDOWN ********************

# #### Create DateDIM

# CELL ********************

from pyspark.sql.types import *
from delta.tables import*

# Define the schema for the DateDim Table
DeltaTable.createIfNotExists(spark) \
    .tableName("ShallowEnd.Dim_Date_FTE") \
    .addColumn("Date", DateType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .execute()

# MARKDOWN ********************

# #### Slice dataframe to get unique date dimensions

# CELL ********************

from pyspark.sql.functions import col, dayofmonth, month, year, date_format

# Create dataframe for dimDate

dfdimDate = df.dropDuplicates(["Date"]).select(col("Date"), col("Month"),col("Year")).orderBy("Date")


# CELL ********************

# Display date dimension
display(dfdimDate.head(50))

# MARKDOWN ********************

# #### Ensure updates gets reflected in DateDIM (maybe irrelavent now)

# CELL ********************

# Insert and update table

deltaTable = DeltaTable.forPath(spark, 'Tables/dim_date_fte')

dfUpdates = dfdimDate

deltaTable.alias('table') \
.merge(
    dfUpdates.alias('updates'),
    'table.Date = updates.Date'
) \
.whenMatchedUpdate(set =
    {

    }
) \
.whenNotMatchedInsert(values =
    {
        "Date": "updates.Date",
        "Month": "updates.Month",
        "Year": "updates.Year"
    }
) \
.execute()

# MARKDOWN ********************

# #### Create Cost/Profit Center

# CELL ********************

# Define the schema for the DateDim Table
DeltaTable.createIfNotExists(spark) \
    .tableName("ShallowEnd.Dim_CostProfit_Center") \
    .addColumn("CompanyCode", StringType()) \
    .addColumn("CostCenter", StringType()) \
    .addColumn("CostCenterDesc", StringType()) \
    .addColumn("CostCenterResponsible", StringType()) \
    .addColumn("CostCenterCategory", IntegerType()) \
    .addColumn("ProfitCenterHier1", StringType()) \
    .addColumn("ProfitCenterHier1Desc", StringType()) \
    .addColumn("ProfitCenterHier2", StringType()) \
    .addColumn("ProfitCenterHier2Desc", StringType()) \
    .addColumn("ProfitCenterHier3", StringType()) \
    .addColumn("ProfitCenterHier3Desc", StringType()) \
    .addColumn("ProfitCenterHier4", StringType()) \
    .addColumn("ProfitCenterHier4Desc", StringType()) \
    .addColumn("ProfitCenterHier5", StringType()) \
    .addColumn("ProfitCenterHier5Desc", StringType()) \
    .addColumn("ProfitCenterHier6", StringType()) \
    .addColumn("ProfitCenterHier6Desc", StringType()) \
    .addColumn("ProfitCenterHier7", StringType()) \
    .addColumn("ProfitCenterHier7Desc", StringType()) \
    .addColumn("Department", StringType()) \
    .execute()


    

# CELL ********************

# Create dataframe for dimDate

dfdimCP = df.dropDuplicates(["Department"]).select(col("Department"), col("CostCenter"), \
    col("CostCenterDesc"),col("CostCenterResponsible"),col("CostCenterCategory"), \
    col("ProfitCenterHier1"), col("ProfitCenterHier1Desc"),col("ProfitCenterHier2"),col("ProfitCenterHier2Desc"), \
    col("ProfitCenterHier3"), col("ProfitCenterHier3Desc"),col("ProfitCenterHier4"),col("ProfitCenterHier4Desc"), \
    col("ProfitCenterHier5"), col("ProfitCenterHier5Desc"),col("ProfitCenterHier6"),col("ProfitCenterHier6Desc"), \
    col("ProfitCenterHier7"), col("ProfitCenterHier7Desc"),col("CompanyCode")\
    ).orderBy("Department")


# CELL ********************

# Display dimension table
display(dfdimCP)

# CELL ********************

# Insert and update table 

deltaTable = DeltaTable.forPath(spark, 'Tables/dim_costprofit_center')

dfUpdates = dfdimCP

deltaTable.alias('table') \
.merge(
    dfUpdates.alias('updates'),
    'table.Department = updates.Department'
) \
.whenMatchedUpdate(set =
    {
        "Department": "updates.Department",
        "CostCenter": "updates.CostCenter",
        "CostCenterDesc": "updates.CostCenterDesc",
        "CostCenterResponsible": "updates.CostCenterResponsible",
        "CostCenterCategory": "updates.CostCenterCategory",
        "ProfitCenterHier1": "updates.ProfitCenterHier1",
        "ProfitCenterHier1Desc": "updates.ProfitCenterHier1Desc",
        "ProfitCenterHier2": "updates.ProfitCenterHier2",
        "ProfitCenterHier2Desc": "updates.ProfitCenterHier2Desc",
        "ProfitCenterHier3": "updates.ProfitCenterHier3",
        "ProfitCenterHier3Desc": "updates.ProfitCenterHier3Desc",
        "ProfitCenterHier4": "updates.ProfitCenterHier4",
        "ProfitCenterHier4Desc": "updates.ProfitCenterHier4Desc",
        "ProfitCenterHier5": "updates.ProfitCenterHier5",
        "ProfitCenterHier5Desc": "updates.ProfitCenterHier5Desc",
        "ProfitCenterHier6": "updates.ProfitCenterHier6",
        "ProfitCenterHier6Desc": "updates.ProfitCenterHier6Desc",
        "ProfitCenterHier7": "updates.ProfitCenterHier7",
        "ProfitCenterHier7Desc": "updates.ProfitCenterHier7Desc",
        "CompanyCode": "updates.CompanyCode",
    }
) \
.whenNotMatchedInsert(values =
    {
        "Department": "updates.Department",
        "CostCenter": "updates.CostCenter",
        "CostCenterDesc": "updates.CostCenterDesc",
        "CostCenterResponsible": "updates.CostCenterResponsible",
        "CostCenterCategory": "updates.CostCenterCategory",
        "ProfitCenterHier1": "updates.ProfitCenterHier1",
        "ProfitCenterHier1Desc": "updates.ProfitCenterHier1Desc",
        "ProfitCenterHier2": "updates.ProfitCenterHier2",
        "ProfitCenterHier2Desc": "updates.ProfitCenterHier2Desc",
        "ProfitCenterHier3": "updates.ProfitCenterHier3",
        "ProfitCenterHier3Desc": "updates.ProfitCenterHier3Desc",
        "ProfitCenterHier4": "updates.ProfitCenterHier4",
        "ProfitCenterHier4Desc": "updates.ProfitCenterHier4Desc",
        "ProfitCenterHier5": "updates.ProfitCenterHier5",
        "ProfitCenterHier5Desc": "updates.ProfitCenterHier5Desc",
        "ProfitCenterHier6": "updates.ProfitCenterHier6",
        "ProfitCenterHier6Desc": "updates.ProfitCenterHier6Desc",
        "ProfitCenterHier7": "updates.ProfitCenterHier7",
        "ProfitCenterHier7Desc": "updates.ProfitCenterHier7Desc",
        "CompanyCode": "updates.CompanyCode",

    }
) \
.execute()


# MARKDOWN ********************

# #### Geography

# CELL ********************

# Define the schema for the DateDim Table
DeltaTable.createIfNotExists(spark) \
    .tableName("ShallowEnd.Dim_Geo_FTE") \
    .addColumn("Department", StringType()) \
    .addColumn("Country", StringType()) \
    .addColumn("Currency", StringType()) \
    .execute()


# CELL ********************

# Create dataframe for dimDate

dfdimGeo = df.dropDuplicates(["Department"]).select(col("Department"), col("Country"), \
    col("Currency") \
    ).orderBy("Department")


# CELL ********************

# Display to preview dimension 
display(dfdimGeo)

# CELL ********************

deltaTable = DeltaTable.forPath(spark, 'Tables/dim_geo_fte')

dfUpdates = dfdimGeo

deltaTable.alias('table') \
.merge(
    dfUpdates.alias('updates'),
    'table.Department = updates.Department'
) \
.whenMatchedUpdate(set =
    {

    }
) \
.whenNotMatchedInsert(values =
    {
        "Department": "updates.Department",
        "Country": "updates.Country",
        "Currency": "updates.Currency"        

    }
) \
.execute()


# MARKDOWN ********************

# #### Job Descriptions and Grouping

# CELL ********************

# Define the schema for the DateDim Table
DeltaTable.createIfNotExists(spark) \
    .tableName("ShallowEnd.Dim_JobDesc") \
    .addColumn("GlobalJob", IntegerType()) \
    .addColumn("GlobalJobDesc", StringType()) \
    .addColumn("GlobalJobLvl", StringType()) \
    .addColumn("HRJobGroup2", StringType()) \
    .addColumn("HRJobGroup2Desc", StringType()) \
    .addColumn("HRJobGroup1", StringType()) \
    .addColumn("HRJobGroup1Desc", StringType()) \
    .execute()


# CELL ********************

# Create dataframe for dimDate

dfdimJobD = df.dropDuplicates(["GlobalJob"]).select( \
    col("GlobalJob"), \
    col("GlobalJobDesc"), \
    col("GlobalJobLvl"), \
    col("HRJobGroup2"),\
    col("HRJobGroup2Desc"), \
    col("HRJobGroup1"),\
    col("HRJobGroup1Desc") \

    )

# CELL ********************

# Display to preview your dimension
display(dfdimJobD)

# CELL ********************

deltaTable = DeltaTable.forPath(spark, 'Tables/dim_jobdesc')

dfUpdates = dfdimJobD

deltaTable.alias('table') \
.merge(
    dfUpdates.alias('updates'),
    'table.GlobalJob = updates.GlobalJob'
) \
.whenMatchedUpdate(set =
    {

    }
) \
.whenNotMatchedInsert(values =
    {
        "GlobalJob": "updates.GlobalJob",
        "GlobalJobDesc": "updates.GlobalJobDesc",
        "GlobalJobLvl": "updates.GlobalJobLvl", 
        "HRJobGroup2": "updates.HRJobGroup2",
        "HRJobGroup2Desc": "updates.HRJobGroup2Desc",
        "HRJobGroup1": "updates.HRJobGroup1",
        "HRJobGroup1Desc": "updates.HRJobGroup1Desc"       

    }
) \
.execute()


# MARKDOWN ********************

# #### FTE fact table

# CELL ********************

# Define the schema for the FactFTE Table
DeltaTable.createIfNotExists(spark) \
    .tableName("ShallowEnd.Fact_FTE") \
    .addColumn("FTE_REA", FloatType()) \
    .addColumn("Department", StringType()) \
    .addColumn("GlobalJob", IntegerType()) \
    .addColumn("Date", DateType()) \
    .addColumn("JobFamily", StringType()) \
    .addColumn("SubFamily", StringType()) \
    .addColumn("IntExt", IntegerType()) \
    .addColumn("NNHeadCountID", IntegerType()) \
    .execute()


# CELL ********************

# Create dataframe for FactFTE

dfFactFTE = df.select( \
    col("NN_FTE_Rea"), \
    col("Department"), \
    col("GlobalJob"), \
    col("Date"),\
    col("JobFamily"),\
    col("SubFamily"),\
    col("IntExt"), \
    col("NNHeadCountID") \
    )


# CELL ********************

# Display to view dimension
display(dfFactFTE)

# CELL ********************

deltaTable = DeltaTable.forPath(spark, 'Tables/fact_fte')

dfUpdates = dfFactFTE

deltaTable.alias('table') \
.merge(
    dfUpdates.alias('updates'),
    'table.Department = updates.Department AND \
    table.GlobalJob = updates.GlobalJob AND \
    table.Date = updates.Date AND \
    table.JobFamily = updates.JobFamily AND \
    table.SubFamily = updates.SubFamily AND \
    table.IntExt = updates.IntExt AND \
    table.NNHeadCountID = updates.NNHeadCountID'
) \
.whenMatchedUpdate(set =
    {

    }
) \
.whenNotMatchedInsert(values =
    {
        "FTE_Rea": "updates.NN_FTE_Rea",
        "Department": "updates.Department",
        "GlobalJob": "updates.GlobalJob", 
        "Date": "updates.Date",
        "JobFamily": "updates.JobFamily",
        "SubFamily": "updates.SubFamily",
        "IntExt": "updates.IntExt",
        "NNHeadCountID": "updates.NNHeadCountID"

    }
) \
.execute()


# CELL ********************

df_test = spark.sql("SELECT IntExt, SUM(FTE_REA) FROM ShallowEnd.fact_fte GROUP BY IntExt")
display(df_test)

# MARKDOWN ********************

# ####
