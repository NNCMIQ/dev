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

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from holidays import Denmark
import pandas as pd

# Define the range of years
start_year = 2000
end_year = 2099

# Create a DataFrame with a range of dates
df = spark.range((end_year - start_year + 1) * 365).selectExpr(f"date_add(date('{start_year}-01-01'), cast(id as int)) as date")

# Extract the time dimensions
df = df.select(
    col("date"),
    year("date").alias("year"),
    quarter("date").alias("quarter"),
    month("date").alias("month"),
    dayofmonth("date").alias("day"),
    dayofweek("date").alias("weekday"),
    weekofyear("date").alias("week_of_year"),
    dayofyear("date").alias("day_of_year"),
    date_format("date", "MMMM").alias("month_name"),
    date_format("date", "EEEE").alias("weekday_name"),
    when(month("date") < 4, "Q1")
    .when(month("date") < 7, "Q2")
    .when(month("date") < 10, "Q3")
    .otherwise("Q4").alias("quarter_name"),
    when(dayofweek("date").isin([1, 7]), "Weekend")
    .otherwise("Weekday").alias("week_day_type")
)

# Define a window partitioned by year and month, ordered by date
window = Window.partitionBy("year", "month").orderBy("date")

# Add a column for the working day of the month, resetting for each month
df = df.withColumn("working_day_of_month", sum(when(col("weekday").between(2, 6), 1).otherwise(0)).over(window))

# Define a window partitioned by year and week of year, ordered by date
window = Window.partitionBy("year", "week_of_year").orderBy("date")

# Add a column for the working day of the week, resetting for each week
df = df.withColumn("working_day_of_week", sum(when(col("weekday").between(2, 6), 1).otherwise(0)).over(window))

# Show the DataFrame
#df.show()

# CELL ********************

df_p = df.toPandas()

# CELL ********************

df_p.loc[(df_p['year'] == 2025) & (df_p['month'] == 12) & (df_p['day'] == 25)]

# CELL ********************

df_p

# MARKDOWN ********************

# ## Adding holidays

# CELL ********************

import datetime

# Define a function to get Danish public holidays
def get_holidays(year):
    return Denmark(years=[year])

# Convert 'date' column to datetime.date
df_p['date'] = pd.to_datetime(df_p['date']).dt.date

# Add a column with a dictionary of holidays for each year
df_p['holidays'] = df_p['year'].apply(get_holidays)

# Add a column with the holiday name for each date
df_p['holiday_name'] = df_p.apply(lambda row: row['holidays'].get(row['date']), axis=1)


# Adding remaining holidays

df_p['date'] = pd.to_datetime(df_p['date'])

conditions = [
    ((df_p['date'].dt.month == 12) & (df_p['date'].dt.day == 25)),
    ((df_p['date'].dt.month == 12) & (df_p['date'].dt.day == 24)),
    ((df_p['date'].dt.month == 12) & (df_p['date'].dt.day == 26)),
    ((df_p['date'].dt.month == 5) & (df_p['date'].dt.day == 6)),
        ((df_p['date'].dt.month == 12) & (df_p['date'].dt.day == 31))
]

choices = [
    'Christmas Day',
    'Christmas Eve',
    'Boxing Day',
    'Danish Constitution Day',
    "New Year's Eve"
]

df_p['holiday_name'] = df_p['holiday_name'].mask(df_p['holiday_name'].isnull(), np.select(conditions, choices, default=None))


# Add a column indicating whether each date is a holiday
df_p['is_holiday'] = df_p['holiday_name'].apply(lambda x: 0 if pd.isnull(x) else 1)

df_p = df_p.drop('holidays', axis=1)




# CELL ********************


df_p.loc[(df_p['year'] == 2025) & (df_p['is_holiday'] == 1)]

# MARKDOWN ********************

#  #### Add relative columns

# CELL ********************

df_p['current_month'] = None
df_p['previous_month'] = None
df_p['next_month'] = None
df_p['moving_year'] = None
df_p['moving_month'] = None
df_p['moving_week'] = None
df_p['moving_day'] = None



# CELL ********************

df_p

# MARKDOWN ********************

# #### Create primary key

# CELL ********************

# If you want to start the index from 1 instead of 0
df_p['PKey'] = df_p['date'].dt.strftime('%Y%m%d')

# CELL ********************

df_p.columns

# MARKDOWN ********************

# #### Make warehouse table

# CELL ********************

from pyspark.sql.types import *
from delta.tables import*

# Define the schema for the DateDim Table
DeltaTable.createIfNotExists(spark) \
    .tableName("ShallowEnd.Dim_Date") \
    .addColumn("date", DateType()) \
    .addColumn("year", IntegerType()) \
    .addColumn("quarter", IntegerType()) \
    .addColumn("month", IntegerType()) \
    .addColumn("day", IntegerType()) \
    .addColumn("weekday", IntegerType()) \
    .addColumn("week_of_year", IntegerType()) \
    .addColumn("day_of_year", IntegerType()) \
    .addColumn("month_name", StringType()) \
    .addColumn("weekday_name", StringType()) \
    .addColumn("quarter_name", StringType()) \
    .addColumn("weekday_type", StringType()) \
    .addColumn("working_day_of_month", IntegerType()) \
    .addColumn("holiday_name", StringType()) \
    .addColumn("is_holiday", IntegerType()) \
    .addColumn("moving_year", IntegerType()) \
    .addColumn("moving_month", IntegerType()) \
    .addColumn("moving_week", IntegerType()) \
    .addColumn("moving_day", IntegerType()) \
    .addColumn("current_month", IntegerType()) \
    .addColumn("previous_month", IntegerType()) \
    .addColumn("next_month", IntegerType()) \
    .addColumn("p_key", IntegerType()) \
    .execute()

# CELL ********************

spark_df = spark.createDataFrame(df_p)

# CELL ********************

# Insert and update table

deltaTable = DeltaTable.forPath(spark, 'Tables/dim_date')

dfUpdates = spark_df

deltaTable.alias('table') \
.merge(
    dfUpdates.alias('updates'),
    'table.p_key = updates.PKey'
) \
.whenMatchedUpdate(set =
    {

    }
) \
.whenNotMatchedInsert(values =
    {
        "date": "updates.date",
        "year": "updates.year",
        "quarter": "updates.quarter",
        "month": "updates.month",
        "day": "updates.day",
        "weekday": "updates.weekday",
        "week_of_year": "updates.week_of_year",
        "day_of_year": "updates.day_of_year",
        "month_name": "updates.month_name",
        "weekday_name": "updates.weekday_name",
        "quarter_name": "updates.quarter_name",
        "weekday_type": "updates.week_day_type",
        "working_day_of_month": "updates.working_day_of_month",
        "holiday_name": "updates.holiday_name",
        "is_holiday": "updates.is_holiday",
        "moving_year": "updates.moving_year",
        "moving_month": "updates.moving_month",
        "moving_week": "updates.moving_week",
        "moving_day": "updates.moving_day",
        "current_month": "updates.current_month",
        "previous_month": "updates.previous_month",
        "next_month": "updates.next_month",
        "p_key": "updates.PKey",

    }
) \
.execute()

# CELL ********************


# CELL ********************

df = spark.sql("SELECT * FROM ShallowEnd.dim_date LIMIT 1000")
display(df)

# CELL ********************

(date
            ,year
            ,quarter
            ,month
            ,day
            ,weekday
            ,week_of_year
            ,day_of_year
            ,month_name
            ,weekday_name
            ,quarter_name
            ,weekday_type
            ,working_day_of_month
            ,holiday_name
            ,is_holiday
            ,moving_year 
            ,moving_month
            ,moving_week
            ,moving_day
            ,current_month
            ,previous_month
            ,next_month
            ,p_key)
