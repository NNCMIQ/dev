# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "3eb89a7a-df5e-4af1-9c13-3a32194b1f1e",
# META       "default_lakehouse_name": "ShallowEnd",
# META       "default_lakehouse_workspace_id": "052c4811-3061-4b7d-86f7-c86b7345b87c"
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


df = df.select(
    col("date"),
    year("date").alias("year"),
    month("date").alias("month")
    )

df = df.withColumn(
    "primary_key",
    concat(
        year(col("date")).cast("string"),
        lpad(month(col("date")).cast("string"), 2, '0'),
        lpad(dayofmonth(col("date")).cast("string"), 2, '0')
    ).cast("integer")
)

df_p = df.toPandas()


# CELL ********************

df_p

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

df_hol = df_p[df_p['is_holiday']==1].drop(['year','month'], axis=1)

# CELL ********************

df_hol

# CELL ********************

sparkDF=spark.createDataFrame(df_hol) 

# CELL ********************

from pyspark.sql.types import *
from delta.tables import*

# Define the schema for the DateDim Table
DeltaTable.createIfNotExists(spark) \
    .tableName("ShallowEnd.DIM_holiday") \
    .addColumn("HolidayName", StringType()) \
    .addColumn("isHoliday", IntegerType()) \
    .addColumn("pKey", IntegerType()) \
    .execute()

# CELL ********************

# Insert and update table

deltaTable = DeltaTable.forPath(spark, 'Tables/dim_holiday')

dfUpdates = sparkDF

deltaTable.alias('table') \
.merge(
    dfUpdates.alias('updates'),
    'table.pKey = updates.primary_key'
) \
.whenMatchedUpdate(set =
    {

    }
) \
.whenNotMatchedInsert(values =
    {
        "HolidayName": "updates.holiday_name",
        "isHoliday": "updates.is_holiday",
        "pKey": "updates.primary_key",

    }
) \
.execute()
