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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# CELL ********************

df = spark.sql("delete FROM CMIQ_Playhouse.costCenter where costCenter is null or costCenter = ''")
display(df)
