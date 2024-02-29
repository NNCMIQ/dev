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

# Placeholders for Azure SQL Database connection info
server_name = "db.ada-prd.corp.aws.novonordisk.com"
port_number = 1433  # Default port number for SQL Server
database_name = "ADA"
client_id = "CMIQ"  # Service principal client ID
client_secret = ""  # Service principal client secret
tenant_id = "YOUR_TENANT_ID"  # Azure Active Directory tenant ID


# Build the Azure SQL Database JDBC URL with Service Principal (Active Directory Integrated)
jdbc_url = f"jdbc:sqlserver://{server_name}:{port_number};database={database_name};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;Authentication=ActiveDirectoryIntegrated"


# Properties for the JDBC connection
properties = {
    #"user": client_id, 
    #"password": client_secret,  
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
   # "tenantId": tenant_id  
}

# Read entire table from Azure SQL Database using AAD Integrated authentication
sql_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Show the Azure SQL DataFrame
sql_df.show()
