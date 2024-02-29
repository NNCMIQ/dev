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

!pip install requests_negotiate_sspi

# CELL ********************

import pandas as pd
import requests
from requests_negotiate_sspi import HttpNegotiateAuth

# CELL ********************

EVALUATE
SUMMARIZE(
    'FTE_Model',
    'Calendar'[Date],
    'Cost Center'[Cost Center Code],
    'Cost Center'[Cost Center Name],
    'Cost Center'[Cost Center Person Responsible],
    'Global Job'[Global Job],
    'Global Job'[Global job Job Group Level 1 Code],
    'Global Job'[Global job Job Group Level 1 Name],
    'Profit Center'[Hierarchy Name],
    'Profit Center'[Profit Center Level 1 Key],
    'Profit Center'[Profit Center Level 1 Name],
    'Profit Center'[Profit Center Level 2 Key],
    'Profit Center'[Profit Center Level 2 Name],
    'Profit Center'[Profit Center Level 3 Key],
    'Profit Center'[Profit Center Level 3 Name],
    'Profit Center'[Profit Center Level 4 Key],
    'Profit Center'[Profit Center Level 4 Name],
    'Profit Center'[Profit Center Name],
    'JobFamily'[JobFamily Name],
    'NNHCT InternalExternal'[NNHCTInternalExternal Key],
    "REA23", [Measures].[REA23]
)
