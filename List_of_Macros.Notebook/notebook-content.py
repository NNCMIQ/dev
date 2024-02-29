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

import os
import xml.etree.ElementTree as ET
import re

# specify your directory path
directory = '/lakehouse/default/Files/'

# open the output file
with open('output.txt', 'w') as f:
    # iterate over all files in the directory
    for filename in os.listdir(directory):
        # check if the file is a .yxmd file
        if filename.endswith('.yxmd'):
            # get the full file path
            filepath = os.path.join(directory, filename)

            # parse the XML document
            tree = ET.parse(filepath)
            root = tree.getroot()

            # Find the Name element and extract its text
            name = root.find('.//MetaInfo/Name').text

            # Find all the Node elements and extract the Macro attribute from the EngineSettings element if it exists
            macros = []
            for node in root.findall('.//Node'):
                engine_settings = node.find('EngineSettings')
                if engine_settings is not None and 'Macro' in engine_settings.keys():
                    macros.append(engine_settings.get('Macro'))

            # Find the Node elements where the Plugin attribute in GuiSettings is "AlteryxBasePluginsGui.DbFileInput.DbFileInput"
            # and extract the File element text from the Configuration element
            sources = []

            for node in root.findall('.//Node'):
                gui_settings = node.find('GuiSettings')
                if gui_settings is not None and 'Plugin' in gui_settings.keys() and gui_settings.get('Plugin') == "AlteryxBasePluginsGui.DbFileInput.DbFileInput":
                    source_file = node.find('.//Configuration/File').text
                    sources.append(source_file)


            for node in root.findall('.//Node'):
                gui_settings = node.find('GuiSettings')
                if gui_settings is not None and 'Plugin' in gui_settings.keys() and gui_settings.get('Plugin') == "LockInGui.LockInInput.LockInInput":
                    query = node.find('.//Configuration/Query').text
                    table_name = re.search('FROM\s+(\S+)', query, re.IGNORECASE)
                    #print(table)
                    if table_name:
                        sources.append(table_name.group(1))


            # Find the Node element where the Plugin attribute in GuiSettings is "LockInGui.LockInOutput.LockInOutput"
            # and extract the Table element text from the Configuration element
            output_tables = []
            for node in root.findall('.//Node'):
                gui_settings = node.find('GuiSettings')
                if gui_settings is not None and 'Plugin' in gui_settings.keys() and gui_settings.get('Plugin') == "LockInGui.LockInOutput.LockInOutput":
                    output_table = node.find('.//Configuration/Table').text
                    output_tables.append(output_table)

            print(f'Name: {name}')
            print(f'Source Files:')
            for source in sources:
                print(source)
            print(f'Output Tables:')
            for output_table in output_tables:
                print(output_table)
            print('Macros:')
            for macro in macros:
                print(macro)

            # write the name, source files, output tables, macros, and connections to the output file
            f.write(f'Name: {name}\n')
            f.write('Source Files:\n')
            for source in sources:
                f.write(f'{source}\n')
            f.write('Output Tables:\n')
            for output_table in output_tables:
                f.write(f'{output_table}\n')
            f.write('Macros:\n')
            for macro in macros:
                f.write(f'{macro}\n')

# CELL ********************

df = spark.read.text("Files/output.txt")
# df now is a Spark DataFrame containing text data from "Files/output.txt".
display(df)
