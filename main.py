import pandas as pd
import pandas.io.sql as psql
import psycopg2
from urllib.request import Request, urlopen
import os
from dotenv import load_dotenv

load_dotenv()

# Get environment variables
HEADER = os.getenv('HEADER','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36')
URL = os.getenv('URL','https://www.ibm.com/support/pages/sites/default/files/software-lifecycle/ibm_software_lifecycle_product_list.csv')
DB_NAME = os.getenv('DB_NAME','mha-ela')
DB_USERNAME = os.getenv('DB_USER','postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD','password')
DB_HOST = os.getenv('DB_HOST','127.0.0.1')
DB_PORT = os.getenv('DB_PORT',5432)
DB_SCHEMA = os.getenv('DB_SCHEMA','public')
DB_MASTERCATALOG_TABLE_NAME = os.getenv('DB_MASTERCATALOG_TABLE_NAME','mastercatalog_entries')
DB_TEMP_TABLE_NAME = os.getenv('DB_TEMP_TABLE_NAME','temp_table')

print("===================================================")
print("   Project Keeper - EOS Update Processing Starts   ")
print("===================================================")

print("Project Keeper > Download IBM SWG EOS List")

# Download EOS list
req = Request(URL)
req.add_header('User-Agent', HEADER)

content = urlopen(req) 
df = pd.read_csv(content)
df = df.reset_index()  # make sure indexes pair with number of rows

'''
for index, row in df.iterrows():
    print(row['IBM Product'], row['VRM'], index)
'''


print("Project Keeper > Loading Master Catalog")
# Make connenction to database
conn = psycopg2.connect(
   database=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

# Connect to database and load Master Catalog into data frame 
df_master_catalog = psql.read_sql('SELECT * FROM '+DB_SCHEMA+'.'+DB_MASTERCATALOG_TABLE_NAME+' ORDER BY id ASC', conn)
#print(df_master_catalog['program_description'])


print("Project Keeper > Perform Matching")
# Match EOS List with Master Catalog and update Catalog EOS Version and EOS Date for EOS Date greater than 1 Jan 2023
count = 0
df_eos = df[ (df['EOS Date'] > '2023-01-01') & (df['EOS Date'].str.len() > 0) ]
for index, row in df_eos.iterrows():
    for index2, row2 in df_master_catalog.iterrows():
        if row['IBM Product'] in row2['program_description']:
            print(count,row['IBM Product'], '-', row['VRM'], '\t matches with','\t', row2['program_description'])
            df_master_catalog.at[index2,'eos_version_number'] = row['VRM']
            df_master_catalog.at[index2,'eos_version_date'] = row['EOS Date']
        count = count + 1
#print(count)

#print(result)

# Filter row that doesn't not have EOS version
df_filtered_master_catalog = df_master_catalog[ (df_master_catalog['eos_version_number'].str.len() > 0) ]
#print(df_filtered_master_catalog)

# Create a temp table to store updated Master Catalog data frame with EOS Version and Date
print("Project Keeper > Update Master Catalog Table")

from sqlalchemy import create_engine

engine = create_engine('postgresql://'+DB_USERNAME+':'+DB_PASSWORD+'@'+DB_HOST+':'+str(DB_PORT)+'/'+DB_NAME+'?gssencmode=disable')
df_filtered_master_catalog.to_sql(DB_TEMP_TABLE_NAME,engine, if_exists='replace')

# To update temp table back to Master Catalog table
sql2 = """
    UPDATE mastercatalog_entries_2 AS f
    SET
      eos_version_number = t.eos_version_number,
      eos_version_date = t.eos_version_date::date
    FROM """ + DB_TEMP_TABLE_NAME + """ AS t
    WHERE f.id::text = t.id AND t.eos_version_number <> ''
"""

cursor.execute(sql2)
conn.commit()
conn.close()

print("===================================================")
print("  Project Keeper - EOS Update Processing Complete  ")
print("===================================================")