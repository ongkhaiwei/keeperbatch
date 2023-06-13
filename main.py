import pandas as pd
import psycopg2
from urllib.request import Request, urlopen

headers = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'
url = 'https://www.ibm.com/support/pages/sites/default/files/software-lifecycle/ibm_software_lifecycle_product_list.csv'

req = Request(url)
req.add_header('User-Agent', headers)
content = urlopen(req) 

df = pd.read_csv(content)
#print(df['IBM Product'])
df = df.reset_index()  # make sure indexes pair with number of rows

for index, row in df.iterrows():
    print(row['IBM Product'], row['VRM'], index)