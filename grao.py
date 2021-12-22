import urllib.request
import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile

#url = "https://www.grao.bg/tna/tadr2020.txt"
#url = "https://www.grao.bg/tna/t41nm-15.12.2021_2.txt"
from pandas.io.common import urlopen

url = "https://www.grao.bg/tna/tadr-2005.txt"

data = urllib.request.urlopen(url)

a = ""
sep = '|'
reg_str = ''
for line in data:
    decoded_line = line.decode("windows-1251")
    decoded_line = decoded_line.replace('!', '|')
    if 'ОБЛАСТ:' in decoded_line:
        region_arg = decoded_line.split()
        region = region_arg[0].replace('ОБЛАСТ:', '').strip()
        reg_str = region
    elif 'ОБЩИНА:' in decoded_line:
        mun_arg = decoded_line.split()
        municipality= mun_arg[0].replace('ОБЩИНА:', '').strip()
        reg_str = reg_str + sep + municipality
    elif 0 < decoded_line.count('община'):
        region_arg = decoded_line.split('община')
        region = region_arg[0].replace('област', '').strip()
        municipality = region_arg[1].strip()
        reg_str = region + sep + municipality
    elif (decoded_line.startswith('|С.')) or (' С.' in decoded_line):
        a = a + sep + reg_str + decoded_line.lstrip()
    elif 'ГР.' in decoded_line:
        a = a + sep + reg_str + decoded_line.lstrip()
    else:
        pass

#TO-DO Q codes, Ekatte, complete dataframes

#TO-DO date from inside markdown or URL, date into CSV -> export dataframe to CSV, for each quarter or year


df = pd.DataFrame([x.split('|') for x in a.split('\r\n')])


if len(df.columns) > 8:
    column_list = [0, 5, 6, 7, 9, 10, 11, 12]
    df.drop(df.columns[column_list],
            axis=1,
            inplace=True)
else:
    df = df.iloc[:, 1:-2]

df.columns = ['region', 'municipality', 'settlement', 'permanent_population', 'current_population']

df['region'] = df['region'].str.lower()
df['municipality'] = df['municipality'].str.lower()
df['settlement'] = df['settlement'].str.lower()
df['settlement'] = df['settlement'].str.replace('гр.', '', regex=False)
df['settlement'] = df['settlement'].str.replace('с.', '', regex=False)
df['settlement'] = df['settlement'].str.strip()


ekatte_url = 'http://www.nsi.bg/sites/default/files/files/EKATTE/Ekatte.zip'
content = requests.get(ekatte_url)
f = ZipFile(BytesIO(content.content))
print(f.namelist())