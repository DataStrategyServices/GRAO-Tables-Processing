import logging
import urllib.request
import os

#import fuzzymatcher
import time

import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile
from collections import OrderedDict
from wikidataintegrator import wdi_core, wdi_login

# TO-DO Better logging

logging.basicConfig(filename='ekatte.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('Something went wrong, read the ekatte.log')


#It works with both annual and quarterly, but annual is not really needed
#url = "https://www.grao.bg/tna/tadr2020.txt"

url = "https://www.grao.bg/tna/t41nm-15.12.2021_2.txt"

data = urllib.request.urlopen(url)

# get the date from the start of the file
read_date = urllib.request.urlopen(url).read().decode('windows-1251')
start_date = read_date.find('дата ')
end_date = read_date.find('\r\n', start_date)
date_var = read_date[start_date+5:end_date]


a = ""
sep = "|"
reg_str = ""
for line in data:
    decoded_line = line.decode("windows-1251")
    decoded_line = decoded_line.replace("!", "|")
    if "ОБЛАСТ:" in decoded_line:
        region_arg = decoded_line.split()
        region = region_arg[0].replace("ОБЛАСТ:", "").strip()
        reg_str = region
    elif "ОБЩИНА:" in decoded_line:
        mun_arg = decoded_line.split(" НА НАСЕЛЕНИЕТО")
        municipality= mun_arg[0].replace("ОБЩИНА:", "").strip()
        reg_str = reg_str + sep + municipality
    elif 0 < decoded_line.count("община"):
        region_arg = decoded_line.split("община")
        region = region_arg[0].replace("област", "").strip()
        municipality = region_arg[1].strip()
        reg_str = region + sep + municipality
    elif (decoded_line.startswith("|С.")) or (" С." in decoded_line):
        a = a + sep + reg_str + decoded_line.lstrip()
    elif "ГР." in decoded_line:
        a = a + sep + reg_str + decoded_line.lstrip()
    # elif "МАН." in decoded_line:
    #     a = a + sep + reg_str + decoded_line.lstrip()
    else:
        pass

#TO-DO Q codes, Ekatte, complete dataframes

#TO-DO date from inside markdown or URL, date into CSV -> export dataframe to CSV, for each quarter or year


df = pd.DataFrame([x.split("|") for x in a.split("\r\n")])

#check if raw data was properly saved
grao_filename = f'grao_data_{date_var}.csv'
grao_directory = 'grao_data'
if not os.path.exists(grao_directory):
      os.makedirs(grao_directory)

df.to_csv(f'./{grao_directory}/{grao_filename}', sep=",", encoding="utf-8", index=False)

#check if it's annual or quarterly!

if len(df.columns) > 8:
    column_list = [0, 5, 6, 7, 9, 10, 11, 12]
    df.drop(df.columns[column_list],
            axis=1,
            inplace=True)
else:
    df = df.iloc[:, 1:-2]

#arrange columns properly
df.columns = ["region", "municipality", "settlement", "permanent_population", "current_population"]

# clear out all "кв." and "ж.к" and "к.к."
def clear_kv_zk(column, string):
    # Receives a column name and a string to filter the dataframe of unneeded rows
    filter = df[column].str.contains(string, na=False, regex=False)
    return df[~filter]

rep_list = ["КВ.", "Ж.К.", "К.К."]

for string in rep_list:
    df = clear_kv_zk('settlement', string)



# a lot of repeating code to turn into 2-3 functions
columns_to_transform = ["region", "municipality", "settlement"]

# DICTIONARY FOR EVENTUAL FUNCTION
# to_replace_dict = {"гр.": "",
#                    "с.": "",
#                    "ь": "ъ",
#                    "ъо": "ьо",
#                    "добричка": "добрич",
#                    "добрич-селска": "добрич",
#                    "добрич-град": "добрич",
#                    "софийска": "софия",
#                    "столична": "софия"}

#replace wrong names to adjust for codes in NSI
for column in columns_to_transform:
    df[column] = df[column].str.lower()
    # df[column] = df[column].str.replace("гр.", "", regex=False)
    # df[column] = df[column].str.replace("с.", "", regex=False)
    df[column] = df[column].str.replace("ь", "ъ", regex=False)
    df[column] = df[column].str.replace("ъо", "ьо", regex=False)
    df[column] = df[column].str.replace("добричка", "добрич", regex=False)
    df[column] = df[column].str.replace("добрич-селска", "добрич", regex=False)
    df[column] = df[column].str.replace("добрич-град", "добрич", regex=False)
    df[column] = df[column].str.replace("софийска", "софия", regex=False)
    df[column] = df[column].str.replace("столична", "софия", regex=False)

df["settlement"] = df["settlement"].str.strip()

# TO DO SAVE DATAFRAME TO CSV WITH DATE AS FILENAME

###### GET EKATTE CODES ##############

ekatte_url = "http://www.nsi.bg/sites/default/files/files/EKATTE/Ekatte.zip"
content = requests.get(ekatte_url)
first_zip = ZipFile(BytesIO(content.content))

with first_zip as z:
    with z.open("Ekatte_xlsx.zip") as second_zip:
        z2_filedata = BytesIO(second_zip.read())
        with ZipFile(z2_filedata) as second_zip:
            df_ekatte = pd.read_excel(second_zip.open("Ek_atte.xlsx"), converters={"ekatte": str})
            df_ek_obl = pd.read_excel(second_zip.open("Ek_obl.xlsx"), converters={"ekatte": str})
            df_ek_obst = pd.read_excel(second_zip.open("Ek_obst.xlsx"), converters={"ekatte": str})


# EKATTE DATAFRAME
df_ekatte["name"] = df_ekatte["name"].str.lower()
df_ekatte = df_ekatte.iloc[1: , :]
columns_list_ek = ["kmetstvo", "kind", "category", "altitude", "document", "tsb", "abc"]
df_ekatte.drop(columns_list_ek,
               axis=1,
               inplace=True)
df_ekatte = df_ekatte.rename({"ekatte": "ekatte",
                              "name": "settlement",
                              "oblast": "region_code",
                              "obstina": "mun_code",
                              "t_v_m": "settlement_type"}, axis=1)

# REGION DATAFRAME
df_ek_obl["name"] = df_ek_obl["name"].str.lower()
columns_list_obl = ["document", "abc", "region", "ekatte"]
df_ek_obl.drop(columns_list_obl,
        axis=1,
        inplace=True)
df_ek_obl = df_ek_obl.rename({"oblast": "region_code",
                              "name": "region"}, axis=1)


# MUNICIPALITY DATAFRAME
df_ek_obst["name"] = df_ek_obst["name"].str.lower()
columns_list_obst = ["ekatte", "category", "document", "abc"]
df_ek_obst.drop(columns_list_obst,
        axis=1,
        inplace=True)
df_ek_obst = df_ek_obst.rename({"obstina": "mun_code",
                                "name": "municipality"}, axis=1)

# MERGED DATAFRAME WITH EKATTE CODES AND SETTLEMENT, REGION, MUNICIPALITY
df_ekatte = pd.merge(df_ekatte, df_ek_obl,
                     how="left")
df_ekatte = pd.merge(df_ekatte, df_ek_obst,
                     how="left")
df_ekatte['settlement'] = df_ekatte['settlement_type'] + df_ekatte['settlement']
df_ekatte.drop('settlement_type',
               axis=1,
               inplace=True)

df_ekatte = df_ekatte[["ekatte", "region", "municipality", "settlement", "region_code", "mun_code"]]

# HARD CODE DUPLICATE NAMES TO AVOID MISTAKES
df_ekatte.loc[df_ekatte["ekatte"] == "14461", "settlement"] = "с.бов (гара бов)"
df_ekatte.loc[df_ekatte["ekatte"] == "14489", "settlement"] = "с.орешец (гара орешец)"
df_ekatte.loc[df_ekatte["ekatte"] == "14475", "settlement"] = "с.лакатник(гара лакатник)"
df_ekatte.loc[df_ekatte["ekatte"] == "18490", "settlement"] = "с.елин пелин (гара елин п"

# replace wrong names to adjust for codes in NSI
for column in columns_to_transform:
    df_ekatte[column] = df_ekatte[column].str.replace("добрич-селска", "добрич", regex=False)
    df_ekatte[column] = df_ekatte[column].str.replace("софийска", "софия", regex=False)
    df_ekatte[column] = df_ekatte[column].str.replace("столична", "софия", regex=False)
    df_ekatte[column] = df_ekatte[column].str.replace("софия (столица)", "софия", regex=False)

# merge main df and ekatte to combine the matches
df_with_ekattes = pd.merge(df, df_ekatte, how="left")

# COMBINED COLUMNS IN CASE OF FUZZY MATCHING
# df["combined_column"] = df["region"] + "_" + df["municipality"] + "_" + df["settlement"]
# df_ekatte["combined_column"] = df_ekatte["region"] + "_" + df_ekatte["municipality"] + "_" + df_ekatte["settlement"]

# FUZZY MATCHING, ONLY NEEDED IN CASE OF HISTORICAL DATA MATCHING WHICH IS OBSOLETE SINCE CSVs ARE PRESENT

#left_on = ["ekatte", "combined_column"]

#right_on = ["ekatte", "combined_column"]

# Matching the previously 1:1 matched initial dataframe with the ekatte dataframe,
# and then fuzzy matching to ekatte one, again to fill gaps

# matched_results = fuzzymatcher.fuzzy_left_join(df4, df_ekatte, left_on, right_on)
# matched_results = matched_results.sort_values(by=["ekatte_right"])
#
# columns_list_to_drop = ["best_match_score", "__id_left", "__id_right", "combined_column_left",
#                         "ekatte_left", "region_code_left", "mun_code_left",
#                         "region_code_right", "mun_code_right", "combined_column_right"]
# matched_results.drop(columns_list_to_drop,
#                      axis=1,
#                      inplace=True)


columns_to_drop = ["region_code", "mun_code"]
df_with_ekattes.drop(columns_to_drop,
        axis=1,
        inplace=True)

#matched settlements with ekatte codes
ekatte_filename = f'ekatte_grao_{date_var}.csv'
ekatte_directory = 'ekatte_grao'
if not os.path.exists(ekatte_directory):
      os.makedirs(ekatte_directory)

df_with_ekattes.to_csv(f'./{ekatte_directory}/{ekatte_filename}', sep=",", encoding="utf-8", index=False)

# GET CODES FROM WIKIDATA

wikidata_url = "https://query.wikidata.org/sparql"
query = """
SELECT ?ekatte ?region ?municipality ?settlement
WHERE
{
?settlement wdt:P31/wdt:P279* wd:Q95993392 .
OPTIONAL { ?settlement wdt:P3990 ?ekatte. }
OPTIONAL { ?settlement wdt:P131 ?municipality. }
OPTIONAL { ?municipality wdt:P131 ?region. }
FILTER( strlen( ?ekatte ) < 6 ) .

SERVICE wikibase:label {
bd:serviceParam wikibase:language "en" .
}
}
ORDER BY ASC(?ekatte)
"""
r = requests.get(wikidata_url, params={"format": "json", "query": query})
wikidata = r.json()

grao_codes = []
for item in wikidata["results"]["bindings"]:
    grao_codes.append(OrderedDict({
        "ekatte": item["ekatte"]["value"],
        "region": item["region"]["value"],
        "municipality": item["municipality"]["value"],
        "settlement": item["settlement"]["value"]}))

df_wikidata = pd.DataFrame(grao_codes)
fix_cols_wiki = ["region", "municipality", "settlement"]
for cols in fix_cols_wiki:
    df_wikidata[cols] = df_wikidata[cols].apply(lambda x: x.split("/")[-1])

matched_data = df_with_ekattes.merge(df_wikidata, how="left", on="ekatte")
matched_data = matched_data[matched_data["region_y"] != "Q219"]

#remove unneeded columns
columns_to_drop = ["region_x", "municipality_x", "settlement_x"]
matched_data.drop(columns_to_drop,
        axis=1,
        inplace=True)

#remove last empty row that's left from the markdown
matched_data = matched_data.iloc[:-1 , :]

matched_data= matched_data.reindex(columns=["ekatte", "region_y", "municipality_y", "settlement_y",
                         "permanent_population", "current_population"])
matched_data = matched_data.rename({"region_y": "region",
                                          "municipality_y": "municipality",
                                          "settlement_y": 'settlement'}, axis=1)
matched_data = matched_data.head(11)

#match to Q codes and receive file that's ready to upload to WikiData
matched_filename = f'matched_data_{date_var}.csv'
matched_directory = 'matched_data'
if not os.path.exists(matched_directory):
      os.makedirs(matched_directory)

matched_data.to_csv(f'./{matched_directory}/{matched_filename}', sep=",", encoding="utf-8", index=False)


### ATTEMPT TO UPLOAD TO WIKIDATA

def login_with_credentials(credentials_path):
  credentials = pd.read_csv(credentials_path)
  username, password = tuple(credentials)

  return wdi_login.WDLogin(username, password)


def update_item(login, settlement_qid, population, permanent_population):
    ref = wdi_core.WDUrl(prop_nr="P854", value=url, is_reference=True)

    determination_method = wdi_core.WDItemID(value='Q90878165', prop_nr="P459", is_qualifier=True)
    determination_method2 = wdi_core.WDItemID(value='Q90878157', prop_nr="P459", is_qualifier=True)

    point_in_time = wdi_core.WDTime(time='+2021-12-15T00:00:00Z', prop_nr='P585', is_qualifier=True)

    qualifiers = []
    qualifiers.append(point_in_time)
    qualifiers.append(determination_method)

    qualifiers2 = []
    qualifiers2.append(point_in_time)
    qualifiers2.append(determination_method2)
    data = []

    prop = wdi_core.WDQuantity(prop_nr='P1082', value=population, qualifiers=qualifiers, references=[[ref]])
    data.append(prop)
    prop2 = wdi_core.WDQuantity(prop_nr='P1082',
                                value=permanent_population,
                                qualifiers=qualifiers2,
                                references=[[ref]])
    data.append(prop2)
    item = wdi_core.WDItemEngine(wd_item_id=settlement_qid, data=data)
    item.update(data, ["P1082"])
    item.write(login, bot_account=True)
    time.sleep(5)


data = pd.read_csv(f"{matched_directory}/{matched_filename}")

data.columns = ['ekatte', 'region', 'municipality', 'settlement', 'permanent_population', 'current_population']

login = login_with_credentials('data/credentials.csv')

error_logs = []
for _, row in data.iterrows():
    try:
        settlement_qid = row['settlement']
        population = row['current_population']
        permanent_population = row['permanent_population']
        update_item(login, settlement_qid, population, permanent_population)

    except:
        error_logs.append(settlement_qid)
        print("An error occured for item : " + settlement_qid)

print("Summarizing failures for specific IDs")
for error in error_logs:
    print("Error for : " + error)