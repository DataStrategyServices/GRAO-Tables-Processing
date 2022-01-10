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
import pywikibot
from markdown_to_df import ReadMarkdownTable
# TO-DO Better logging

logging.basicConfig(filename='ekatte.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('Need better logging')


#It works with both annual and quarterly, but annual is not really needed
#url = "https://www.grao.bg/tna/tadr2020.txt"

url = "https://www.grao.bg/tna/t41nm-15.12.2021_2.txt"

# MOST RECENT QUARTERLY REPORTS GRAO
#url = "https://www.grao.bg/tna/t41nm-15-03-2021_2.txt"
#url = "https://www.grao.bg/tna/t41nm-15-06-2021_2.txt"
#url = 'https://www.grao.bg/tna/t41nm-15-09-2021_2.txt'



# ''''''''''''''' Create markdown object '''''''''''''''''#
markdown_object = ReadMarkdownTable(url, '|')
# Extract date
date_object = markdown_object.extract_date()

# ''''''''''''''' Drop unused column '''''''''''''''''#
column_list = [0, 5, 6, 7, 9, 10, 11, 12]
markdown_object.drop_attributes(column_list)

# ''''''''''''''' Use meaningful labels '''''''''''''''''#
markdown_object.markdown_df.columns = ['region', 'municipality', 'settlement',
                                       'permanent_population', 'current_population']

# Cleaning initial data
atts_clean = ['region', 'municipality', 'settlement']
markdown_object.strip_columns(atts_clean)
markdown_object.lower_columns(atts_clean)
markdown_object.replace_letters(atts_clean, 'ь,ъ')
markdown_object.replace_letters(atts_clean, 'ъо,ьо')

markdown_object.change_labels('settlement', 'с.елин пелин (гара елин п', 'с.елин пелин')
markdown_object.change_labels('municipality', 'добричка', 'добрич-селска')
markdown_object.change_labels('municipality', 'добрич-град', 'добрич')

# ''''''''''''''' Remove neighborhoods, vacation areas, Vulchovci '''''''''''''''''#
clear_settlements = [
    "кв.",
    "ж.к.",
    "к.к.",
    "с.вълчовци"
]
markdown_object.clear_kv_zk('settlement', clear_settlements)

# ''''''''''''''' Clean Markdown DataFrame '''''''''''''''''#
df = markdown_object.markdown_df
print(df.shape)

###### GET EKATTE CODES ##############

# ekatte_url = "http://www.nsi.bg/sites/default/files/files/EKATTE/Ekatte.zip"
# content = requests.get(ekatte_url)
# first_zip = ZipFile(BytesIO(content.content))
#
# with first_zip as z:
#     with z.open("Ekatte_xlsx.zip") as second_zip:
#         z2_filedata = BytesIO(second_zip.read())
#         with ZipFile(z2_filedata) as second_zip:
#             df_ekatte = pd.read_excel(second_zip.open("Ek_atte.xlsx"), converters={"ekatte": str})
#             df_ek_obl = pd.read_excel(second_zip.open("Ek_obl.xlsx"), converters={"ekatte": str})
#             df_ek_obst = pd.read_excel(second_zip.open("Ek_obst.xlsx"), converters={"ekatte": str})
#
#
# # EKATTE DATAFRAME
# df_ekatte["name"] = df_ekatte["name"].str.lower()
# df_ekatte = df_ekatte.iloc[1: , :]
# columns_list_ek = ["kmetstvo", "kind", "category", "altitude", "document", "tsb", "abc"]
# df_ekatte.drop(columns_list_ek,
#                axis=1,
#                inplace=True)
# df_ekatte = df_ekatte.rename({"ekatte": "ekatte",
#                               "name": "settlement",
#                               "oblast": "region_code",
#                               "obstina": "mun_code",
#                               "t_v_m": "settlement_type"}, axis=1)
#
# # REGION DATAFRAME
# df_ek_obl["name"] = df_ek_obl["name"].str.lower()
# columns_list_obl = ["document", "abc", "region", "ekatte"]
# df_ek_obl.drop(columns_list_obl,
#         axis=1,
#         inplace=True)
# df_ek_obl = df_ek_obl.rename({"oblast": "region_code",
#                               "name": "region"}, axis=1)
#
#
# # MUNICIPALITY DATAFRAME
# df_ek_obst["name"] = df_ek_obst["name"].str.lower()
# columns_list_obst = ["ekatte", "category", "document", "abc"]
# df_ek_obst.drop(columns_list_obst,
#         axis=1,
#         inplace=True)
# df_ek_obst = df_ek_obst.rename({"obstina": "mun_code",
#                                 "name": "municipality"}, axis=1)
#
# # MERGED DATAFRAME WITH EKATTE CODES AND SETTLEMENT, REGION, MUNICIPALITY
# df_ekatte = pd.merge(df_ekatte, df_ek_obl,
#                      how="left")
# df_ekatte = pd.merge(df_ekatte, df_ek_obst,
#                      how="left")
# df_ekatte['settlement'] = df_ekatte['settlement_type'] + df_ekatte['settlement']
# df_ekatte.drop('settlement_type',
#                axis=1,
#                inplace=True)
#
# df_ekatte = df_ekatte[["ekatte", "region", "municipality", "settlement", "region_code", "mun_code"]]
#
# # HARD CODE DUPLICATE NAMES TO AVOID MISTAKES
# df_ekatte.loc[df_ekatte["ekatte"] == "14461", "settlement"] = "с.бов (гара бов)"
# df_ekatte.loc[df_ekatte["ekatte"] == "14489", "settlement"] = "с.орешец (гара орешец)"
# df_ekatte.loc[df_ekatte["ekatte"] == "14475", "settlement"] = "с.лакатник(гара лакатник)"
# df_ekatte.loc[df_ekatte["ekatte"] == "18490", "settlement"] = "с.елин пелин (гара елин п"
#
# # replace wrong names to adjust for codes in NSI
# for column in columns_to_transform:
#     df_ekatte[column] = df_ekatte[column].str.replace("добрич-селска", "добрич", regex=False)
#     df_ekatte[column] = df_ekatte[column].str.replace("софийска", "софия", regex=False)
#     df_ekatte[column] = df_ekatte[column].str.replace("столична", "софия", regex=False)
#     df_ekatte[column] = df_ekatte[column].str.replace("софия (столица)", "софия", regex=False)
#
# # merge main df and ekatte to combine the matches
# df_with_ekattes = pd.merge(df, df_ekatte, how="left")
#
#
# columns_to_drop = ["region_code", "mun_code"]
# df_with_ekattes.drop(columns_to_drop,
#         axis=1,
#         inplace=True)
#
#
# # GET CODES FROM WIKIDATA
#
# wikidata_url = "https://query.wikidata.org/sparql"
# query = """
# SELECT ?ekatte ?region ?municipality ?settlement
# WHERE
# {
# ?settlement wdt:P31/wdt:P279* wd:Q95993392 .
# OPTIONAL { ?settlement wdt:P3990 ?ekatte. }
# OPTIONAL { ?settlement wdt:P131 ?municipality. }
# OPTIONAL { ?municipality wdt:P131 ?region. }
# FILTER( strlen( ?ekatte ) < 6 ) .
#
# SERVICE wikibase:label {
# bd:serviceParam wikibase:language "en" .
# }
# }
# ORDER BY ASC(?ekatte)
# """
# r = requests.get(wikidata_url, params={"format": "json", "query": query})
# wikidata = r.json()
#
# grao_codes = []
# for item in wikidata["results"]["bindings"]:
#     grao_codes.append(OrderedDict({
#         "ekatte": item["ekatte"]["value"],
#         "region": item["region"]["value"],
#         "municipality": item["municipality"]["value"],
#         "settlement": item["settlement"]["value"]}))
#
# df_wikidata = pd.DataFrame(grao_codes)
# fix_cols_wiki = ["region", "municipality", "settlement"]
# for cols in fix_cols_wiki:
#     df_wikidata[cols] = df_wikidata[cols].apply(lambda x: x.split("/")[-1])
#
# matched_data = df_with_ekattes.merge(df_wikidata, how="left", on="ekatte")
# matched_data = matched_data[matched_data["region_y"] != "Q219"]
#
# #remove unneeded columns
# columns_to_drop = ["region_x", "municipality_x", "settlement_x"]
# matched_data.drop(columns_to_drop,
#         axis=1,
#         inplace=True)
#
# #remove last empty row that's left from the markdown
# matched_data = matched_data.iloc[:-1 , :]
#
# matched_data= matched_data.reindex(columns=["ekatte", "region_y", "municipality_y", "settlement_y",
#                          "permanent_population", "current_population"])
# matched_data = matched_data.rename({"region_y": "region",
#                                           "municipality_y": "municipality",
#                                           "settlement_y": 'settlement'}, axis=1)
# #matched_data = matched_data.head(1)
#
#
# # DATE OBJECT TO BE USED FOR THE POINT-IN-TIME PROPERTY IN WIKIDATA
# date_object = pd.to_datetime(date_var, format="%d.%m.%Y").date()
#
# # ATTEMPT TO UPLOAD TO WIKIDATA
# settlement_q_list = matched_data['settlement'].tolist()
# for settlement in settlement_q_list:
#     site = pywikibot.Site('wikipedia:en')
#     repo = site.data_repository()
#     item = pywikibot.ItemPage(repo, settlement)
#     data = item.get()
#     inhab_claims = data['claims'].get('P1082', [])
#     for claim in inhab_claims:
#         if claim.rank == "preferred":
#             claim.setRank('normal')
#             item.editEntity({"claims": [ claim.toJSON() ]}, summary='Change P1082 rank to normal')
#
# def login_with_credentials(credentials_path):
#   credentials = pd.read_csv(credentials_path)
#   username, password = tuple(credentials)
#
#   return wdi_login.WDLogin(username, password)
#
#
# def update_item(login, settlement_qid, population, permanent_population):
#     ref = wdi_core.WDUrl(prop_nr="P854", value=url, is_reference=True)
#
#     determination_method = wdi_core.WDItemID(value='Q90878165', prop_nr="P459", is_qualifier=True)
#     determination_method2 = wdi_core.WDItemID(value='Q90878157', prop_nr="P459", is_qualifier=True)
#     point_in_time = wdi_core.WDTime(time=f'+{date_object}T00:00:00Z', prop_nr='P585', is_qualifier=True)
#
#     qualifiers = []
#     qualifiers.append(point_in_time)
#     qualifiers.append(determination_method)
#     qualifiers2 = []
#     qualifiers2.append(point_in_time)
#     qualifiers2.append(determination_method2)
#     data = []
#
#     prop = wdi_core.WDQuantity(prop_nr='P1082',
#                                value=population,
#                                qualifiers=qualifiers,
#                                references=[[ref]],
#                                rank="normal")
#     data.append(prop)
#     prop2 = wdi_core.WDQuantity(prop_nr='P1082',
#                                 value=permanent_population,
#                                 qualifiers=qualifiers2,
#                                 references=[[ref]],
#                                 rank="normal")
#     data.append(prop2)
#     item = wdi_core.WDItemEngine(wd_item_id=settlement_qid, data=data)
#     item.update(data, ["P1082"])
#     item.write(login, bot_account=True)
#     time.sleep(5)
#
#
# data = matched_data
#
# data.columns = ['ekatte', 'region', 'municipality', 'settlement', 'permanent_population', 'current_population']
#
# login = login_with_credentials('data/credentials.csv')
#
# error_logs = []
# for _, row in data.iterrows():
#     try:
#         settlement_qid = row['settlement']
#         population = row['current_population']
#         permanent_population = row['permanent_population']
#         update_item(login, settlement_qid, population, permanent_population)
#     except:
#         error_logs.append(settlement_qid)
#         print("An error occured for item : " + settlement_qid)
#
# print("Summarizing failures for specific IDs")
# for error in error_logs:
#     print("Error for : " + error)


