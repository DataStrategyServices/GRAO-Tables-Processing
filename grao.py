import logging
import urllib.request
import os
import time
import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile
from collections import OrderedDict
from wikidataintegrator import wdi_core, wdi_login
import pywikibot

from markdown_to_df import ReadMarkdownTable
from acquire_url import DataURL
from ekatte_dataframe import EkatteDataframe
from wikidata_codes import WikidataCodes
from wikidata_uploader import WikidataUploader

# TO-DO Better logging

logging.basicConfig(filename='ekatte.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('Need better logging')

url_object = DataURL()
url = DataURL.generate_data_url(url_object)
print(url)

''''''''''''''' Create markdown object '''''''''''''''''#
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

ekatte_object = EkatteDataframe(df)
df_with_ekattes = EkatteDataframe.merge_with_main(ekatte_object)
print(df_with_ekattes.shape)

wikidata_codes = WikidataCodes(df_with_ekattes)
matched_data = WikidataCodes.merge_with_ekatte(wikidata_codes)
print(matched_data.shape)

to_upload = WikidataUploader(matched_data, date_object, url)
WikidataUploader.rank_to_normal(to_upload)
WikidataUploader.update_item(to_upload)

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
#                                rank="preferred")
#     data.append(prop)
#     prop2 = wdi_core.WDQuantity(prop_nr='P1082',
#                                 value=permanent_population,
#                                 qualifiers=qualifiers2,
#                                 references=[[ref]],
#                                 rank="preferred")
#     data.append(prop2)
#     item = wdi_core.WDItemEngine(wd_item_id=settlement_qid, data=data)
#     item.update(data, ["P1082"])
#     item.write(login, bot_account=True)
#     time.sleep(1)
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

# # UPDATE DATA FILE AT THE VERY END, IN CASE UPLOAD FAILS
DataURL.update_date_file(url_object)

