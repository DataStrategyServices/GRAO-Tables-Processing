import os

#import fuzzymatcher
import time

import pandas as pd
import requests
from collections import OrderedDict

from wikidataintegrator import wdi_core, wdi_login
import pywikibot


current_year = 1998
while current_year < 2020:
    df_with_ekattes = pd.read_csv(
        f'https://raw.githubusercontent.com/data-for-good-bg/GRAO-tables-processing/master/grao_data/grao_data_{current_year}.csv',
        dtype={'ekatte': object})
    df_with_ekattes = df_with_ekattes.rename({f"permanent_{current_year}": "permanent_population",
                                              f"current_{current_year}": "current_population"}, axis=1)\
        .sort_values(by="ekatte")
    date_var = f'15.12.{current_year}'
    url = f'https://www.grao.bg/tna/tadr-{current_year}.txt'
    current_year += 1

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
    matched_data.dropna()

    #match to Q codes and receive file that's ready to upload to WikiData
    # matched_filename = f'matched_data_{date_var}.csv'
    # matched_directory = 'matched_data'
    # if not os.path.exists(matched_directory):
    #       os.makedirs(matched_directory)
    #
    # matched_data.to_csv(f'./{matched_directory}/{matched_filename}', sep=",", encoding="utf-8", index=False)


    # DATE OBJECT TO BE USED FOR THE POINT-IN-TIME PROPERTY IN WIKIDATA
    date_object = pd.to_datetime(date_var, format="%d.%m.%Y").date()

# ATTEMPT TO UPLOAD TO WIKIDATA
    settlement_q_list = matched_data['settlement'].tolist()
    for settlement in settlement_q_list:
        site = pywikibot.Site('wikipedia:en')
        repo = site.data_repository()
        item = pywikibot.ItemPage(repo, settlement)
        data = item.get()
        inhab_claims = data['claims'].get('P1082', [])
        for claim in inhab_claims:
            if claim.rank == "preferred":
                claim.setRank('normal')
                item.editEntity({"claims": [ claim.toJSON() ]}, summary='Change P1082 rank to normal')

    def login_with_credentials(credentials_path):
      credentials = pd.read_csv(credentials_path)
      username, password = tuple(credentials)

      return wdi_login.WDLogin(username, password)


    def update_item(login, settlement_qid, population, permanent_population):
        ref = wdi_core.WDUrl(prop_nr="P854", value=url, is_reference=True)

        determination_method = wdi_core.WDItemID(value='Q90878165', prop_nr="P459", is_qualifier=True)
        determination_method2 = wdi_core.WDItemID(value='Q90878157', prop_nr="P459", is_qualifier=True)
        point_in_time = wdi_core.WDTime(time=f'+{date_object}T00:00:00Z', prop_nr='P585', is_qualifier=True)

        qualifiers = []
        qualifiers.append(point_in_time)
        qualifiers.append(determination_method)
        qualifiers2 = []
        qualifiers2.append(point_in_time)
        qualifiers2.append(determination_method2)
        data = []

        prop = wdi_core.WDQuantity(prop_nr='P1082',
                                   value=population,
                                   qualifiers=qualifiers,
                                   references=[[ref]],
                                   rank="preferred")
        data.append(prop)
        prop2 = wdi_core.WDQuantity(prop_nr='P1082',
                                    value=permanent_population,
                                    qualifiers=qualifiers2,
                                    references=[[ref]],
                                    rank="preferred")
        data.append(prop2)
        item = wdi_core.WDItemEngine(wd_item_id=settlement_qid, data=data)
        item.update(data, ["P1082"])
        item.write(login, bot_account=True)
        time.sleep(1)


    data = matched_data

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
            error_logs.append(f'{settlement_qid},{date_object}')
            print("An error occured for item : " + settlement_qid)

    print("Summarizing failures for specific IDs")
    for error in error_logs:
        print("Error for : " + error)

df_errors = pd.DataFrame(error_logs)
df_errors.to_csv("error_log.csv")