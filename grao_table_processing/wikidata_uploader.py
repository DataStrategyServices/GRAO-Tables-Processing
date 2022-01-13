import os
from wikidataintegrator import wdi_core, wdi_login
import pywikibot
import pandas as pd
import datetime


# Rank back to normal
def set_old_ranks_to_normal(matched_data: pd.DataFrame) -> None:
    # settlement_q_list = matched_data['settlement'].tolist()


    for settlement in matched_data['settlement'].tolist():
        site = pywikibot.Site('wikipedia:en')
        repo = site.data_repository()
        item = pywikibot.ItemPage(repo, settlement)
        data = item.get()
        inhab_claims = data['claims'].get('P1082', [])
        for claim in inhab_claims:
            if claim.rank == "preferred":
                claim.setRank('normal')
                item.editEntity({"claims": [ claim.toJSON() ]}, summary='Change P1082 rank to normal')


# credentials for the WD Integrator login
def login_with_credentials() -> wdi_login.WDLogin:
    username = os.environ['usernamewiki']
    password = os.environ['pwwiki']
    return wdi_login.WDLogin(username, password)


# Upload Data per quarter
def update_item(login: wdi_login.WDLogin, settlement_qid: str,
                population: str, permanent_population:str, date_object: datetime.date, url: str) -> None:
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


def upload_to_wikidata(matched_data: pd.DataFrame, url: str, date: datetime.date) -> None:
    """
    TODO: docstring
    """
    login = login_with_credentials()

    set_old_ranks_to_normal(matched_data)

    matched_data.columns = ['ekatte', 'region', 'municipality', 'settlement', 'permanent_population', 'current_population']

    error_logs = []
    # TODO: avoid .iterrows() unless ABSOLUTELY necessary, prefer apply, map, etc. pandas data frame
    # methods
    for _, row in matched_data.iterrows():
        try:
            settlement_qid = row['settlement']
            population = row['current_population']
            permanent_population = row['permanent_population']
            update_item(login, settlement_qid, population, permanent_population, date, url)
        except:
            error_logs.append(settlement_qid)
            print("An error occured for item : " + settlement_qid)

    print("Summarizing failures for specific IDs")
    for error in error_logs:
        print("Error for : " + error)


