import os
from wikidataintegrator import wdi_core, wdi_login
import pywikibot
import pandas as pd
import datetime


def set_old_ranks_to_normal(matched_data: pd.DataFrame) -> None:
    """
        The function accepts a dataframe of matched Ekatte codes to Q codes from Wikidata,
        it checks each settlement's wikidata page and
        resets its population property values to "Normal"
        rank. This allows to set the newest value to "Preferred" when uploading.

        Args:
            matched_data: a pandas DataFrame

        Return:
            None
    """
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


def login_with_credentials(username: str, password: str) -> wdi_login.WDLogin:
    """
        The function accepts a username and password
        from environment variables, or from any other possible way of providing credentials
        (ABSOLUTELY AVOID UPLOADING HARDCODED CREDENTIALS OR AS A FILE TO ANY CODE-SHARING WEBSITE).
        It then uses the credentials to log the WikiData Integrator library into the website,
        which thus allows the bot to carry out its value updates.

        Args:
            username: string, the username to log in with
            password: string, the password to log in with

        Return:
            wdi_login.WDLogin: a WikiDataIntegrator class that handles logging into WikiData
    """
    return wdi_login.WDLogin(username, password)


def update_item(login: wdi_login.WDLogin, settlement_qid: str,
                population: str, permanent_population:str, date_object: datetime.date, url: str) -> None:
    """
        The function receives its parameters and then updates the population properties
        on each settlement's WikiData page. It doesn't overwrite data, but rather appends
        each successive year, annually from 1998 onwards.
        Past 2020, the reporting becomes Quarterly.
        The integrator uses the settlement's unique Q code and specific property
        codes to set the necessary values.

        Args:
            login: wdi_login.WDLogin, login credentials for the bot
            settlement_qid: string, the unique code of each settlement
            population: string, the current population of the settlement
            permanent_population: string, the permanent population of the settlement
            date_object: datetime, the date used in the "point of time" property
            url: the source of the changes, in the "Reference" property
        Return:
            None
    """
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
        This function essentially "wraps" the update_item function,
        it provides a for cycle in which each settlement is updated.y
        It also calls the set_old_ranks_to_normal function to reset the ranks,
        the resetting happens before upload of the most recent data.
        Iterrows() is slow, but used because both map and apply
        pandas methods could not fulfill the requirements to upload
        each row's values as needed.
        Args:
            matched_data: a pandas DataFrame
            url: string, the link used for the "Reference" property
            date: datetime, the date used for the "point_in_time" property
        Return:
            None
    """
    username = os.environ['usernamewiki']
    password = os.environ['pwwiki']
    login = login_with_credentials(username, password)

    set_old_ranks_to_normal(matched_data)

    matched_data.columns = ['ekatte', 'region', 'municipality', 'settlement', 'permanent_population', 'current_population']

    error_logs = []
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


