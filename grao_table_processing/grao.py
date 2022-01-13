import logging

import pandas as pd
import datetime

from grao_table_processing.markdown_to_df import ReadMarkdownTable
from grao_table_processing.acquire_url import DataURL
from grao_table_processing.ekatte_dataframe import EkatteDataframe
from grao_table_processing.wikidata_codes import WikidataCodes
from grao_table_processing import wikidata_uploader

# TO-DO Better logging

logging.basicConfig(filename='ekatte.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logging.warning('Need better logging')


def generate_url(url_object) -> str:
    return DataURL.generate_data_url(url_object)


# ''''''''''''''' Create markdown object '''''''''''''''''
def extract_date(url: str) -> datetime.date:
    markdown_object = ReadMarkdownTable(url, '|')
    date_object = markdown_object.extract_date()
    return date_object


def transformations(url: str) -> pd.DataFrame:
    markdown_object = ReadMarkdownTable(url, '|')

# ''''''''''''''' Drop unused column '''''''''''''''''
    column_list = [0, 5, 6, 7, 9, 10, 11, 12]
    markdown_object.drop_attributes(column_list)

# ''''''''''''''' Use meaningful labels '''''''''''''''''
    markdown_object.markdown_df.columns = ['region', 'municipality', 'settlement',
                                           'permanent_population', 'current_population']

# '''''''''''Cleaning initial data'''''''''''''''''
    atts_clean = ['region', 'municipality', 'settlement']
    markdown_object.strip_columns(atts_clean)
    markdown_object.lower_columns(atts_clean)
    markdown_object.replace_letters(atts_clean, 'ь,ъ')
    markdown_object.replace_letters(atts_clean, 'ъо,ьо')

    markdown_object.change_labels('settlement', 'с.елин пелин (гара елин п', 'с.елин пелин')
    markdown_object.change_labels('municipality', 'добричка', 'добрич-селска')
    markdown_object.change_labels('municipality', 'добрич-град', 'добрич')

# ''''''''''''''' Remove neighborhoods, vacation areas, Vulchovci '''''''''''''''''
    clear_settlements = [
        "кв.",
        "ж.к.",
        "к.к.",
        "с.вълчовци"
    ]
    markdown_object.clear_kv_zk('settlement', clear_settlements)

# ''''''''''''''' Clean Markdown DataFrame '''''''''''''''''
    return markdown_object.markdown_df


def merge_with_ekatte(dataframe: pd.DataFrame) -> pd.DataFrame:
    ekatte_frame = EkatteDataframe(dataframe)
    return EkatteDataframe.merge_with_main(ekatte_frame)


def merge_with_q_codes(dataframe: pd.DataFrame) -> pd.DataFrame:
    wikidata_codes = WikidataCodes(dataframe)
    matched_data = WikidataCodes.merge_with_ekatte(wikidata_codes)
    return matched_data


#
def upload_data(matched_data, url, date):
    wikidata_uploader.upload_to_wikidata(matched_data, url, date)
#


def update_date_file(url_object) -> None:
    DataURL.update_date_file(url_object)


def main():
    url_object = DataURL()
    url = generate_url(url_object)
    print(url)
    date = extract_date(url)
    print(date)
    dataframe = transformations(url)
    print(dataframe.head())
    ekatte_frame = merge_with_ekatte(dataframe)
    print(ekatte_frame.head())
    matched_data = merge_with_q_codes(ekatte_frame)
    print(matched_data.head())
    # upload_data(matched_data, url, date)
    update_date_file(url_object)


if __name__ == "__main__":
    main()
