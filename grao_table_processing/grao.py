import logging
import pandas as pd
import datetime

from grao_table_processing.markdown_to_df import ReadMarkdownTable
from grao_table_processing.acquire_url import DataURL
from grao_table_processing.ekatte_dataframe import EkatteDataframe
from grao_table_processing.wikidata_codes import WikidataCodes
from grao_table_processing.wikidata_uploader import upload_to_wikidata, set_old_ranks_to_normal


logging.basicConfig(filename='logs/ekatte.log', filemode='a+', level=logging.CRITICAL,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')

"""
This is the project's main file, it has a function for each of the main classes, as well
as the wikidata uploader file. The imports are above, as well as split into each file.
There is a simple logger to track any critical errors.
"""


def generate_url(url_object: DataURL()) -> str:
    """
    Calls the DataURL class and provides a URL to extract data from

    Args:
        url_object: DataURL() class object
    Return:
        DataURL.generate_data_url(url_object): string
    """
    return DataURL.generate_data_url(url_object)


def extract_date(url: str) -> datetime.date:
    """
    By using the result of the generate URL function, extracts the date to be used
    in the data loading.

    Args:
        url: a string URL that leads to a markdown table for date extraction
    Return:
        date_object: datetime.date object
    """
    markdown_object = ReadMarkdownTable(url, '|')
    date_object = markdown_object.extract_date()
    return date_object


def transformations(url: str) -> pd.DataFrame:
    """
    This function invokes the ReadMarkdownTable class, and does all necessary
    transformations so that it is ready to be merged forward.

    Args:
        url: a string URL that leads to a markdown table for data extraction
    Return:
        markdown_object.markdown_df: pd.DataFrame
    """
    markdown_object = ReadMarkdownTable(url, '|')

# ''''''''''''''' Drop unused columns '''''''''''''''''
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
    markdown_object.change_labels('municipality', 'добрич-селска', 'добрич')
    markdown_object.change_labels('region', 'добрич-град', 'добрич')
    markdown_object.change_labels('region', 'добрич-селска', 'добрич')
    markdown_object.change_labels('region', "софийска", 'софия')
    markdown_object.change_labels('region', "столична", 'софия')
    markdown_object.change_labels('municipality', "софийска", 'софия')
    markdown_object.change_labels('municipality', "столична", 'софия')
    markdown_object.change_labels('municipality', "софия (столица)", 'софия')
    markdown_object.change_labels('region', "софия (столица)", 'софия')

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
    """
    The function invokes the EkatteDataframe class, produces the ekatte_dataframe
    and merges it with the GRAO markdown table.

    Args:
        dataframe: pd.DataFrame
    Return:
        EkatteDataframe.merge_with_main(ekatte_frame): a pd.DataFrame
    """
    ekatte_frame = EkatteDataframe(dataframe)
    return EkatteDataframe.remove_ambigious_names(ekatte_frame)


def merge_with_q_codes(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    The function invokes the WikidataCodes class, produces the Q code dataframe
    and merges it with the GRAO Dataframe with Ekatte codes.

    Args:
        dataframe: pd.DataFrame
    Return:
        matched_data: pd.DataFrame, ready to have its values uploaded to WikiData
        """
    wikidata_codes = WikidataCodes(dataframe)
    matched_data = WikidataCodes.merge_with_ekatte(wikidata_codes)
    return matched_data


def reset_ranks_to_normal(dataframe: pd.DataFrame) -> None:
    """
    This function calls the Set old ranks to normal function from wikidata uploader,
    it goes through each settlement resetting its historic values to "Normal Rank"
    subsequently, the upload sets the most recent population values to "Preferred Rank"
    and they appear on Wikipedia. This repeats each subsequent time data is uploaded.

    Args:
        dataframe: pandas DataFrame
    Return:
        None
    """
    set_old_ranks_to_normal(dataframe)


def upload_data(matched_data: pd.DataFrame, url: str, date: datetime.date) -> None:
    """
    This function invokes the Upload_to_wikidata function from the wikidata_uploader module.
    it carries out the functions by clearing out the ranks, uploading the data quarter by quarter
    for all settlements.

    Args:
        matched_data: pd.DataFrame
        url: string
        date: datetime.date
    Return:
        None
    """
    upload_to_wikidata(matched_data, url, date)


def update_date_file(url_object: DataURL()) -> None:
    """
    If the upload concludes successfully, this function is invoked. It finally updates
    the date helper file with the most recent date, so that the future uploads happen on schedule.
    The URL is added to the processed reports log for archiving.

    Args:
        url_object: DataURL() class object
    Return:
        None
    """
    DataURL.update_date_file(url_object)


def main():
    """
    The main function sets the parameters for every function in the grao.py module.
    It carries out the entire logic of the project from data extraction to loading.

    Return:
        None
    """
    # url_object = DataURL()
    # url = generate_url(url_object)
    url = "https://www.grao.bg/tna/tadr2020.txt"
    print(url)
    date = extract_date(url)
    print(date)
    dataframe = transformations(url)
    print(dataframe.shape)
    ekatte_frame = merge_with_ekatte(dataframe)
    ekatte_frame.to_csv('ekatte.csv', sep=',', encoding='UTF-8')
    print(ekatte_frame.shape)
    matched_data = merge_with_q_codes(ekatte_frame)
    matched_data.to_csv('matched.csv', sep=',', encoding='UTF-8')
    print(matched_data.shape)
    #reset_ranks_to_normal(matched_data)
    #upload_data(matched_data, url, date)
    #update_date_file(url_object)


if __name__ == "__main__":
    main()
