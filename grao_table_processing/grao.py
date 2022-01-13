import logging


from grao_table_processing.markdown_to_df import ReadMarkdownTable
from grao_table_processing.acquire_url import DataURL
from grao_table_processing.ekatte_dataframe import EkatteDataframe
from grao_table_processing.wikidata_codes import WikidataCodes
from grao_table_processing import wikidata_uploader

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

#wikidata_uploader.upload_to_wikidata(matched_data, url, date_object)

# # UPDATE DATA FILE AT THE VERY END, IN CASE UPLOAD FAILS
#DataURL.update_date_file(url_object)

