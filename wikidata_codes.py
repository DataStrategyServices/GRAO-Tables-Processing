import pandas as pd
import requests
from collections import OrderedDict


class WikidataCodes:
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

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def get_codes(self):
        r = requests.get(self.wikidata_url, params={"format": "json", "query": self.query})
        wikidata = r.json()

        grao_codes = []
        for item in wikidata["results"]["bindings"]:
            grao_codes.append(OrderedDict({
                "ekatte": item["ekatte"]["value"],
                "region": item["region"]["value"],
                "municipality": item["municipality"]["value"],
                "settlement": item["settlement"]["value"]}))
        return grao_codes

    def grao_with_frame(self):
        grao_codes = self.get_codes()
        df_wikidata = pd.DataFrame(grao_codes)
        fix_cols_wiki = ["region", "municipality", "settlement"]
        for cols in fix_cols_wiki:
            df_wikidata[cols] = df_wikidata[cols].apply(lambda x: x.split("/")[-1])
        return df_wikidata

    @staticmethod
    def drop_columns(df, column_list):
        return df.drop(column_list, axis=1, inplace=True)

    @staticmethod
    def reindex_df(df, column_list):
        return df.reindex(columns=column_list)

    @staticmethod
    def rename_columns(df, column_dict):
        return df.rename(column_dict, axis=1)

    def merge_with_ekatte(self):
        df_wikidata = self.grao_with_frame()
        matched_data = self.dataframe.merge(df_wikidata, how="left", on="ekatte")

        # remove Q that only relates to Bulgaria and nothing else as it serves no purpose
        matched_data = matched_data[matched_data["region_y"] != "Q219"]

        # remove unneeded columns
        columns_to_drop = ["region_x", "municipality_x", "settlement_x"]
        self.drop_columns(matched_data, columns_to_drop)

        # remove last empty row that's left from the markdown
        matched_data = matched_data.iloc[:-1, :]

        # reindex the columns to be arranged in a more useful manner
        columns_to_reindex = ["ekatte", "region_y",
                              "municipality_y", "settlement_y",
                              "permanent_population", "current_population"]
        matched_data = self.reindex_df(matched_data, columns_to_reindex)

        # rename columns
        columns_to_rename = {"region_y": "region",
                             "municipality_y": "municipality",
                             "settlement_y": 'settlement'}
        matched_data = self.rename_columns(matched_data, columns_to_rename)

        # reset index to avoid confusion
        matched_data.reset_index(drop=True, inplace=True)

        return matched_data
