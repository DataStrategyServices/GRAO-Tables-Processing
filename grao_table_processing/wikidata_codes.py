import pandas as pd
import requests
from collections import OrderedDict


class WikidataCodes:
    """
    The class uses WikiData's SparQl to get
    all Q codes and respective Ekatte codes for settlements in Bulgaria.
    It turns those Q codes into a dataframe,
    which it then merges into the Dataframe with ekatte codes.
    It returns a dataframe whose values are ready to be uploaded to WikiData

    Attributes:
        wikidata_url: a Class attribute, a link that is used for the query
        query: the query that is used to extract the Q codes and Ekatte codes from Wikidata
        dataframe: a pandas DataFrame, which is merged with the Q codes, on the Ekatte Codes
    """

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

    def get_codes(self) -> list:
        """
        The method uses the requests library to extract a json file from WikiData's SparQl via a query.
        the json is iterated through and fills the ekatte, region, municipality and settlement
        from the wikidata json and returns an ordered dictionary with all necessary values.

        Return:
            q_codes: a list of OrderedDict containing the Q Codes for each settlement, its Municipality
            and Region, as well as its Ekatte code.
        """
        source = requests.get(self.wikidata_url, params={"format": "json", "query": self.query})
        wikidata = source.json()

        q_codes = []
        for item in wikidata["results"]["bindings"]:
            q_codes.append(OrderedDict({
                "ekatte": item["ekatte"]["value"],
                "region": item["region"]["value"],
                "municipality": item["municipality"]["value"],
                "settlement": item["settlement"]["value"]}))

        return q_codes

    def grao_with_frame(self) -> pd.DataFrame:
        """
        This method takes the q_codes list with OrderedDict inside and turns into a dataframe,
        with properly named columns.

        Return:
            df_wikidata - a pd DataFrame to be subject to transformations
        """
        q_codes = self.get_codes()
        df_wikidata = pd.DataFrame(q_codes)
        fix_cols_wiki = ["region", "municipality", "settlement"]
        for cols in fix_cols_wiki:
            df_wikidata[cols] = df_wikidata[cols].apply(lambda x: x.split("/")[-1])

        return df_wikidata

    @staticmethod
    def drop_columns(df: pd.DataFrame, column_list: list) -> pd.DataFrame:
        """
        A static method that takes the dataframe and drops the unneeded columns after merging.

        Args:
            df: pd.DataFrame
            column_list: a list of columns to be dropped
        Return:
            df: pd.DataFrame
        """
        return df.drop(column_list, axis=1, inplace=True)

    @staticmethod
    def reindex_df(df: pd.DataFrame, column_list: list) -> pd.DataFrame:
        """
        This method reindexes the dataframe according to a given column list.

        Args:
            df: pd.DataFrame
            column_list: a list of columns against which the dataframe will be reindexed
        Return:
            df: pd.DataFrame
        """
        return df.reindex(columns=column_list)

    @staticmethod
    def rename_columns(df: pd.DataFrame, column_dict: dict) -> pd.DataFrame:
        """
        This method reindexes the dataframe according to a given column list.

        Args:
            df: pd.DataFrame
            column_dict: a list of columns to be renamed, and their replacement names
        Return:
            df: pd.DataFrame
        """
        return df.rename(column_dict, axis=1)

    def merge_with_ekatte(self) -> pd.DataFrame:
        """
        The final method uses the now ready Q code dataframe and merges it into the DataFrame with
        settlement names and ekatte codes, this is the final step, and the resulting dataframe's values
        are ready to be loaded into WikiData. It calls the static methods, carries out
        the transformations and produces a finished dataframe.

        Return:
            matched_data: pd.DataFrame
        """
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
