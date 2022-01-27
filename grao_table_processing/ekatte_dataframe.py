import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile


class EkatteDataframe:
    """
        The class receives a dataframe and uses three excel files downloaded in memory
        from the NSI website with unique Ekatte codes to merge into it.
        The resulting dataframe is then ready to match with WikiData's unique Q codes
        and the settlement to be uploaded to the WikiData.
        All unneeded columns are cleared, as well as unneeded data

        Attributes:
            ekatte_url: a Class attribute, that is a permalink from the NSI website
            dataframe: a pandas DataFrame that is merged with the Ekatte code files
        Return:
            pandas DataFrame

    """

    ekatte_url = "http://www.nsi.bg/sites/default/files/files/EKATTE/Ekatte.zip"

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def unzip_files(self) -> tuple[pd.DataFrame, ...]:
        """
            Uses the ekatte_url class attribute to unzip the initial
            file that contains another zip, which then contains the
            three required Excel files. All of this happens in memory,
            there is no writing on files.
            Notably, the initial link file remains Ekatte.zip, while the inside zip varies,
            as a result logic was necessary to avoid depending on the filename at all.
            The logic scans the inside of the file, finds the necessary zip
            then extracts what it needs.

            return:
                df_ekatte, df_ek_obl, df_ek_obst a tuple of three Pandas dataframes
        """
        content = requests.get(self.ekatte_url)
        first_zip = ZipFile(BytesIO(content.content))
        with first_zip as z:
            files = first_zip.namelist()
            for file in files:
                if file.endswith('.zip'):
                    with z.open(file) as second_zip:
                        z2_filedata = BytesIO(second_zip.read())
                        with ZipFile(z2_filedata) as third_zip:
                            df_ekatte = pd.read_excel(third_zip.open("Ek_atte.xlsx"),
                                                      converters={"ekatte": str})
                            df_ek_obl = pd.read_excel(third_zip.open("Ek_obl.xlsx"),
                                                      converters={"ekatte": str})
                            df_ek_obst = pd.read_excel(third_zip.open("Ek_obst.xlsx"),
                                                       converters={"ekatte": str})
        return df_ekatte, df_ek_obl, df_ek_obst

    @staticmethod
    def drop_columns(df: pd.DataFrame, column_list: list) -> pd.DataFrame:
        """
        A static method that receives a dataframe and a column list
        it drops the columns from the dataframe, as they are unneeded.

        Args:
            df: a pandas DataFrame
            column_list: a list of columns in the pandas DataFrame that are to be dropped
        return
            pd.DataFrame: a dataframe with some columns removed
        """
        return df.drop(column_list, axis=1, inplace=True)

    @staticmethod
    def rename_columns(df: pd.DataFrame, column_dict: dict) -> pd.DataFrame:
        """
        A static method that receives a dataframe and a column dict
        the key and value pairs are used to rename the colums in the frame

        Args:
            df: a pandas DataFrame
            column_dict: a list of column names and their replacements
        return
            pd.DataFrame: a dataframe with some (or all) columns renamed
        """
        return df.rename(column_dict, axis=1, inplace=True)

    def transformations(self) -> tuple[pd.DataFrame, ...]:
        """
        A function that carries out various transformations on the Ekatte dataframes,
        initially the unzip_files function is called to produce the three needed dataframes.
        Afterwards, the class methods above are called for each dataframe.
        The result is a tuple of three dataframes that are cleared and ready to merge.

        return:
            df_ek, df_obl, df_obst: a tuple of three dataframes ready to merge
        """

        df_ek, df_obl, df_obst = self.unzip_files()

        # EKATTE DATAFRAME
        df_ek["name"] = df_ek["name"].str.lower()
        df_ek = df_ek.iloc[1:, :]

        # drop unneeded columns
        columns_list_ek = ["kmetstvo", "kind", "category", "altitude", "document", "tsb", "abc"]
        self.drop_columns(df_ek, columns_list_ek)

        # rename columns
        column_dict = {"ekatte": "ekatte", "name": "settlement",
                       "oblast": "region_code",
                       "obstina": "mun_code",
                       "t_v_m": "settlement_type"}
        self.rename_columns(df_ek, column_dict)

        # REGION DATAFRAME
        df_obl["name"] = df_obl["name"].str.lower()

        # drop unneeded columns
        columns_list_obl = ["document", "abc", "region", "ekatte"]
        self.drop_columns(df_obl, columns_list_obl)

        # rename columns
        column_dict = {"oblast": "region_code", "name": "region"}
        self.rename_columns(df_obl, column_dict)

        # MUNICIPALITY DATAFRAME
        df_obst["name"] = df_obst["name"].str.lower()

        # drop unneeded columns
        columns_list_obst = ["ekatte", "category", "document", "abc"]
        self.drop_columns(df_obst, columns_list_obst)

        # rename columns
        column_dict = {"obstina": "mun_code", "name": "municipality"}
        self.rename_columns(df_obst, column_dict)

        return df_ek, df_obl, df_obst

    def merge_frames(self) -> pd.DataFrame:
        """
        Begins by calling the transformation method and then merges the three input dataframes.
        They've already been prepared in the previous operations. The resulting dataframe is
        ready to be merged into the GRAO dataframe. The 'settlement' and 'settlement_type'
        columns are combined to use the village and city indication to disambiguate
        similar or duplicate values

        return:
            df_ek: a pandas DataFrame ready to be merged further
        """

        df_ek, df_obl, df_obst = self.transformations()

        df_ek = pd.merge(df_ek, df_obl, how="left")
        df_ek = pd.merge(df_ek, df_obst, how="left")

        df_ek['settlement'] = df_ek['settlement_type'] + df_ek['settlement']

        columns_to_drop = ['settlement_type', 'region_code', 'mun_code']
        self.drop_columns(df_ek, columns_to_drop)

        df_ek = df_ek[["ekatte", "region", "municipality", "settlement"]]

        return df_ek

    @staticmethod
    def replace_in_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        This static method takes the df_ek and corrects some values in order to make it
        possible to properly merge into the GRAO dataframe. It simply replaces some
        values that are hard to disambiguate with such that are easier.

        Args:
            df: a pandas DataFrame
        return:
            df: a pandas DataFrame with corrected values
        """

        df.loc[df['region'] == "добрич-селска", 'region'] = "добрич"
        df.loc[df['region'] == "софийска", 'region'] = "софия"
        df.loc[df['region'] == "столична", 'region'] = "софия"
        df.loc[df['region'] == "софия (столица)", 'region'] = "софия"
        df.loc[df['municipality'] == "добрич-селска", 'municipality'] = "добрич"
        df.loc[df['municipality'] == "софийска", 'municipality'] = "софия"
        df.loc[df['municipality'] == "столична", 'municipality'] = "софия"
        df.loc[df['municipality'] == "софия (столица)", 'municipality'] = "софия"
        return df

    @staticmethod
    def replace_by_ekatte(df: pd.DataFrame) -> pd.DataFrame:
        """
        This static method takes the dataframe and hardcodes several values that
        cannot be disambiguated otherwise. Nothing is lost, rather it is gained.

        Args:
            df: a pandas DataFrame
        return:
            df: a pandas DataFrame
        """
        df.loc[df["ekatte"] == "14461", "settlement"] = "с.бов (гара бов)"
        df.loc[df["ekatte"] == "14489", "settlement"] = "с.орешец (гара орешец)"
        df.loc[df["ekatte"] == "14475", "settlement"] = "с.лакатник(гара лакатник)"
        df.loc[df["ekatte"] == "18490", "settlement"] = "с.елин пелин (гара елин п"
        return df

    def remove_ambigious_names(self) -> pd.DataFrame:
        """
        This method takes both static methods above and uses them to disambiguate
        values in the Ekatte dataframe. The result is completely ready to be merged
        with the GRAO dataframe.

        return:
            df_ekatte: a pandas DataFrame
        """
        df_ekatte = self.merge_frames()
        df_ekatte.to_csv('ekatte654645.csv', sep=',', encoding='UTF-8')
        df1 = self.replace_in_columns(df_ekatte)
        df = self.replace_by_ekatte(df1)

        df_with_ekattes = pd.merge(self.dataframe, df, how="left")
        df_with_ekattes.to_csv('ekatte123.csv', sep=',', encoding='UTF-8')
        return df_with_ekattes


