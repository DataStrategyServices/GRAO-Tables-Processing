import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile


class EkatteDataframe:
    ekatte_url = "http://www.nsi.bg/sites/default/files/files/EKATTE/Ekatte.zip"

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def unzip_files(self):
        content = requests.get(self.ekatte_url)
        first_zip = ZipFile(BytesIO(content.content))
        with first_zip as z:
            with z.open("Ekatte_xlsx.zip") as second_zip:
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
    def drop_columns(df, column_list):
        return df.drop(column_list, axis=1, inplace=True)

    @staticmethod
    def rename_columns(df, column_dict):
        return df.rename(column_dict, axis=1, inplace=True)

    def transformations(self):
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

    def merge_frames(self):
        # MERGED DATAFRAME WITH EKATTE CODES AND SETTLEMENT, REGION, MUNICIPALITY
        df_ek, df_obl, df_obst = self.transformations()

        df_ek = pd.merge(df_ek, df_obl, how="left")
        df_ek = pd.merge(df_ek, df_obst, how="left")

        df_ek['settlement'] = df_ek['settlement_type'] + df_ek['settlement']

        columns_to_drop = ['settlement_type', 'region_code', 'mun_code']
        self.drop_columns(df_ek, columns_to_drop)

        df_ek = df_ek[["ekatte", "region", "municipality", "settlement"]]

        return df_ek

    @staticmethod
    def replace_in_columns(df, column_list):
        for column in column_list:
            df[column] = df[column].str.replace("добрич-селска", "добрич", regex=False)
            df[column] = df[column].str.replace("софийска", "софия", regex=False)
            df[column] = df[column].str.replace("столична", "софия", regex=False)
            df[column] = df[column].str.replace("софия (столица)", "софия", regex=False)
        return df

    @staticmethod
    def replace_by_ekatte(df):
        df.loc[df["ekatte"] == "14461", "settlement"] = "с.бов (гара бов)"
        df.loc[df["ekatte"] == "14489", "settlement"] = "с.орешец (гара орешец)"
        df.loc[df["ekatte"] == "14475", "settlement"] = "с.лакатник(гара лакатник)"
        df.loc[df["ekatte"] == "18490", "settlement"] = "с.елин пелин (гара елин п"
        return df

    def remove_ambigious_names(self):
        df_ekatte = self.merge_frames()

        # HARD CODE DUPLICATE NAMES TO AVOID MISTAKES
        df_ekatte = self.replace_by_ekatte(df_ekatte)

        # replace wrong names to adjust for codes in NSI
        columns_to_fix = ['region', 'municipality', 'settlement']
        self.replace_in_columns(df_ekatte, columns_to_fix)
        return df_ekatte

    def merge_with_main(self):
        # merge main df and ekatte to combine the matches
        df_ekatte = self.remove_ambigious_names()
        df_with_ekattes = pd.merge(self.dataframe, df_ekatte, how="left")
        return df_with_ekattes
