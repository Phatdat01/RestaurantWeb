import delta_sharing
import streamlit as st
from pandas import DataFrame as PandasDataFrame

class DataSharing:

    profile_file: str = "/home/nguynmanh/works/recommender/RestaurantWeb/app/secret/delta-sharing.share"

    def __init__(self):
        self.client = delta_sharing.SharingClient(profile=self.profile_file)

    def list_tables(self) -> list:
        '''
        Get list of tables in delta-sharing.share
        '''
        list_tables = self.client.list_all_tables()

        return list_tables

    def read(self, layer: str, schema:str, table:str) -> PandasDataFrame:
        '''
        Get data from delta-sharing.share

        :param layer: layer of share
        :param schema: schema of table
        :param table: table name

        :return: data from table

        '''

        table_url = f"{self.profile_file}#{layer}.{schema}.{table}"

        data = delta_sharing.load_as_pandas(table_url)

        return data

