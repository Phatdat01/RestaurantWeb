import streamlit as st
from pandas import DataFrame as PandasDataFrame

from app.data.dataframe import DataSharing

class DashboardPage:
    def __init__(self):
        self._data_sharing = DataSharing()

    @st.cache
    def _get_data(self, layer: str, schema:str, table:str) -> PandasDataFrame:
        '''
        Get data from delta-sharing.share

        :param layer: layer of share
        :param schema: schema of table
        :param table: table name

        :return: data from table

        '''

        data = self._data_sharing.read(layer, schema, table)

        return data

    def show(self, df):
        self._show_dashboard_page()
        self.show_restaurant_category_chart(df)
        self.show_restaurant_city_chart(df)

    def _show_dashboard_page(self):
        st.title('Dashboard page')
        st.write('This is the dashboard page')


    def show_restaurant_category_chart(self, df):
        st.title('Restaurant Category')
        st.write('The following chart shows the number of restaurants in each category')
        st.bar_chart(df['categories'].value_counts())


    def show_restaurant_city_chart(self, df):
        st.title('Restaurant City')
        st.write('The following chart shows the number of restaurants in each city')
        st.map(df[['latitude', 'longitude']])


    #   main
    def main(self):

        df = self._get_data('bronze-layer', 'bronze', 'business')

        self.show(df)


if __name__ == '__main__':
    dashboard = DashboardPage()
    dashboard.main()

    