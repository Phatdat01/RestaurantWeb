import pickle
import streamlit as st
import streamlit.components.v1 as components
from script.recommender import contend_based_recommendations, weighted_average_based_recommendations
from config import score_based_cfg, content_based_cfg, content_extra_based_cfg
from UI.widgets import initialize_res_widget, show_recommended_res_info
import constants as const
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("app") \
    .master("local") \
    .getOrCreate()

st.set_page_config(page_title="Recommender system", layout="wide")


# add styling
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# # load main movie dataframe
with open('data/res_df.pickle', 'rb') as handle:
    res = pickle.load(handle)

# result=spark.read.json('./data/ress_df.json')
# res = [row[0] for row in result.select('name').distinct().collect()]

st.markdown('# Restaurant Recommender system')
social_components = open("assets/social_components.html", 'r', encoding='utf-8')
components.html(social_components.read())

# add search panel and search button
main_layout, search_layout = st.columns([10, 1])
options = main_layout.multiselect('Which restaurant do you like?', res["name"].unique())
show_recommended_res_btn = search_layout.button("search")

# add widgets on sidebar
recommended_res_num = st.sidebar.slider("Recommended restaurant number", min_value=5, max_value=10)
if recommended_res_num:
    const.RES_NUMBER = recommended_res_num
show_score = st.sidebar.checkbox("Show score")

# create horizontal layouts for res
col_for_score_based = initialize_res_widget(score_based_cfg)
col_for_content_based = initialize_res_widget(content_based_cfg)
# col_for_content_based_extra = initialize_res_widget(content_extra_based_cfg)

# show recommended res based on weighted average (this is same for all res)
score_based_recommended_res = weighted_average_based_recommendations()
show_recommended_res_info(score_based_recommended_res, col_for_score_based, show_score)

# when search clicked
if show_recommended_res_btn:
    contend_based_recommended_res = contend_based_recommendations(res,options)
    show_recommended_res_info(contend_based_recommended_res, col_for_content_based, show_score)

    # contend_extra_based_recommended_res = contend_based_recommendations_extra(movie, options)
    # show_recommended_res_info(contend_extra_based_recommended_res, col_for_content_based_extra, show_score)
