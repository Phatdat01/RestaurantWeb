import streamlit as st
import constants as const
from script.utils import fetch_poster
from PIL import Image

def initialize_res_widget(cfg):
    """here we create empty blanks for all recommended restaurants
    and add description and title from appropriate config file"""

    res_cols = st.columns(const.RES_NUMBER)
    for c in res_cols:
        with c:
            st.empty()
    return res_cols


def show_recommended_res_info(recommended_res, res_cols, show_score):
    """in this function we get all data what we want to show and put in on webpage"""
    res_ids = recommended_res["business_id"]
    res_name = recommended_res["name"]
    res_scores = recommended_res["score"]
    posters = [fetch_poster(i) for i in res_ids]
    # links = [res_link(i) for i in res_ids]
    for c, t, s, p in zip(res_cols, res_name, res_scores, posters):#, links):
        with c:
            # st.markdown(f"<a style='display: block; text-align: center;' href='{l}'>{t}</a>", unsafe_allow_html=True)
            st.markdown(f"<a style='display: block; text-align: center;'>{t}</a>", unsafe_allow_html=True)
            try:
                p=image = Image.open(p)
            except:
                pass
            st.image(p)
            if show_score:
                st.write(round(s, 3))
                