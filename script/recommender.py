import scipy
import pickle
from sklearn.metrics.pairwise import linear_kernel, cosine_similarity
import constants as const
from script.utils import get_recommendations


def weighted_average_based_recommendations():
    """we have already saved dataframe which is sorted based on scores.
    and just read and get top res"""
    with open('data/res_scores.pickle', 'rb') as handle:
        ress = pickle.load(handle)
    ress = ress.head(const.RES_NUMBER)
    ress = ress[["business_id", "name", "score"]]
    ress.columns = ["business_id", "name", "score"]
    return ress


def contend_based_recommendations(res,titles):
    """read matrix create similarity function and call main function"""
    tfidf_matrix = scipy.sparse.load_npz('data/res_matrix.npz')
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
    return get_recommendations(res, titles, cosine_sim)


# def contend_based_recommendations_extra(res, titles):
#     """read matrix create similarity function and call main function"""
#     count_matrix = scipy.sparse.load_npz("data/count_matrix.npz")
#     cosine_sim = cosine_similarity(count_matrix, count_matrix)
#     return get_recommendations(res, titles, cosine_sim)

