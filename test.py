import pickle

# load main movie dataframe
with open('data/movie_df.pickle', 'rb') as handle:
    movie = pickle.load(handle)
    print(movie)
