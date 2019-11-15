from __future__ import print_function
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import RegexpTokenizer

import dataframe_utils as df_utils



NAME = 0
DESCRIPTION = 1
DENSE = 1



def create_vec_tfidf(train, test, min_d=2, max_f=None):
    names = df_utils.get_name_desc_columns(train)
    features_vec_n = fit_tfidf(names, NAME, min_d, max_f)
    train_vec = features_vec_n
    test_vec = create_vec_test(test, tv_n)
    return train_vec, test_vec


def fit_tfidf(df_, column, min_d, max_f):
    global tv_n

    tokenizer_ = RegexpTokenizer(r'\w+')
    tv = TfidfVectorizer(analyzer="word", min_df=min_d, max_features=max_f, sublinear_tf=False, norm='l1', tokenizer=tokenizer_.tokenize)
    tv.fit(df_)
    features_vec_ = tv.transform(df_)

    #print("feature name",tv.get_feature_names())

    if column == NAME:
        tv_n = tv
    else:
        tv_d = tv

    return features_vec_


def create_vec_test(test, tv_n):
    n_test = df_utils.get_name_desc_columns(test)
    search_features_vec = tv_n.transform(n_test)
    return search_features_vec



