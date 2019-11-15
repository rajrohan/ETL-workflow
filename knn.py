import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedKFold
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neighbors import NearestNeighbors
import vectorize
import dataframe_utils as df_utils 



idx = 1
TFIDF = 'tf-idf'
KNClassifier = 'KNeighborsClassifier'
NNeighbors = 'NearestNeighbors'


fields = ['ProductID', 'Product_Name', 'Product_Price','Product_Category']
df = pd.read_csv("./data/distinctProduct_clean.csv", usecols=fields)


def create_results_file():
    params = ['classifier', 'scale_method', 'min_d', 'max_f', 'k_clusters', 'nbrs', 'metric']
    filename = "distinctProduct_result.csv"
    l_ = [p for p in params]
    l_.extend(['Correctly Classified Instances',
               'Accuracy percentage',
               'Incorrectly Classified Instances',
               'Inaccurate percentage'])
    df_ = pd.DataFrame(columns=l_)
    df_.to_csv(filename)


def calc_accuracy(predicted_, labels):
    if type(labels) == np.ndarray:
        actual_ = labels.tolist()
    elif type(labels) == list:
        actual_ = labels
    else:
        actual_ = [labels.iloc[i] for i in range(labels.shape[0])]
    positive_counter = sum(1 for p, a in zip(predicted_, actual_) if p == a)
    return (positive_counter * 100) / actual_.__len__()


def most_common_labels(label_train, k_neigh):
    """Convert k_neigh indices to the corresponding target-label
    and calculate the most common target for each list-item"""
    iloc_attr = getattr(label_train, 'iloc', None)

    if iloc_attr is not None:
        k_neigh = [label_train.iloc[i] for n in k_neigh for i in n]
        k_neigh = [k_neigh[i].values.tolist() for i in range(len(k_neigh))]
    else:
        k_neigh = [label_train[i] for n in k_neigh for i in n]
        k_neigh = [k_neigh[i].tolist() for i in range(len(k_neigh))]
    return most_common_neighbor(k_neigh)


def most_common_neighbor(predicted_):
    """most common class amongst knn"""
    res = []
    for knn in predicted_:
        most_common = max(set(knn), key=knn.count)
        res.append(most_common)
    return res


def divide_to_chunks(test_v):
    chunks = []
    as_csr = test_v.tocsr()
    rows = test_v.shape[0]
    for i in range(0, rows, 500):
        if i < rows - 500:
            chunks.append(as_csr[i:i + 500, :])
        else:
            chunks.append(as_csr[i:, :])
    return chunks


def try_KNeighborsClassifier(train_v, test_v, train_l, n, met):
    neigh_ = KNeighborsClassifier(n_neighbors=1, metric=met).fit(train_v, train_l)
    chunks = divide_to_chunks(test_v)
    k_neigh = [neigh_.kneighbors(chunk, n_neighbors=n, return_distance=False) for chunk in chunks]
    predictions = [neigh_.predict(x) for x in chunks]
    predictions_flat = [item for sublist in predictions for item in sublist]
    return predictions_flat, k_neigh


def try_NearestNeighbors(train_v, test_v, train_l, n, met, r=0.2):
    neigh_ = NearestNeighbors(radius=r, n_neighbors=n, metric=met).fit(train_v, train_l)
    chunks = divide_to_chunks(test_v)
    k_neigh = [neigh_.kneighbors(chunk, return_distance=False) for chunk in chunks]
    return k_neigh



def get_labels(train_, test_):
    return train_["Product_Category"], test_["Product_Category"]


def vec_by_selection(train_, test_, min_d_, max_f_, selection):
    if selection == TFIDF:
        return vectorize.create_vec_tfidf(train_, test_, min_d_, max_f_)


def classify_by_selection(train_v, test_v, train_l, n, met, k_clstrs, selection, r=0.2):
    if selection == KNClassifier:
        predicted_, k_nbrs = try_KNeighborsClassifier(train_v, test_v, train_l, n, met)
        k_nbrs = most_common_labels(train_l, k_nbrs)
        return predicted_
    if selection == NNeighbors:
        k_nbrs = try_NearestNeighbors(train_v, test_v, train_l, n, met, r)
        k_nbrs = most_common_labels(train_l, k_nbrs)
        return k_nbrs


def write_results(accuracy_, len_labels, filename):
    global idx

    with open(filename, 'a') as f:
        params = ['classifier', 'scale_method', 'min_d', 'max_f', 'k_clusters', 'nbrs', 'metric']
        correct = (accuracy_ * len_labels) / 100
        x = [eval(p) for p in params]
        x.extend([correct, accuracy_, len_labels - correct, 100 - accuracy_])
        df_ = pd.DataFrame([x])
        # df_ = pd.read_csv(filename)
        # df_.loc[idx] = x
        df_.to_csv(f, index=False, header=False)
        idx += 1


def stratified_test(max_, met_, c_, s_, file):
    res = [[], []]
    features, labels = df_utils.get_features_and_labels(df)
    skf = StratifiedKFold(n_splits=2, shuffle=True)
    for train_index, test_index in skf.split(features, labels):
        train_, test_ = features[train_index], features[test_index]
        train_l, test_l = labels[train_index], labels[test_index]
        train_v, test_v = vec_by_selection(train_, test_, min_d, max_, selection=s_)
        predicted_ = classify_by_selection(train_v, test_v, train_l, nbrs, met_, k_clusters, c_)
        print("Accuracy of " + c_+":", calc_accuracy(predicted_, test_l))
        res[0].extend(predicted_)
        res[1].extend(test_l)
    
    accuracy_ = calc_accuracy(res[0], res[1])
    write_results(accuracy_, res[1].__len__(), file)


def test_scales(scale_m, file):
    global classifier, scale_method, min_d, max_f, k_clusters, nbrs, metric

    k_clusters = 5
    min_d = 1
    nbrs = 6

    scale_method = scale_m
    print("scale_method: ", scale_m)
    for c in classifiers:
        print("classifier: ", c)
        classifier = c
        for max_ in max_features:
            print("max_features: ", max_)
            max_f = max_
            for met_ in metrics:
                print("metric: ", met_)
                metric = met_
                stratified_test(max_, met_, c, scale_method, file)



# def main():
classifiers = [KNClassifier,NNeighbors]
max_features = [3000, 5000, 10000]
metrics = ['euclidean', 'cosine']
params = ['classifier', 'scale_method', 'min_d', 'max_f', 'k_clusters', 'nbrs', 'metric']


test_scales(TFIDF, file="./data/result/TFIDF_distinctProduct_result.csv")



