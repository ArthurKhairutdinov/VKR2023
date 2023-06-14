import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

def process_all(df_to_process, norm_names=[], cat_names=[], cat_threshold=10, fillna_strat='mean', fillna_val=0):
    if fillna_strat == 'mean':
        df_to_process = df_to_process.fillna(df_to_process.mean())
    elif fillna_strat == 'mode':
        df_to_process = df_to_process.fillna(df_to_process.mode())
    elif fillna_strat == 'val':
        df_to_process = df_to_process.fillna(fillna_val)
    df_to_process = df_to_process.drop_duplicates()
    for item in norm_names:
        arr = np.array(df_to_process[item])
        b = np.ndarray.sum(arr)
        df_to_process[item] = df_to_process[item] / b
    for item in cat_names:
        if df_to_process[item].nunique() < cat_threshold:
            y = pd.get_dummies(df_to_process[item], prefix=item)
            df_to_process = df_to_process.join(y)
            df_to_process = df_to_process.drop(item, axis=1)
    return df_to_process

def learn(df_to_learn, target_name,exclude,test_size):
    target = df_to_learn[target_name]
    for item in exclude:
        df_to_learn = df_to_learn.drop(item, axis=1)
    df_to_learn = df_to_learn.drop(target_name, axis=1)
    X_train, X_test, y_train, y_test = train_test_split(df_to_learn, target, test_size=test_size, random_state=42)
    reg = LinearRegression().fit(X_train, y_train)
    return reg.score(X_test, y_test)
