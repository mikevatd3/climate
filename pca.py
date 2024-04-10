import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

df = (
    pd.read_csv("demos.csv")
    .drop(["total_population.1", "name"], axis=1)
    .set_index(["geoid"])
)

X = df.values

scaler = StandardScaler()
scaled_X = scaler.fit_transform(X)
pca = PCA(n_components=1)
pca.fit(scaled_X)

print(pca.components_)
