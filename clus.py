"""Perform the data clustering algorithm using any Clustering algorithm"""

import pandas as pd
from sklearn import datasets
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns

iris = datasets.load_iris()
X = iris.data  # Features: sepal length, sepal width, petal length, petal width

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

kmeans = KMeans(n_clusters=3, random_state=42)
kmeans.fit(X_scaled)

labels = kmeans.labels_
centers = kmeans.cluster_centers_

plt.figure(figsize=(8, 6))
plt.scatter(X_scaled[:, 0], X_scaled[:, 1], c=labels, cmap='viridis', marker='o', edgecolor='k', s=100)

plt.scatter(centers[:, 0], centers[:, 1], c='red', marker='X', s=200, label='Centroids')
