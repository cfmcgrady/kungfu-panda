from pandas import DataFrame

Data = {
    'x': [25, 34, 22, 27, 33, 33, 31, 22, 35, 34, 67, 54, 57, 43, 50, 57, 59, 52, 65, 47, 49, 48, 35, 33, 44, 45, 38,
          43, 51, 46],
    'y': [79, 51, 53, 78, 59, 74, 73, 57, 69, 75, 51, 32, 40, 47, 53, 36, 35, 58, 59, 50, 25, 20, 14, 12, 20, 5, 29, 27,
          8, 7]
    }

df = DataFrame(Data, columns=['x', 'y'])
# print(df)
from sklearn.cluster import KMeans

kmeans = KMeans(n_clusters=3).fit(df)


d2 = {
    'x': [100],
    'y': [100]
}
d = DataFrame(d2, columns=['x', 'y'])
print(kmeans.predict(d))

# import matplotlib.pyplot as plt
# df['cl'] = kmeans.labels_
# df.plot.scatter('x', 'y', c='cl', colormap='gist_rainbow')
#
# plt.show()

import mlflow
import mlflow.sklearn
with mlflow.start_run():
    mlflow.sklearn.log_model(kmeans, "model")
