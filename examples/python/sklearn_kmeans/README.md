# Sklearn KMeans

1.环境安装
2.模型训练
```bash
export MLFLOW_TRACKING_URI="http://192.168.218.172:9999" && /usr/local/share/anaconda3/envs/mlflow-sklearn2/bin/python sklearn_kmeans/new_train.py
```
3.部署成REST API

```bash
source activate mlflow-sklearn2
export MLFLOW_TRACKING_URI="http://192.168.218.172:9999" && mlflow models serve -m runs:/a4dc870e46274ff28fce1a537abd07a0/model --no-conda
```

预测
```bash
curl -X POST \
 http://localhost:5000/invocations \
 -H 'cache-control: no-cache' \
 -H 'content-type: application/json' \
 -H 'postman-token: d26ffff1-3cfd-fab7-114a-8976764b4985' \
 -d '{
       "data": [
               {"x": 1,
       "y": 1}
       ]
}'
```

4. Spark SQL分布式批量预测
