# Bamboo

Spark运行MLFlow模型的缓存组件

# 环境变量依赖

构建docker镜像和部署服务都需要依赖相关的环境变量。

| 环境变量 | 说明 |
| --- | --- |
| AWS_ACCESS_KEY_ID | minio访问id，需要有mlflow的bucket权限|
| AWS_SECRET_ACCESS_KEY | minio访问key，需要有mlflow的bucket权限|
| MLFLOW_S3_ENDPOINT_URL | minio地址，外部测试时候使用http://minio.k8s.uc.host.dxy，部署到k8s的时候需要内部域名 |
| BAMBOO_CACHE_DIR | 缓存根路径，默认为/tmp/cache |

build docker image:
```shell
docker build -t bamboo -f dev/bamboo/Dockerfile .
```
