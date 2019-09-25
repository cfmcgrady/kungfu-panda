# Kungfu Panda
**Kungfu Panda** is a library for register python pandas UDFs in Spark SQL.

[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/cfmcgrady/kungfu-panda/blob/master/LICENSE)

# Quick Start

1. download project.
```
git clone https://github.com/cfmcgrady/kungfu-panda.git
```

2. install python environment by conda.
```
conda env create -f path/to/conda.yaml -p /tmp/kungfu-panda
```

3. train a Kmean classify model with mlflow.
```
/tmp/kungfu-panda/bin/python path/to/train.py
```

4. register model.
```scala
    val spark = SparkSession
      .builder()
      .appName("kungfu panda example")
      .master("local[4]")
      .getOrCreate()

    val python = "/tmp/kungfu-panda/bin/python"
    val artifactRoot = "."
    // find run id with mlflow.
    val runid = "9c6c59d0f57f40dfbbded01816896687"
    val pythonExec = Option(python)
    PandasFunctionManager.registerMLFlowPythonUDF(
      spark, "test",
      returnType = Option(IntegerType),
      artifactRoot = Option(artifactRoot),
      runId = runid,
      driverPythonExec = pythonExec,
      driverPythonVer = None,
      pythonExec = pythonExec,
      pythonVer = None)
    spark.sql(
      """
        |select test(x, y) from (
        |select 1 as x, 1 as y
        |)
        |""".stripMargin)
      .show()
```

# Register Function With Spark SQL

1. add parser extensions when we create `SparkSession`
```scala
val spark = SparkSession
  .builder()
  .appName("panda sql example")
  .master("local[4]")
  .withExtensions(CreateFunctionParser.extBuilder)
  .getOrCreate()
```

2. register mlflow function.
```sql
CREATE FUNCTION `test` AS '${runid}' USING `type` 'mlflow', `returns` 'integer', `artifactRoot` '${artifactRoot}', `pythonExec` '${python}'
```

visit [PandaSqlExample](./examples/local/src/main/scala/org/panda/example/local/PandaSqlExample.scala) for full example.

# Run On Yarn Cluster

// todo
