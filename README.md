# Kungfu Panda
**Kungfu Panda** is a library for register python pandas UDFs in Spark SQL.

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

# Run On Yarn Cluster

// todo
