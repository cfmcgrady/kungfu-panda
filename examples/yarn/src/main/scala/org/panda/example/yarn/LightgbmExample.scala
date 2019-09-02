package org.panda.example.yarn

import scala.util.Random

import org.apache.spark.sql.{RowFactory, SparkSession}
import org.apache.spark.sql.panda.PandasFunctionManager
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, StructField, StructType}

/**
 * @time 2019-08-27 11:46
 * @author fchen <cloud.chenfu@gmail.com>
 */
object LightgbmExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("LightGBM on Yarn Example")
      .getOrCreate()

    val condaYaml = scala.io.Source.fromFile("/home/chenfu/mlflow/lgb_mlflow_pyfunc/conda.yaml")
      .getLines()
      .mkString("\n")

    // scalastyle:off println
    val python = "/home/chenfu/mlflow/python/bin/python"
    val pythonExec = Option("lightgbm2/bin/python")
    PandasFunctionManager.registerMLFlowPythonUDFLocal(
      spark, "test",
      "/home/chenfu/mlflow/lgb_mlflow_pyfunc",
      returnType = Option(ArrayType(FloatType)),
      driverPythonExec = Option(python),
      pythonExec = pythonExec)

    mockData(spark)

    val df = spark.sql(
      """
        |select test(*) as result
        |from data
      """.stripMargin)

    df.printSchema()
    df.show()
  }

  def mockData(spark: SparkSession): Unit = {
    val row = RowFactory.create((1 to 100).map(i => Float.box(i)).toArray: _*)
    val rdd = spark.sparkContext.parallelize(Seq(row, row, row))
    val schema = StructType(
      (1 to 100).map(i => {
        new StructField(i.toString, FloatType)
      })
    )

    spark.createDataFrame(rdd, schema)
      .createOrReplaceTempView("data")
  }
}
