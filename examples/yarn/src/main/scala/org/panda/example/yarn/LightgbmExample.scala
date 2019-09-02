package org.panda.example.yarn

import java.util.Base64

import scala.util.Random

import org.apache.spark.SparkFiles
import org.apache.spark.panda.utils.{Conda, Util}
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

    run2(spark)

    mockData(spark)

    val df = spark.sql(
      """
        |select test(*) as result
        |from data
      """.stripMargin)

    df.printSchema()
    df.show()
  }

  def setup(spark: SparkSession): Unit = {
    // scalastyle:off println
    val python = "/home/chenfu/mlflow/python/bin/python"
    val pythonExec = Option("lightgbm2/bin/python")
    PandasFunctionManager.registerMLFlowPythonUDFLocal(
      spark, "test",
      "/home/chenfu/mlflow/lgb_mlflow_pyfunc",
      returnType = Option(ArrayType(FloatType)),
      driverPythonExec = Option(python),
      pythonExec = pythonExec)
  }

  def run2(sparkSession: SparkSession): Unit = {
    // setup env.
    val modelLocalPath = "/home/chenfu/mlflow/lgb_mlflow_pyfunc"

    val condaYaml = scala.io.Source.fromFile(s"${modelLocalPath}/conda.yaml")
      .getLines()
      .mkString("\n")

    val name = Conda.normalize(condaYaml).get("name").toString
    val encodeConf = Base64.getEncoder.encodeToString(condaYaml.getBytes("utf-8"))
    val url = s"http://192.168.200.69:8100/api/v1/conda/createAndGet/${encodeConf}/${name}.tgz"

    sparkSession.sparkContext.addFile(url)
    val pythonPath = s"${name}/bin/python"
    val dpp = SparkFiles.get(name) + "/bin/python"

    PandasFunctionManager.registerMLFlowPythonUDFLocal(
      sparkSession, "test",
      modelLocalPath,
      returnType = Option(ArrayType(FloatType)),
      driverPythonExec = Option(dpp),
      pythonExec = Option(pythonPath))
  }

//  def setup(spark: SparkSession,
//            bambooAddress: String,
//           ): Unit = {
//  }

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
