package org.panda.example.local

import java.io.{BufferedInputStream, File, FileOutputStream, IOException}
import java.util.zip.{ZipEntry, ZipFile, ZipOutputStream}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.panda.PandasFunctionManager
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType}

/**
 * Created by fchen on 2017/9/11.
 */
object Test{
  def main(args: Array[String]): Unit = {
    run
  }

  def run {
    // scalastyle:off println
    val spark = SparkSession
      .builder()
      .appName("Spark count example")
      .master("local[4]")
      .getOrCreate()
    println(StringType.json)
    println(StringType.catalogString)
    println(ArrayType(StringType).json)
    println(ArrayType(StringType).catalogString)
    println(DataType.fromJson(StringType.json))

    val s = ArrayType(StringType).catalogString
    println(DataType.fromDDL(s))

    val python = "/usr/local/share/anaconda3/envs/mlflow-study/bin/python"
//    val python = "/usr/local/share/anaconda3/envs/pyspark-2.4.3/bin/python"
    val artifactRoot = "/Users/fchen/Project/python/mlflow-study/mlruns"
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


  }


}
