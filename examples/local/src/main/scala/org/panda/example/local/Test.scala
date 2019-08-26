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
//    test()
//    unzip("/tmp/test/a.zip", "/tmp/test/dd", true)
    run
//    PandasFunctionManager.a
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
    val modelPath = "/Users/fchen/Project/python/mlflow-study/mlruns/0/9c6c59d0f57f40dfbbded01816896687/artifacts/model"
    val pythonExec = Option(python)
    PandasFunctionManager.registerMLFlowPythonUDFLocal(
      spark, "test", modelPath,
      returnType = Option(IntegerType),
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
