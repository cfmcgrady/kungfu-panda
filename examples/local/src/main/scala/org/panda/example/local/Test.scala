package org.panda.example.local

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.panda.PandasFunctionManager
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType}

/**
 * Created by fchen on 2017/9/11.
 */
object Test{
  def main(args: Array[String]): Unit = {
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
    val pythonExec = Option(python)
    PandasFunctionManager.registerMLFlowPythonUDF(spark, functionName = "test", "",
      returnType = Option(IntegerType), pythonExec = pythonExec)

    spark.sql(
      """
        |select test(x, y) from (
        |select 1 as x, 1 as y
        |)
        |""".stripMargin)
      .show()


  }
}
