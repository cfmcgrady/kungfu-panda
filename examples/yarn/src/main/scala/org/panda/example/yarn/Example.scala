package org.panda.example.yarn

import java.io.File

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.panda.PandasFunctionManager
import org.apache.spark.sql.types.IntegerType

/**
 * @time 2019-08-23 16:49
 * @author fchen <cloud.chenfu@gmail.com>
 */
object Example {
  def main(args: Array[String]): Unit = {
    // TODO:(fchen) 区分driver和exeuctor端的python命令
    val spark = SparkSession
      .builder()
      .appName("Spark count example")
      .getOrCreate()
    // scalastyle:off println
    println(SparkFiles.get("."))
//    Thread.sleep(1000000)
//    val python = "/home/chenfu/mlflow/mlflow/bin/python"
//    println(Util.a)
    val python = "/home/chenfu/mlflow/python/bin/python"
    val pythonExec = Option("mlflow/mlflow/bin/python")
    PandasFunctionManager.registerMLFlowPythonUDFLocal(
      spark,
      functionName = "test",
      "/home/chenfu/mlflow/model",
      returnType = Option(IntegerType),
      driverPythonExec = Option(python),
      pythonExec = pythonExec)

    spark.sql(
      """
        |select test(x, y) from (
        |select 1 as x, 1 as y
        |)
        |""".stripMargin)
      .show()

  }

}
