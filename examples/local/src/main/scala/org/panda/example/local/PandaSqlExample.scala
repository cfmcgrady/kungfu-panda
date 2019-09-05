package org.panda.example.local

import org.apache.spark.catalyst.parser.CreateFunctionParser
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
 * @time 2019-09-05 14:59
 * @author fchen <cloud.chenfu@gmail.com>
 */
object PandaSqlExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("panda sql example")
      .master("local[4]")
      .withExtensions(CreateFunctionParser.extBuilder)
      .getOrCreate()

    val python = "/usr/local/share/anaconda3/envs/mlflow-study/bin/python"
    val artifactRoot = "/Users/fchen/Project/python/mlflow-study/mlruns"
    val runid = "9c6c59d0f57f40dfbbded01816896687"

    spark.sql(
      s"""
        |CREATE FUNCTION `test` AS '${runid}' USING `type` 'mlflow', `returns` 'integer', `artifactRoot` '${artifactRoot}', `pythonExec` '${python}'
      """.stripMargin)

    spark.sql(
      """
        |select test(x, y) from (
        |select 1 as x, 1 as y
        |)
        |""".stripMargin)
      .show()

  }
}
