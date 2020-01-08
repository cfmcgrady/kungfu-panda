package org.panda.example.yarn

import java.io.File

import org.apache.spark.sql.SparkSession

/**
 * @time 2019/12/10 下午3:52
 * @author fchen <cloud.chenfu@gmail.com>
 */
object KmeansExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("panda sql example")
      .master("local[4]")
      .config("spark.sql.extensions", "org.apache.spark.catalyst.parser.PandaSparkExtensions")
      .config("spark.files.fetchTimeout", "600s")
      .config("spark.panda.bamboo.server", "192.168.202.205:8888")
//      .config("spark.panda.bamboo.server", "192.168.200.69:8100")
      .getOrCreate()

    import spark.implicits._

    //    val python = "/usr/local/share/anaconda3/envs/tensorflow-example/bin/python"
    val runid = "a063487ee34e463baf7101d145b96bb7"
    //    val python = "/usr/local/share/anaconda3/envs/mlflow-study/bin/python"
    //    val artifactRoot = "/Users/fchen/Project/python/mlflow-study/mlruns"
    //    val runid = "9c6c59d0f57f40dfbbded01816896687"
    //
    spark.sql("set spark.sql.crossJoin.enabled = true")

    spark.sql(
      s"""
         |CREATE FUNCTION `test` AS '${runid}' USING
         |  `type` 'mlflow',
         |  `returns` 'int'
          """.stripMargin)

    spark.sql(
      s"""
         |CREATE FUNCTION `test` AS '${runid}' USING
         |  `type` 'mlflow',
         |  `returns` 'int'
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
