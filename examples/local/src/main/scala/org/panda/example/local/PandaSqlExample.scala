package org.panda.example.local

import java.io.{File, FileInputStream}
import java.util.Base64

import com.google.common.io.ByteStreams
import org.apache.spark.catalyst.parser.CreateFunctionParser
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.panda.PandasFunctionManager
import org.apache.spark.sql.types.StringType

/**
 * @time 2019-09-05 14:59
 * @author fchen <cloud.chenfu@gmail.com>
 */
// scalastyle:off
object PandaSqlExample {
  def main(args: Array[String]): Unit = {
//
    test()
    System.exit(0)
    val spark = SparkSession
      .builder()
      .appName("panda sql example")
      .master("local[4]")
      .config("spark.sql.extensions", "org.apache.spark.catalyst.parser.PandaSparkExtensions")
      .config("spark.panda.bamboo.server.enable", "false")
//      .withExtensions(CreateFunctionParser.extBuilder)
      .getOrCreate()
    val python = "/usr/local/share/anaconda3/envs/mlflow-study/bin/python"
//    val python = "/usr/local/share/anaconda3/envs/pyspark-2.4.3/bin/python"
    val path = "/Users/fchen/Project/fchen/kungfu-panda/examples/python/sklearn_kmeans/mlruns/1/e60af958648a4f7981c1195f82d82c1d/artifacts/model"
    spark.sql(
      s"""
        |CREATE FUNCTION `test` AS '909e8c3a8b504f11ac29150af83cee42' USING
        |         `type` 'mlflow',
        |         `modelLocalPath` '$path',
        |         `pythonExec` '$python',
        |         `returns` 'int'
        |""".stripMargin)

    import spark.implicits._
    Seq(
      (11, 22),
      (33, 44)
    ).toDF("x", "y")
      .repartition(1)
      .selectExpr("test(x, y)")
      .show()
//      .explain(true)

//    spark.sql(
//      """
//        |select test(x, y) from (
//        |select 1 as x, 1 as y
//        |)
//        |""".stripMargin)
//      .show()

  }

  def test(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("panda sql example")
      .master("local[4]")
      .config("spark.sql.extensions", "org.apache.spark.catalyst.parser.PandaSparkExtensions")
      .config("spark.panda.bamboo.server.enable", "false")
      //      .withExtensions(CreateFunctionParser.extBuilder)
      .getOrCreate()
    val path = "/Users/fchen/Project/fchen/examples/mlflow-in-action/add/mlruns/1/58d234e03699404c938e0ba87d627920/artifacts/model"
    val python = "/usr/local/share/anaconda3/envs/mlflow-study/bin/python"
    //    val python = "/usr/local/share/anaconda3/envs/pyspark-2.4.3/bin/python"
//    val path = "/Users/fchen/Project/fchen/kungfu-panda/examples/python/sklearn_kmeans/mlruns/1/e60af958648a4f7981c1195f82d82c1d/artifacts/model"
    spark.sql(
      s"""
         |CREATE FUNCTION `test` AS '909e8c3a8b504f11ac29150af83cee42' USING
         |         `type` 'mlflow',
         |         `modelLocalPath` '$path',
         |         `pythonExec` '$python',
         |         `returns` 'int'
         |""".stripMargin)

    val df = spark.sql(
      """
        |select *,test(x) from (
        |select 1223 as x, 13334
        |)
        |""".stripMargin)
//      .show()
    df.explain(true)
    println("------------")
//    spark.sql("select 1").explain(true)
    import spark.implicits._
    Seq(
      (11, 22),
      (33, 44)
    ).toDF("x", "y")
      .repartition(1)
      .selectExpr("test(x)")
      .explain(true)
    df.show
//    spark.sql(
//      """
//        |select x + 1 from (
//        |select 1 as x
//        |)
//        |""".stripMargin)
//      .explain(true)
//      .show()
  }
  def badcase: Unit = {
    // todo:(fchen) 嵌套下为什么会有问题
    val sql =
      """
        |select test(test(x)) from (
        |select 1223 as x, 13334 as y
        |)
        |""".stripMargin
    //      .show()
  }
}
