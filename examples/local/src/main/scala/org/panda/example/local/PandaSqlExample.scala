package org.panda.example.local

import java.io.{File, FileInputStream}
import java.util.Base64

import com.google.common.io.ByteStreams
import org.apache.spark.catalyst.parser.CreateFunctionParser
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
 * @time 2019-09-05 14:59
 * @author fchen <cloud.chenfu@gmail.com>
 */
// scalastyle:off
object PandaSqlExample {
  def main(args: Array[String]): Unit = {
//
    val path = "/tmp/testdata/12891819633_e4c82b51e8.jpg"
    val file = new File(path)
    val in = new FileInputStream(file)
    val array = ByteStreams.toByteArray(in)
    println(array.slice(0, 100).mkString(","))
//    println(new String(Base64.getEncoder.encode(array), "utf-8"))
//    println(new String(      org.apache.commons.codec.binary.Base64.encodeBase64(
//      array
//    )))
////    println(array.slice(0, 100).mkString(","))
//    System.exit(0)


    val spark = SparkSession
      .builder()
      .appName("panda sql example")
      .master("local[4]")
      .config("spark.sql.extensions", "org.apache.spark.catalyst.parser.PandaSparkExtensions")

//      .withExtensions(CreateFunctionParser.extBuilder)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val binaryFile = spark.sparkContext.binaryFiles("/tmp/testdata/*")
        .map(r => (r._1, r._2.toArray()))
        .toDF("filename", "image")

//    val df = spark.read.format("binaryFile")
      val df = binaryFile
          .selectExpr("cast(base64(image) as string) as feature", "filename")
//    val df = spark.read
//      .format("image")
//      .load("/tmp/flower_photos/*")
////      .select($"value", input_file_name as "name")
////      .printSchema()
//      .select("image.*")
//      .selectExpr("base64(data) as feature_array", "origin", "data")
//      .selectExpr("cast(feature_array as string) as feature", "xx(feature_array) as f2", "feature_array", "data", "yy(data)")
//    df.show()
//    System.exit(0)
//      .show()

//    val python = "/usr/local/share/anaconda3/envs/tensorflow-example/bin/python"
    val python = "/usr/local/share/anaconda3/envs/flower_classifier/bin/python"
    val artifactRoot = "/tmp"
    val runid = "c1f48fc796f3467cb104114f3fa501df"
//    val python = "/usr/local/share/anaconda3/envs/mlflow-study/bin/python"
//    val artifactRoot = "/Users/fchen/Project/python/mlflow-study/mlruns"
//    val runid = "9c6c59d0f57f40dfbbded01816896687"
//
    spark.sql(
      s"""
        |CREATE FUNCTION `test` AS '${runid}' USING
        |  `type` 'mlflow',
        |  `returns` 'array<string>',
        |  `artifactRoot` '${artifactRoot}',
        |  `pythonExec` '${python}',
        |  `pythonVer` '3.7'
      """.stripMargin)

    df.selectExpr("test(feature) as predict", "filename").show()
//
//    spark.sql(
//      """
//        |select test(x, y) from (
//        |select 1 as x, 1 as y
//        |)
//        |""".stripMargin)
//      .show()

  }
}
