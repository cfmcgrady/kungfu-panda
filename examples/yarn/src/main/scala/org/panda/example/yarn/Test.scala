// scalastyle:off
//package org.panda.example.yarn
//
//import cloud.fchen.spark.utils.IdeaUtil
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.panda.PandasFunctionManager
//import org.apache.spark.sql.types.IntegerType
//
///**
// * Created by fchen on 2017/9/11.
// */
//object Test {
//  def main(args: Array[String]): Unit = {
//    val uuid = this.getClass.getSimpleName.replaceAll("\\$", "")
//    val classpathTempDir = s"/tmp/aaa/$uuid"
//    val util = new IdeaUtil(
//      None,
//      Option(classpathTempDir),
//      dependenciesInHDFSPath = s"libs/$uuid",
//      principal = Option("chenfu@CDH.HOST.DXY"),
//      keytab = Option("/Users/fchen/tmp/chenfu.keytab")
//    )
//    util.setup()
//    val conf = new SparkConf()
//      .setMaster("yarn-client")
//      .set("spark.yarn.archive", "hdfs:///user/chenfu/libs/spark-2.4.3-bin-hadoop2.7.jar.zip")
//      .set("spark.repl.class.outputDir", classpathTempDir)
//    // scalastyle:off
//    conf.getAll.foreach(println)
//    println("-----")
//    val spark = SparkSession
//      .builder()
//      .appName("Spark count example")
//      .config(conf)
//      .getOrCreate()
//    val python = "/usr/local/share/anaconda3/envs/mlflow-study/bin/python"
//    spark.sparkContext.addFile("http://192.168.218.12:8080/api/v1/test/kp")
////    val pythonExec = Option(python)
////    PandasFunctionManager.registerMLFlowPythonUDF(spark, functionName = "test", "",
////      returnType = Option(IntegerType), pythonExec = pythonExec)
////
////    spark.sql(
////      """
////        |select test(x, y) from (
////        |select 1 as x, 1 as y
////        |)
////        |""".stripMargin)
////      .show()
//    spark.sql("select 1").show()
//    Thread.sleep(Int.MaxValue)
//  }
//}
