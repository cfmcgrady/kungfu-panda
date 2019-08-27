package org.panda.example.local

import scala.util.Random

import org.apache.spark.sql.{RowFactory, SparkSession}
import org.apache.spark.sql.panda.PandasFunctionManager
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, StructField, StructType}

/**
 * @time 2019-08-27 11:06
 * @author fchen <cloud.chenfu@gmail.com>
 */
object LightgbmExample {
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

    spark.udf.register("mdata", () => {
      Array(
        (1 to 100).map(
          i =>
            (Random.nextDouble() * 100).toFloat
        )
      )
    })
    import spark.implicits._
    val row = RowFactory.create((1 to 100).map(i => Float.box(i)).toArray: _*)
    val rdd = spark.sparkContext.parallelize(Seq(row))
    val schema = StructType(
      (1 to 100).map(i => {
        new StructField(i.toString, FloatType)
      })
    )
    schema.printTreeString()

    spark.createDataFrame(rdd, schema)
        .show()
//    Seq(
//      (0 to 100)
//    ).toDF((0 to 100).map(_.toString): _*).show()
    System.exit(0)

    val python = "/usr/local/share/anaconda3/envs/lightgbm-test/bin/python"
    //    val python = "/usr/local/share/anaconda3/envs/pyspark-2.4.3/bin/python"
    val mpath = "/Users/fchen/Project/dxy/app-lightgbm-rerank/lgb_mlflow_pyfunc"
    val pythonExec = Option(python)
    PandasFunctionManager.registerMLFlowPythonUDFLocal(
      spark, "test", mpath,
      returnType = Option(ArrayType(FloatType)),
      driverPythonExec = pythonExec,
      driverPythonVer = None,
      pythonExec = pythonExec,
      pythonVer = None)
//
//    spark.sql(
//      """
//        |select test(x, y) from (
//        |select 1 as x, 1 as y
//        |)
//        |""".stripMargin)
//      .show()
    val df = spark.sql(
      """
        |select test(feature) as result
        | from(
        |select mdata() as feature
        | )
      """.stripMargin)

    df.printSchema()
//    df.show()

  }

}
