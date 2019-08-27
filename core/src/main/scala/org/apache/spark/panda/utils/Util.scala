package org.apache.spark.panda.utils

import org.apache.spark.api.python.PythonUtils
import org.apache.spark.sql.SparkSession

/**
 * @time 2019-08-26 13:00
 * @author fchen <cloud.chenfu@gmail.com>
 */
object Util {
  def setupPython(spark: SparkSession, pythonExec: String): String = {
    spark.sparkContext
      .getSparkHome()
      .map(home => {
        s"${home}/python/lib/py4j-0.10.7-src.zip:${home}/python/:$pythonExec"
      })
      .getOrElse(pythonExec)
  }

  def a: String = {
    PythonUtils.sparkPythonPath
  }
}
