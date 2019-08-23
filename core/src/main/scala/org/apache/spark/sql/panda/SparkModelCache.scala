package org.apache.spark.sql.panda

import java.io.File

import org.apache.spark.sql.SparkSession

/**
 * @time 2019-08-22 14:08
 * @author fchen <cloud.chenfu@gmail.com>
 */
object SparkModelCache {

  def addLocalModel(sparkSession: SparkSession,
                    modelPath: String): String = {
    sparkSession.sparkContext.addFile(modelPath, true)

//    val file = new File(modelPath)
//
//    assert(
//      file.exists(),
//      s"model not found in path ${modelPath}, please make sure the confiurations is right."
//    )
//    file.getName
    // return modelPath if we run spark in local mode.
    modelPath
  }

}
