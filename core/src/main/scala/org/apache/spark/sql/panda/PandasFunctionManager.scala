package org.apache.spark.sql.panda

import java.io.File
import java.nio.file.Files

import scala.sys.process.Process

import org.apache.commons.io.IOUtils
import org.apache.spark.api.python.{PythonBroadcast, PythonEvalType, PythonFunction, PythonUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.panda.utils.Util
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.util.Utils

/**
 * @time 2019-08-22 11:39
 * @author fchen <cloud.chenfu@gmail.com>
 */
object PandasFunctionManager {

  def registerMLFlowPythonUDF(spark: SparkSession,
                              functionName: String,
                              returnType: Option[DataType] = None,
                              trackingServerUri: Option[String] = None,
                              artifactRoot: Option[String] = None,
                              runId: String,
                              driverPythonExec: Option[String] = None,
                              driverPythonVer: Option[String] = None,
                              pythonExec: Option[String] = None,
                              pythonVer: Option[String] = None
                             ): Unit = {

    val modelLocalPath = downloadArtifactFromUri(trackingServerUri, artifactRoot, runId, "")
    registerMLFlowPythonUDFLocal(spark, functionName, modelLocalPath, returnType, driverPythonExec,
      driverPythonVer, pythonExec, pythonVer)
  }

  def registerMLFlowPythonUDFLocal(spark: SparkSession,
                              functionName: String,
                              modelLocalPath: String,
                              returnType: Option[DataType] = None,
                              driverPythonExec: Option[String] = None,
                              driverPythonVer: Option[String] = None,
                              pythonExec: Option[String] = None,
                              pythonVer: Option[String] = None
                             ): Unit = {
    val modelPath = SparkModelCache.addLocalModel(spark, modelLocalPath)
    val funcSerPath = Utils.createTempDir().getPath + File.separator + "dump_func"
    writeBinaryPythonFunc(
      funcSerPath, modelPath, returnType.getOrElse(IntegerType),
      driverPythonExec.getOrElse("python")
    )
    registerPythonUDF(spark, funcSerPath, functionName, returnType, pythonExec, pythonVer)
  }

  /**
   * download mlflow artifact from given uri.
   */
  private def downloadArtifactFromUri(trackingServerUri: Option[String],
                                      artifactRoot: Option[String],
                                      runId: String,
                                      workDir: String): String = {
    if (trackingServerUri.isDefined) {
      // TODO:(Fchen) find --default-artifact-root from trackingServerUri
      throw new UnsupportedOperationException()
    } else if (artifactRoot.isDefined) {
      val rootURI = Utils.resolveURI(artifactRoot.get)
      rootURI.getScheme.toLowerCase match {
        case "file" =>
          Util.getArtifactByRunId(artifactRoot.get, runId)
        case "sftp" =>
          throw new UnsupportedOperationException()
        case "hdfs" =>
          throw new UnsupportedOperationException()
        case _ =>
          throw new UnsupportedOperationException()
      }
    } else {
      throw new IllegalArgumentException("please input tracking server uri or artifact root.")
    }
  }

  def registerPythonUDF(spark: SparkSession,
                        funcDumpPath: String,
                        functionName: String,
                        returnType: Option[DataType],
                        pythonExec: Option[String],
                        pythonVer: Option[String]): Unit = {

    val binaryPythonFunc = Files.readAllBytes(new File(funcDumpPath).toPath)
    val pythonFunc = binaryPythonFunc
    val workerEnv = new java.util.HashMap[String, String]()

    // in local run mode, we get pyspark runtime from python environment. but in cluster manager run
    // mode, we should put pyspark runtime environment (pyspark.zip and py4j.zip) into executor host
    // and set PYTHONPATH system environment point to pyspark package file path. this action is in
    // order to reduce the package size of python runtime.
    addPysparkRuntime(spark)
    workerEnv.put("PYTHONPATH", {
      if (spark.sparkContext.isLocal) {
        pythonExec.getOrElse("python")
      } else {
        "pyspark.zip:py4j-0.10.7-src.zip"
      }
    })
    import scala.collection.JavaConverters._
    val udf = new UserDefinedPythonFunction(
      name = functionName,
      func = PythonFunction(
        command = pythonFunc,
        envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
        pythonIncludes = List.empty[String].asJava,
        pythonExec = pythonExec.getOrElse("python"),
        pythonVer = pythonVer.getOrElse("3.6"),
        broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
        accumulator = null),
      dataType = returnType.getOrElse(IntegerType),
      pythonEvalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      udfDeterministic = true)
    spark.udf.registerPython(udf.name, udf)
  }

  private def writeBinaryPythonFunc(binaryFilePath: String,
                                    modelPath: String,
                                    returnType: DataType,
                                    pythonExec: String): Unit = {
    val s = getClass.getClassLoader.getResourceAsStream("dump_pyfunc.py")
    val cmd = IOUtils.toString(s)
    val command = Seq(
      pythonExec,
      "-c",
      cmd,
      binaryFilePath,
      returnType.json,
      modelPath)
    // todo
    // scalastyle:off
    println(command.mkString(" "))
    println(Process(
      command, None, "PYTHONPATH" -> PythonUtils.sparkPythonPath
    )!!)
  }

  /**
   * add pyspark runtime for python work in the executor side.
   */
  private def addPysparkRuntime(spark: SparkSession): Unit = {
    if (!spark.sparkContext.isLocal) {
      PythonUtils.sparkPythonPath.split(":")
        .foreach(path => {
          // TODO:(fchen) check whether these files have already uploaded to spark cache.
          spark.sparkContext.addFile(path)
        })
    }
  }

}
