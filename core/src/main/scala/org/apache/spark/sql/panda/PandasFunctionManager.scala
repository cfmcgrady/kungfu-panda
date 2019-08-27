package org.apache.spark.sql.panda

import java.io.File
import java.nio.file.Files

import scala.sys.process.Process

import org.apache.commons.io.IOUtils
import org.apache.spark.api.python.{PythonBroadcast, PythonEvalType, PythonFunction, PythonUtils}
import org.apache.spark.broadcast.Broadcast
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
                                   runId: String,
                                   returnType: Option[DataType] = None,
                                   trackingServerUri: Option[String] = None,
                                   driverPythonExec: Option[String] = None,
                                   driverPythonVer: Option[String] = None,
                                   pythonExec: Option[String] = None,
                                   pythonVer: Option[String] = None
                                  ): Unit = {
    val modelLocalPath = downloadArtifactFromUri(trackingServerUri, runId, "")
//    val modelPath = SparkModelCache.addLocalModel(spark, modelLocalPath)
//    val funcSerPath = Utils.createTempDir().getPath + File.separator + "dump_func"
//    writeBinaryPythonFunc(
//      funcSerPath, modelPath, returnType.getOrElse(IntegerType),
//      driverPythonExec.getOrElse("python")
//    )
//    registerPythonUDF(spark, funcSerPath, functionName, pythonExec, pythonVer)
    registerMLFlowPythonUDF(spark, functionName, modelLocalPath, returnType, driverPythonExec,
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
                                      runId: String,
                                      workDir: String): String = {
//    "/Users/fchen/Project/python/mlflow-study/mlruns/0/9c6c59d0f57f40dfbbded01816896687/artifacts/model"
    "/home/chenfu/mlflow/model"
  }

  def registerPythonUDF(
                       spark: SparkSession,
                       funcDumpPath: String,
                       functionName: String,
                       returnType: Option[DataType],
                       pythonExec: Option[String],
                       pythonVer: Option[String]
                       ): Unit = {
    val binaryPythonFunc = Files.readAllBytes(new File(funcDumpPath).toPath)
    val pythonFunc = binaryPythonFunc
    val workerEnv = new java.util.HashMap[String, String]()
    workerEnv.put("PYTHONPATH", pythonExec.getOrElse("python"))

    import scala.collection.JavaConverters._
    val udf = new UserDefinedPythonFunction(
      name = functionName,
      func = PythonFunction(
        command = pythonFunc,
        envVars = workerEnv.clone().asInstanceOf[java.util.Map[String, String]],
        pythonIncludes = List.empty[String].asJava,
        pythonExec = pythonExec.getOrElse("python"),
//        pythonExec = "mlflow/mlflow/bin/python",
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
}
