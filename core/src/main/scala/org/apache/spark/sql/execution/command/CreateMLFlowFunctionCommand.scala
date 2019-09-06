package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.panda.PandasFunctionManager
import org.apache.spark.sql.types.DataType

/**
 * @time 2019-09-05 13:52
 * @author fchen <cloud.chenfu@gmail.com>
 */
case class CreateMLFlowFunctionCommand(
    databaseName: Option[String],
    functionName: String,
    className: String,
    options: Map[String, String],
    isTemp: Boolean,
    ignoreIfExists: Boolean,
    replace: Boolean)
  extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val pythonExec = Option(options("pythonexec"))
    PandasFunctionManager.registerMLFlowPythonUDF(
      sparkSession, functionName,
      returnType = Option(DataType.fromDDL(options("returns"))),
      artifactRoot = Option(options("artifactroot")),
      runId = className,
      driverPythonExec = pythonExec,
      driverPythonVer = None,
      pythonExec = pythonExec,
      pythonVer = None)
    Seq.empty[Row]
  }
}
