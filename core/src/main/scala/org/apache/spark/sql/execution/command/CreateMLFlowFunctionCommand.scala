package org.apache.spark.sql.execution.command

import java.util.Base64

import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.panda.utils.{Conda, MLmodelParser}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
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
//    val pythonExec = options.get("pythonexec")
//    val pythonVer = options.get("pythonver")
//    PandasFunctionManager.registerMLFlowPythonUDF(
//      sparkSession, functionName,
//      returnType = Option(DataType.fromDDL(options("returns"))),
//      artifactRoot = Option(options("artifactroot")),
//      runId = className,
//      driverPythonExec = pythonExec,
//      driverPythonVer = pythonVer,
//      pythonExec = pythonExec,
//      pythonVer = pythonVer)
    setup(sparkSession, options.getOrElse("runid", className))
    Seq.empty[Row]
  }

  def setup(sparkSession: SparkSession,
            runid: String): Unit = {

    // first we download mlflow run from bamboo server and parser MLmodel file.
    val run = s"http://${bambooServer}/api/v1/artifact/createAndGet/${runid}/${runid}.tgz"
    sparkSession.sparkContext.addFile(run)
    val mlmodelPath = SparkFiles.get(runid) + s"/artifacts/model/MLmodel"
    val content = scala.io.Source.fromFile(mlmodelPath)
      .getLines()
      .mkString("\n")
    val mlmodel = new MLmodelParser(content)

    // second we download the python environment from bamboo server with the conda configurations.
    val condaConfPath = SparkFiles.get(runid) + s"/artifacts/${mlmodel.artifactPath}/${mlmodel.env}"
    val condaYaml = scala.io.Source.fromFile(condaConfPath)
      .getLines()
      .mkString("\n")
    val name = Conda.normalize(condaYaml).get("name").toString
    val encodeConf = Base64.getEncoder.encodeToString(condaYaml.getBytes("utf-8"))
    val condaUrl = s"http://${bambooServer}/api/v1/conda/createAndGet/${encodeConf}/${name}.tgz"
    sparkSession.sparkContext.addFile(condaUrl)

    val driverPython = s"${SparkFiles.get(name)}/bin/python"
    val pythonPath = s"./${name}/bin/python"
    val pythonExec = Option(pythonPath)
    val pythonVer = options.get("pythonver")

    PandasFunctionManager.registerMLFlowPythonUDFLocal(
      sparkSession,
      functionName,
      s"./${runid}/artifacts/${mlmodel.artifactPath}",
      returnType = Option(DataType.fromDDL(options("returns"))),
      driverPythonExec = Option(driverPython),
      driverPythonVer = pythonVer,
      pythonExec = pythonExec,
      pythonVer = pythonVer)
    Seq.empty[Row]
  }

  val PANDA_BAMBOO_SERVER = SQLConf.buildConf("spark.panda.bamboo.server")
    .stringConf
    .checkValue(address => address != "",
      "can't find spark.panda.bamboo.server in spark conf, " +
        "please make sure you have set right configurations"
    ).createWithDefaultString("")

  val bambooServer = SQLConf.get.getConf(PANDA_BAMBOO_SERVER)
  //    throw new RuntimeException("can't find spark.panda.bamboo.server in spark conf, " +
  //      "please make sure you have set right configurations"))

}
