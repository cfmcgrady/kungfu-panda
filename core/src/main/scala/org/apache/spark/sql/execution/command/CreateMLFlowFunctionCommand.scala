package org.apache.spark.sql.execution.command
import org.apache.spark.sql.panda.PandasFunctionManager
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.sql.{Row, SparkSession}

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


//case class CreateFunctionCommand(
//                                  databaseName: Option[String],
//                                  functionName: String,
//                                  className: String,
//                                  resources: Seq[FunctionResource],
//                                  isTemp: Boolean,
//                                  ignoreIfExists: Boolean,
//                                  replace: Boolean)
//  extends RunnableCommand {
//
//  if (ignoreIfExists && replace) {
//    throw new AnalysisException("CREATE FUNCTION with both IF NOT EXISTS and REPLACE" +
//      " is not allowed.")
//  }
//
//  // Disallow to define a temporary function with `IF NOT EXISTS`
//  if (ignoreIfExists && isTemp) {
//    throw new AnalysisException(
//      "It is not allowed to define a TEMPORARY function with IF NOT EXISTS.")
//  }
//
//  // Temporary function names should not contain database prefix like "database.function"
//  if (databaseName.isDefined && isTemp) {
//    throw new AnalysisException(s"Specifying a database in CREATE TEMPORARY FUNCTION " +
//      s"is not allowed: '${databaseName.get}'")
//  }
//
//  override def run(sparkSession: SparkSession): Seq[Row] = {
//    val catalog = sparkSession.sessionState.catalog
//    val func = CatalogFunction(FunctionIdentifier(functionName, databaseName), className, resources)
//    if (isTemp) {
//      // We first load resources and then put the builder in the function registry.
//      catalog.loadFunctionResources(resources)
//      catalog.registerFunction(func, overrideIfExists = replace)
//    } else {
//      // Handles `CREATE OR REPLACE FUNCTION AS ... USING ...`
//      if (replace && catalog.functionExists(func.identifier)) {
//        // alter the function in the metastore
//        catalog.alterFunction(func)
//      } else {
//        // For a permanent, we will store the metadata into underlying external catalog.
//        // This function will be loaded into the FunctionRegistry when a query uses it.
//        // We do not load it into FunctionRegistry right now.
//        catalog.createFunction(func, ignoreIfExists)
//      }
//    }
//    Seq.empty[Row]
//  }
//}
