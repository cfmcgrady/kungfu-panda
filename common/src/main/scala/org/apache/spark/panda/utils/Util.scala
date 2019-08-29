package org.apache.spark.panda.utils

import java.io.File
import java.nio.file.{Path, Paths}

/**
 * @time 2019-08-26 13:00
 * @author fchen <cloud.chenfu@gmail.com>
 */
object Util {

  type MLFLOW_RUN = (Path, (Path, Path))

  // TODO:(fchen) list mlflow runs from mlflow metadata file `meta.yaml`
  private def listMLFlowRuns(root: String): Array[MLFLOW_RUN] = {
    val rootPath = Paths.get(root)
    recursiveListFiles(new File(root))
      .map {
        case f =>
          rootPath.relativize(f.toPath)
      }.filter(_.getNameCount >= 2)
      .map {
        case path =>
          val experimentId = path.getName(0)
          val runId = path.getName(1)
          (runId, (experimentId, path))
      }
  }

  def getArtifactByRunId(artifactRoot: String, runId: String): String = {
    val runs = listMLFlowRuns(artifactRoot)
    runs.filter {
      case (ri, _) =>
        ri.toString == runId
    }.filter {
      case (_, (_, path)) =>
        path.getFileName.toString == "MLmodel"
    }.headOption
      .map{
        case (_, (_, path)) =>
          artifactRoot + File.separator + path.getParent.toString
      }.getOrElse(
        throw new IllegalArgumentException(s"illegal run id: [ ${runId} ]," +
          s" please make sure the input arguments is right.")
      )
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

}
