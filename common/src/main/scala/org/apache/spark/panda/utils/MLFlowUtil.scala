package org.apache.spark.panda.utils

import org.mlflow.tracking.{MlflowClient, MlflowHttpException}

/**
 * @time 2019/12/9 下午4:24
 * @author fchen <cloud.chenfu@gmail.com>
 */
trait MLFlowUtil {
  def mlflowTrackingUri: String
  protected val client = new MlflowClient(mlflowTrackingUri)

  def artifactUri(runId: String): Either[Throwable, String] = {
    try {
      Right(client.getRun(runId).getInfo.getArtifactUri)
    } catch {
//      case e: MlflowHttpException =>
//        e.printStackTrace()
//        None
      case t: Throwable =>
        Left(t)
    }
//    run.map(_.getInfo.getArtifactUri)
  }
}

object MLFlowUtilTest extends MLFlowUtil {
  //  http://localhost:5000/api/2.0/mlflow/experiments/list
//  override def mlflowTrackingUri: String = "http://localhost:5000"
//  override def mlflowTrackingUri: String = "http://192.168.205.91:9999"
  override def mlflowTrackingUri: String = "http://mlflow.k8s.uc.host.dxy"

  def main(args: Array[String]): Unit = {
//    import scala.collection.JavaConverters._
//    client.listExperiments().asScala.foreach(e => {
//      println(e.getName)
//      println(client.listRunInfos(e.getExperimentId).size())
//      //      client.listArtifacts(e.getName).asScala.foreach(x => {
//      //        println(x.getPath)
//      //      })
//      client.listRunInfos(e.getExperimentId).asScala.foreach(x => {
//        println(x.getArtifactUri)
//      })
//      println(e.getArtifactLocation)
//    })
//
//    println("-------")
    val run = client.getRun("778825406cf44b02940a617b611c8384")

    run.getInfo
      .getArtifactUri
//    println(artifactUri("778825406cf44b02940a617b611c8384"))
//    client.getExperiment("").getExperiment.get

  }
}
