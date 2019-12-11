package org.apache.spark.panda.utils

import java.net.{URI, URL}
import java.nio.file.Paths

import scala.collection.JavaConverters._

import io.minio.MinioClient
import io.minio.messages.Item
import org.apache.commons.logging.LogFactory

/**
 * @time 2019/12/9 下午5:22
 * @author fchen <cloud.chenfu@gmail.com>
 */
trait MinioUtil {

  val logger = LogFactory.getLog(this.getClass)

  def serverInfo: S3ServerInfo

  val minioClient =
    new MinioClient(serverInfo.endpoint, serverInfo.accessKeyId, serverInfo.secretAccessKey)

  def downloadAsZip(bucket: String,
                    obj: String,
                    localFilePath: String,
                    normalizePath: String => String): Unit = {
    logger.info(s"begin to download bucket = ${bucket}, object = ${obj}, localFilePath = ${localFilePath}.")
    GZIPUtilV2.streamCreateTarArchive(localFilePath) {
      tar => {
        val f = {
          (item: Item) =>
            val in = minioClient.getObject(bucket, item.objectName())
            try {
              GZIPUtilV2.streamAddToArchive(in, item.objectSize(), normalizePath(item.objectName()), tar)
            } finally {
              in.close()
            }
        }
        visit(bucket, obj, f)
      }
    }
  }

//  def resovleUri(uri: URI): (String, String) = {
//    val path = uri.getPath.split("/")
//    (path.head, path.slice(1, path.length).mkString("/"))
//  }

  /**
   * visit given path, bucket/directory. if the bucket doesn't exist, than throw IllegalArgumentException.
   * @param bucket the bucket to visit.
   * @param obj the object in the bucket to visit.
   */
  def visit(bucket: String,
            obj: String,
            f: Item => Unit): Unit = {
    if (minioClient.bucketExists(bucket)) {
      minioClient.listObjects(bucket, obj)
        .asScala
        .map(result => {
          val item = result.get()
          if (item.isDir) {
            visit(bucket, item.objectName(), f)
          } else {
            f(item)
          }
        })
    } else {
      throw new IllegalArgumentException(s"unknown bucket ${bucket}.")
    }

  }

}

case class S3ServerInfo(
    endpoint: String,
    accessKeyId: String,
    secretAccessKey: String)

object MLFlowMinioUtilImpl extends MinioUtil {
  override def serverInfo: S3ServerInfo = S3ServerInfo(
    sys.env.getOrElse("MLFLOW_S3_ENDPOINT_URL", throw new RuntimeException()),
    sys.env.getOrElse("AWS_ACCESS_KEY_ID", throw new RuntimeException()),
    sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", throw new RuntimeException())
  )

  // normalize the sub object path in the root obj. drop the experiment id in the path head.
  // e.g.
  // /0/a063487ee34e463baf7101d145b96bb7/artifacts => /a063487ee34e463baf7101d145b96bb7/artifacts
  val _normalizePath = {
    (absolutePathInBucket: String) =>
      absolutePathInBucket.split("/") match {
        case Array(_, _, normalized @ _*) => normalized.mkString("/", "/", "")
      }
  }

  override def downloadAsZip(bucket: String,
                             obj: String,
                             localFilePath: String,
                             normalizePath: String => String = _normalizePath): Unit = {
    super.downloadAsZip(bucket, obj, localFilePath, normalizePath)
  }
}

object MinioUtilTest extends MinioUtil {
  override def serverInfo: S3ServerInfo = S3ServerInfo(
    "http://192.168.218.59:9000",
    "AKIAIOSFODNN7EXAMPLE",
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  )

  def main(args: Array[String]): Unit = {
    val a = "s3://test/0/a063487ee34e463baf7101d145b96bb7/artifacts"
    val uri = URI.create(a)
  }
}
