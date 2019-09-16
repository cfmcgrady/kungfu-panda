package org.panda.bamboo.util

import java.nio.file.{Path, Paths}
import java.util.{Map => JMap}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.panda.utils.Conda

/**
 * @time 2019-08-29 14:33
 * @author fchen <cloud.chenfu@gmail.com>
 */
object CacheManager {

  private lazy val _pythonEnvCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[CacheKey, PythonEnvironmentCacheEntity] {
        override def load(k: CacheKey): PythonEnvironmentCacheEntity = {
          new PythonEnvironmentCacheEntity(k.name, k.conf)
        }
      }
    )

  private lazy val _mlflowRunCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[MLFlowRunCacheKey, MLFlowRunCacheEntity] {
        override def load(k: MLFlowRunCacheKey): MLFlowRunCacheEntity = {
          new MLFlowRunCacheEntity(k.runid)
        }
      }
    )

  private lazy val _cache = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .build(
      new CacheLoader[Key, CacheEntity[String]] {
        override def load(key: Key): CacheEntity[String] = {
          key match {
            case cacheKey: CacheKey =>
              new PythonEnvironmentCacheEntity(cacheKey.name, cacheKey.conf)
            case runKey: MLFlowRunCacheKey =>
              new MLFlowRunCacheEntity(runKey.runid)
          }
        }
      }
    )

//  def get(yaml: String): String = {
    // TODO:(fchen) vilidate the yaml is in legal format.
//    val ymap = Conda.normalize(yaml)
//    _pythonEnvCache.get(
//      CacheKey(ymap.getOrDefault("name", "").asInstanceOf[String], ymap)
//    ).get()
//  }

  def get: (Key) => String = {
    key => _cache.get(key).get()
//    case k: CacheKey =>
//      _pythonEnvCache.get(k).get()
//    case k: MLFlowRunCacheKey =>
//      _mlflowRunCache.get(k).get()
  }

  /**
   * get the tar archive file path by environment name.
   */
  def getFileByName(name: String): Path = {
    Paths.get(basePath, Array(name, s"${name}.tgz"): _*)
  }

  // TODO:(fchen) generate base path with server info(hostname: port).
  // so that we can deploy multi server on the same host.
  val basePath = "/tmp/cache"

  def main(args: Array[String]): Unit = {
    val yaml =
      """
        |channels:
        |  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/main/
        |  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/free/
        |  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/r/
        |  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/pro/
        |  - http://nexus.k8s.uc.host.dxy/repository/anaconda/pkgs/msys2/
        |dependencies:
        |- python=3.6.0
        |- numpy
        |name: conda-test
      """.stripMargin

    (1 to 10).foreach(i => {
      new Thread(new Runnable {
        override def run(): Unit = {
          val ymap = Conda.normalize(yaml)
          val k = CacheKey(ymap.getOrDefault("name", "").asInstanceOf[String], ymap)
          get(k)
        }
      }).start()
    })
  }
}

trait Key {
  protected def keyHashCode: Int
  protected def keyEquals(obj: Any): Boolean

  override def hashCode(): Int = {
    keyHashCode
  }

  override def equals(obj: Any): Boolean = {
    keyEquals(obj)
  }
}

case class CacheKey(name: String, conf: JMap[String, Object]) extends Key {

  override protected def keyHashCode: Int = name.hashCode

  override protected def keyEquals(obj: Any): Boolean = {
    obj match {
      case that: CacheKey =>
        that.name == name
      case _ =>
        false
    }
  }
}

case class MLFlowRunCacheKey(runid: String) extends Key {

  override protected def keyHashCode: Int = runid.hashCode

  override protected def keyEquals(obj: Any): Boolean = {
    obj match {
      case that: MLFlowRunCacheKey =>
        that.runid == runid
      case _ =>
        false
    }
  }
}

