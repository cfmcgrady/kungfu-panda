package org.panda.bamboo.util

import java.nio.file.{Path, Paths}
import java.util.{Map => JMap}
import java.util.concurrent.locks.ReentrantLock

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.juli.logging.LogFactory
import org.apache.spark.panda.utils.Conda

/**
 * @time 2019-08-29 14:33
 * @author fchen <cloud.chenfu@gmail.com>
 */
object CacheManager {

  private val logger = LogFactory.getLog(getClass)

  private val _lock = new ReentrantLock()
  // todo:(fchen) 基于文件锁来实现
  private lazy val _cache = CacheBuilder.newBuilder()
    .maximumSize(100000)
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

  def get: (Key) => String = {
    withLock {
      key => _cache.get(key).get()
    }
//    case k: CacheKey =>
//      _pythonEnvCache.get(k).get()
//    case k: MLFlowRunCacheKey =>
//      _mlflowRunCache.get(k).get()
  }

  def remove: (Key) => Unit = {
    withLock {
      key =>
        logger.info(s"start to remove ${key}")
        _cache.get(key).remove
        _cache.invalidate(key)
    }
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

  private def withLock[T](f: T): T = {
    _lock.lock()
    try {
      f
    } finally {
      _lock.unlock()
    }
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

