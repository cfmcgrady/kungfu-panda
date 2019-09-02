package org.panda.bamboo.util

import java.io.File
import java.nio.file.{Path, Paths}
import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.panda.utils.CompressUtil

/**
 * @time 2019-08-29 14:33
 * @author fchen <cloud.chenfu@gmail.com>
 */
object CacheManager {

  private val _cache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[CacheKey, CacheEntity] {
        override def load(k: CacheKey): CacheEntity = {
          new CacheEntity(k.name, k.conf)
        }
      }
    )

  def get(yaml: String): String = {
    // TODO:(fchen) vilidate the yaml is in legal format.
    val ymap = Conda.normalize(yaml)
    _cache.get(
      CacheKey(ymap.getOrDefault("name", "").asInstanceOf[String], ymap)
    ).get()
  }

  /**
   * get the tar archive file path by environment name.
   */
  def getFileByName(name: String): Path = {
    Paths.get(basePath, Array(name, s"${name}.tgz"): _*)
  }

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

    (1 to 1000).foreach(i => {
      new Thread(new Runnable {
        override def run(): Unit = {
          get(yaml)
        }
      }).start()
    })
  }
}

case class CacheKey(name: String, conf: JMap[String, Object]) {
  override def hashCode(): Int = {
    name.hashCode
  }
  override def equals(obj: Any): Boolean = {
    obj match {
      case that: CacheKey =>
        that.name == name
      case _ =>
        false
    }
  }
}

class CacheEntity(name: String,
                  configuration: JMap[String, Object]) {

  private val _lock = new ReentrantReadWriteLock()
  private val _cacheVaild: AtomicBoolean = new AtomicBoolean(false)

  def get(): String = {

    _lock.readLock().lock()
    if (!_cacheVaild.get()) {
      _lock.readLock().unlock()
      _lock.writeLock().lock()
      try {
        if (!_cacheVaild.get()) {
          // do package download
          // TODO:(fchen) throws execption when we has downloaded fail.
          downloadAndPackage()
          _cacheVaild.set(true)
        }
        _lock.readLock().lock()
      } finally {
        _lock.writeLock().unlock()
      }
    }
    try {
      // read data
      name
    } finally {
      _lock.readLock().unlock()
    }
  }

  private def downloadAndPackage(): Unit = {
    import CacheManager._
    if (!(Paths.get(basePath, Array(name): _*).toFile.exists() &&
        Paths.get(basePath, Array(name, s"${name}.tgz"): _*).toFile.exists())) {
      // the environment has never been download before, so we download this package now.
      val envpath = Conda.createEnv(name, configuration, basePath + File.separator + name)

      // compress environment
      CompressUtil.tar2(envpath.toString, s"${envpath}.tgz")
    }
  }

  /**
   * .
   * └── name
   *     ├── name.tgz
   *     ├── env
   *     └── meta
   */
}
