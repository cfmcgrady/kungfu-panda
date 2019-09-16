package org.panda.bamboo.util

import java.io.File
import java.net.URI
import java.nio.file.{Path, Paths}
import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.logging.LogFactory
import org.apache.spark.panda.utils.{CompressUtil, Conda, SFTPUtil, Util}

/**
 * @time 2019-09-12 16:48
 * @author fchen <cloud.chenfu@gmail.com>
 */
trait CacheEntity[T] {

  private val _lock = new ReentrantReadWriteLock()
  private val _cacheVaild: AtomicBoolean = new AtomicBoolean(false)

  protected def write: Unit

  protected def read: T

  def delete: Unit

  def remove: Unit = {
    _lock.writeLock().lock()
    try {
      delete
        _cacheVaild.set(false)
    } finally {
      _lock.writeLock().unlock()
    }
  }

  def get(): T = {

    _lock.readLock().lock()
    if (!_cacheVaild.get()) {
      _lock.readLock().unlock()
      _lock.writeLock().lock()
      try {
        if (!_cacheVaild.get()) {
          // do package download
          // TODO:(fchen) throws execption when we has downloaded fail.
          write
          _cacheVaild.set(true)
        }
        _lock.readLock().lock()
      } finally {
        _lock.writeLock().unlock()
      }
    }
    try {
      // read data
      read
    } finally {
      _lock.readLock().unlock()
    }
  }
}

class PythonEnvironmentCacheEntity (
     name: String,
     configuration: JMap[String, Object]) extends CacheEntity[String] {

  val logger = LogFactory.getLog(this.getClass)

  private def downloadAndPackage(): Unit = {
    import CacheManager._
    if (!(Paths.get(basePath, Array(name): _*).toFile.exists() &&
      Paths.get(basePath, Array(name, s"${name}.tgz"): _*).toFile.exists())) {
      logger.info("env not found, begin download from internet.")
      // the environment has never been download before, so we download this package now.
      val envpath = Conda.createEnv(name, configuration, basePath + File.separator + name)

      // make python command executable.
      val makeExecutable = {
        (path: Path, entry: TarArchiveEntry) => {
          if (path.getFileName.toString.equalsIgnoreCase("python")) {
            entry.setMode(755)
          }
        }
      }
      CompressUtil.tar(envpath.toString, s"${envpath}.tgz", makeExecutable)
    }
  }

  override protected def write: Unit = {
    downloadAndPackage()
  }

  override protected def read: String = name

  override def delete: Unit = throw new UnsupportedOperationException("")

  /**
   * .
   * └── name
   *     ├── name.tgz
   *     ├── env
   *     └── meta
   */
}

class MLFlowRunCacheEntity(runid: String) extends CacheEntity[String] {

  private lazy val ARTIFACT_ROOT = new URI(sys.env.getOrElse("MLFLOW_ARTIFACT_ROOT", throw new RuntimeException("")))

  override protected def write: Unit = {

    val path = ARTIFACT_ROOT.getScheme.toLowerCase match {
      case "file" =>
        Util.getArtifactByRunId(ARTIFACT_ROOT.toString, runid)
      case "sftp" =>
        // TODO:(fchen) we should look the remote path from mlflow tracking server api.
        val remotePath = ARTIFACT_ROOT.getPath + "/0/" + runid
        SFTPUtil.download(ARTIFACT_ROOT.getHost, remotePath, "/tmp/runs")
        s"/tmp/runs/$runid"
      case _ =>
        throw new UnsupportedOperationException()
    }
    CompressUtil.tar(path, s"${path}.tgz")
  }

  override protected def read: String = runid

  override def delete: Unit = throw new UnsupportedOperationException()

}
