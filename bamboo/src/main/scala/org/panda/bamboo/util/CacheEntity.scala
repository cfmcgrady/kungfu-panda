package org.panda.bamboo.util

import java.io.{File, IOException}
import java.net.{URI, URISyntaxException}
import java.nio.file.{Files, Path, Paths}
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
  private val logger = LogFactory.getLog(getClass)

  private lazy val ARTIFACT_ROOT = new URI(sys.env.getOrElse("MLFLOW_ARTIFACT_ROOT", throw new RuntimeException("")))

  private lazy val BASE_PATH = "/tmp/runs"

  override protected def write: Unit = {

    // make sure this run was not downloaded before.
    if (new File(compressFilePath).getParentFile.exists()) {
      return
    }
    logger.info(s"start to download run ${runid} from artifact ${ARTIFACT_ROOT}")

    val path = ARTIFACT_ROOT.getScheme.toLowerCase match {
      case "file" =>
        Util.getArtifactByRunId(ARTIFACT_ROOT.getPath, runid)
      case "sftp" =>
        // TODO:(fchen) we should look the remote path from mlflow tracking server api.
        val remotePath = ARTIFACT_ROOT.getPath + "/0/" + runid
        val localPath = s"${BASE_PATH}/$runid/${runid}"

        // make sure the `BASE_PATH` exist. otherwise we should create the directory manually.
        Util.mkdir(localPath)

        SFTPUtil.download(ARTIFACT_ROOT.getHost, remotePath, localPath)
        localPath
      case _ =>
        throw new UnsupportedOperationException()
    }
    CompressUtil.tar2(path, compressFilePath)
  }

  def compressFilePath: String = {
    s"${BASE_PATH}/${runid}/${runid}.tgz"
  }

  override protected def read: String = compressFilePath

  override def delete: Unit = {
    try {
      Util.recursiveListFiles(Paths.get(s"${BASE_PATH}/${runid}").toFile)
          .foreach(f => {
            Files.deleteIfExists(f.toPath)
          })
      Files.deleteIfExists(Paths.get(s"${BASE_PATH}/${runid}"))
    } catch {
      case e: IOException =>
        logger.info(s"remove run $runid failed!", e)
    }
  }

  private def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

}
