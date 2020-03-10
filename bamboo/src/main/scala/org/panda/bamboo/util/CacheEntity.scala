package org.panda.bamboo.util

import java.io.{File, IOException}
import java.net.{URI, URISyntaxException}
import java.nio.file.{Files, Path, Paths}
import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.panda.utils.{CompressUtil, Conda, MLFlowMinioUtilImpl, MLFlowUtil, SFTPUtil, Util}
import org.panda.Config

/**
 * @time 2019-09-12 16:48
 * @author fchen <cloud.chenfu@gmail.com>
 */
trait CacheEntity[T] {

  private val _lock = new ReentrantReadWriteLock()
  private val _cacheValid: AtomicBoolean = new AtomicBoolean(false)

  protected def write: Unit

  protected def read: T

  def delete: Unit

  def remove: Unit = {
    _lock.writeLock().lock()
    try {
      delete
        _cacheValid.set(false)
    } catch {
      case t: Throwable => throw t
    } finally {
      _lock.writeLock().unlock()
    }
  }

  def get(): T = {

    _lock.readLock().lock()
    if (!_cacheValid.get()) {
      _lock.readLock().unlock()
      _lock.writeLock().lock()
      try {
        if (!_cacheValid.get()) {
          // do package download
          // TODO:(fchen) throws exception when we has downloaded fail.
          write
          _cacheValid.set(true)
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

  import CacheManager._

  val logger = LogFactory.getLog(this.getClass)

  val resolvedEnvRootPath: Path = PythonEnvironmentResolvedPath.resolveEnvPath(name)

  private def downloadAndPackage(): Unit = {
    if (!resolvedEnvRootPath.toFile.exists() &&
      PythonEnvironmentResolvedPath.compressFilePath(name).toFile.exists()) {
      logger.info("env not found, begin download from internet.")
      // the environment has never been download before, so we download this package now.
      val envpath = Conda.createEnv(name, configuration, resolvedEnvRootPath.toString)

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

  override def delete: Unit = {
    FileUtils.deleteDirectory(PythonEnvironmentResolvedPath.resolveEnvPath(name).toFile)
//    Util.recursiveDeleteFile(envRootPath)
  }

  /**
   * .
   * └── name
   *     ├── name.tgz
   *     ├── env
   *     └── meta
   */
}

class MLFlowRunCacheEntity(runid: String) extends CacheEntity[String]
    with MLFlowUtil
    with ResolvedPath {

  private val logger = LogFactory.getLog(getClass)

  private lazy val MLFLOW_TRACKING_URI = sys.env.getOrElse("MLFLOW_TRACKING_URI",
    throw new IllegalArgumentException("please set MLFLOW_TRACKING_URI environment variable.")
  )

  override def mlflowTrackingUri: String = MLFLOW_TRACKING_URI

  logger.info(s"service start with MLFLOW_TRACKING_URI = ${mlflowTrackingUri}")

  private val resolvedRunPath = resolveRunPath(runid)

  private val resolvedCompressionPath = compressFilePath(runid)

  override protected def write: Unit = {

    // make sure this run was not downloaded before.
    if (new File(compressFilePath(runid)).getParentFile.exists()) {
      return
    }

    artifactUri(runid) match {
      case Right(uri) =>
        logger.info(s"start to download run ${runid} from artifact ${uri}")
        downloadAndCompress(new URI(uri))
      case Left(e) =>
        throw e
    }
  }

  def downloadAndCompress(uri: URI): Unit = {
    uri.getScheme.toLowerCase match {
      case "file" =>
        val path = Util.getArtifactByRunId(uri.getPath, runid)
        CompressUtil.tar2(path, resolvedCompressionPath)
      case "sftp" =>
        // TODO:(fchen) we should look the remote path from mlflow tracking server api.
        val remotePath = uri.getPath + "/0/" + runid
        // make sure the `BASE_PATH` exist. otherwise we should create the directory manually.
        Util.mkdir(resolvedRunPath)

        SFTPUtil.download(uri.getHost, remotePath, resolvedRunPath)
        CompressUtil.tar2(resolvedRunPath, resolvedCompressionPath)
      case "s3" =>
        // make sure the `BASE_PATH` exist. otherwise we should create the directory manually.
        Util.mkdir(resolvedRunPath)
        MLFlowMinioUtilImpl.downloadAsZip(uri.getHost, uri.getPath, resolvedCompressionPath)
      case _ =>
        throw new UnsupportedOperationException()
    }
  }

  override protected def read: String = compressFilePath(runid)

  override def delete: Unit = {
    Util.recursiveDeleteFile(resolvedRunPath)
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

trait ResolvedPath {
  self: MLFlowRunCacheEntity =>
  private lazy val BASE_PATH = sys.env.getOrElse("panda.cache.dir", s"${Config.CACHE_ROOT_DIR}/panda/runs")

  /**
   * the root cache path of this run.
   */
  protected val resolveRunPath = (runid: String) => s"${BASE_PATH}/${runid}"

  /**
   * the compressed file path of this run.
   */
  val compressFilePath = {
    runid: String => s"${resolveRunPath(runid)}/${runid}.tgz"
  }

}

object PythonEnvironmentResolvedPath {
  //  // TODO:(fchen) generate base path with server info(hostname: port).
  //  // so that we can deploy multi server on the same host.
  //  val basePath = Config.CACHE_ROOT_DIR
  private lazy val BASE_PATH = s"${Config.CACHE_ROOT_DIR}/conda"

  val resolveEnvPath = (name: String) => Paths.get(BASE_PATH, Array(name): _*)

  val compressFilePath = (name: String) => Paths.get(BASE_PATH, Array(name, s"${name}.tgz"): _*)

}
