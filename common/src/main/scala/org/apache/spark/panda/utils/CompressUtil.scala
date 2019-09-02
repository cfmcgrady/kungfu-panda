package org.apache.spark.panda.utils

import java.io.{BufferedInputStream, File, FileOutputStream, IOException}
import java.nio.file.{Files, FileSystems, Path, Paths}
import java.util.zip.{ZipEntry, ZipFile, ZipOutputStream}

import scala.sys.process.Process

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream


/**
 * @time 2019-08-23 12:15
 * @author fchen <cloud.chenfu@gmail.com>
 */
object CompressUtil {

  def zip(sourceDirectory: String, targetZipFile: String): Unit = {
    val p = Files.createFile(Paths.get(targetZipFile))
    val zs = new ZipOutputStream(Files.newOutputStream(p))
    try {
      val sourceDirectoryPath = Paths.get(sourceDirectory)
      Util.recursiveListFiles(new File(sourceDirectory))
        .filter(!_.isDirectory)
        .foreach {
          case file =>
            val path = file.toPath
            val zipEntry = new ZipEntry(sourceDirectoryPath.relativize(path).toString())
            zs.putNextEntry(zipEntry)
            Files.copy(path, zs)
            zs.closeEntry()
        }
    } finally {
      zs.flush()
      zs.close()
    }
  }

  def tar(sourceDirectory: String,
          targetTarFile: String,
          action: (Path, TarArchiveEntry) => Unit = (_, _) => Unit): Unit = {
//    throw new RuntimeException("something wrong with this function.")
    val p = Files.createFile(Paths.get(targetTarFile))
    val gzipOS = new GzipCompressorOutputStream(Files.newOutputStream(p))
//    val taos = new TarArchiveOutputStream(Files.newOutputStream(p))
    val taos = new TarArchiveOutputStream(gzipOS)
    taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)
    try {
      val sourceDirectoryPath = Paths.get(sourceDirectory)
      Util.recursiveListFiles(new File(sourceDirectory))
        .filter(!_.isDirectory)
        .sortBy(x => x.toString)
        .foreach {
          case file =>
            val path = file.toPath
            val tarEntry = new TarArchiveEntry(path.toFile, sourceDirectoryPath.getParent.relativize(path).toString())
            action(path, tarEntry)
            taos.putArchiveEntry(tarEntry)
//            val in = new FileInputStream(path.toFile)
            Files.copy(path, taos)
//            IOUtils.copy(in, taos)
            taos.closeArchiveEntry()
        }
    } catch {
      case e: Exception =>
        throw e
    }
    finally {
      taos.flush()
      taos.close()
      gzipOS.flush()
      gzipOS.close()
    }
  }

  def tar2(sourceDirectory: String, targetTarFile: String): Unit = {
    GZIPUtil.createTarArchive(sourceDirectory, targetTarFile)
  }

  def tar3(): Unit = {
    Process(
      "tar czf "
    )
  }

  @throws(classOf[IOException])
  def unzip(sourceZipFile: String,
            uncompressedDirectory: String,
            forceReplace: Boolean): Unit = {
    val zfile = new ZipFile(sourceZipFile)
    try {
      val fs = FileSystems.getDefault
      val entries = zfile.entries()
      // unzip files in the {{targetDirectory}} forder
      if (forceReplace) {
        val p = fs.getPath(uncompressedDirectory)
        if (Files.exists(p)) {
          //          Files.deleteIfExists(p)
          val filesInTarget = Util.recursiveListFiles(new File(p.toString))
          filesInTarget.foreach(f => Files.delete(f.toPath))
          Files.delete(p)
        }
      }
      Files.createDirectory(fs.getPath(uncompressedDirectory))
      // iterate over entries
      while (entries.hasMoreElements) {
        val entry = entries.nextElement()
        // If directory then create a new directory in uncompressed folder
        if (entry.isDirectory) {
          // scalastyle:off println
          println("Creating Directory:" + uncompressedDirectory + entry.getName())
          // scalastyle:on
          Files.createDirectories(fs.getPath(uncompressedDirectory + entry.getName()))
        } else {
          // we got a file entry.
          val path = Paths.get(entry.getName)
          if (path.getParent != null) {
            // this file has a parent directory. we should create parent directory first.
            Files.createDirectories(
              fs.getPath(uncompressedDirectory + File.separator + path.getParent))
          }
          val is = zfile.getInputStream(entry)
          val bis = new BufferedInputStream(is)
          val uncompressedFileName = uncompressedDirectory + File.separator + entry.getName()
          val uncompressedFilePath = fs.getPath(uncompressedFileName)
          Files.createFile(uncompressedFilePath)
          val fileOutput = new FileOutputStream(uncompressedFileName)
          try {
            while (bis.available() > 0) {
              fileOutput.write(bis.read())
            }
            fileOutput.close()
          } finally {
            fileOutput.close()
            bis.close()
            is.close()
          }
          // scalastyle:off println
          println("Written :" + entry.getName())
          // scalastyle:on
        }
      }
    } finally {
      zfile.close()
    }
  }
}
