package org.apache.spark.panda.utils

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream, InputStream, IOException}
import java.util.zip.GZIPOutputStream

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.io.IOUtils
import org.apache.commons.logging.LogFactory

/**
 * @time 2019/12/10 上午10:19
 * @author fchen <cloud.chenfu@gmail.com>
 */
object GZIPUtilV2 {

  private val logger = LogFactory.getLog(getClass)

  def createTarArchive(parentDir: String, outFile: String): Unit = {
    var tarArchive: TarArchiveOutputStream = null
    try {
      val fos = new FileOutputStream(outFile)
      val gzipOS = new GZIPOutputStream(new BufferedOutputStream(fos))
      tarArchive = new TarArchiveOutputStream(gzipOS)
      tarArchive.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)
      addToArchive(parentDir, "", tarArchive)

    } finally {
      tarArchive.finish()
      tarArchive.close()
    }
  }

  @throws(classOf[IOException])
  def addToArchive(filePath: String,
                   parent: String,
                   tarArchive: TarArchiveOutputStream): Unit = {
    val file = new File(filePath);
    // Create entry name relative to parent file path
    // for the archived file
    val entryName = parent + file.getName()
    // scalastyle:off println
    println("entryName " + entryName)
    // scalastyle:on
    // add tar ArchiveEntry

    //    tarEntry.setMode(755)
    val tae = new TarArchiveEntry(file, entryName)
    tae.setSize(file.length())
    tarArchive.putArchiveEntry(tae)
    if (file.isFile()) {
      val fis = new FileInputStream(file)
      val bis = new BufferedInputStream(fis)
      //      // Write file content to archive
      //      IOUtils.copy(bis, tarArchive);
      org.apache.commons.io.IOUtils.copyLarge(bis, tarArchive)
      //      Files.copy(file.toPath(), tarArchive);
      tarArchive.closeArchiveEntry()
      //      bis.close();
    } else if (file.isDirectory()) {
      // no content to copy so close archive entry
      tarArchive.closeArchiveEntry()
      // if this directory contains more directories and files
      // traverse and archive them
      file.listFiles().foreach(f => addToArchive(f.getAbsolutePath, entryName + File.separator, tarArchive))
    }
  }

  def streamCreateTarArchive(outFile: String)(f: TarArchiveOutputStream => Unit): Unit = {
    var tarArchive: TarArchiveOutputStream = null
    val fos = new FileOutputStream(outFile)
    val gzipOS = new GZIPOutputStream(new BufferedOutputStream(fos))
    try {
      tarArchive = new TarArchiveOutputStream(gzipOS)
      tarArchive.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)
      f(tarArchive)
    } finally {
      tarArchive.finish()
      tarArchive.close()
      gzipOS.flush()
      gzipOS.close()
      fos.flush()
      fos.close()
    }
  }

  @throws(classOf[IOException])
  def streamAddToArchive(in: InputStream,
                         size: Long,
                         pathInArchive: String,
                         tarArchive: TarArchiveOutputStream): Unit = {

    logger.info(s"add entry ${pathInArchive} to archive.")
    // add tar ArchiveEntry

    //    tarEntry.setMode(755)
    val tae = new TarArchiveEntry(pathInArchive)
    tae.setSize(size)
    tarArchive.putArchiveEntry(tae)
    //      // Write file content to archive
    try {
//      IOUtils.copy(in, tarArchive);
      org.apache.commons.io.IOUtils.copyLarge(in, tarArchive)
    } finally {
      //      Files.copy(file.toPath(), tarArchive);
      tarArchive.closeArchiveEntry()
    }
  }

}
