package org.apache.spark.panda.utils;

/**
 * @author fchen <cloud.chenfu@gmail.com>
 * @time 2019-08-30 17:52
 */

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;

public class GZIPUtil {

  public static void createTarArchive(String parentDir, String outFile){
    TarArchiveOutputStream tarArchive = null;
    try {
      File root = new File(parentDir);
      // create output name for tar archive
//      FileOutputStream fos = new FileOutputStream(root.getAbsolutePath().concat(".tar.gz"));
      FileOutputStream fos = new FileOutputStream(outFile);
      GZIPOutputStream gzipOS = new GZIPOutputStream(new BufferedOutputStream(fos));
      tarArchive = new TarArchiveOutputStream(gzipOS);
      tarArchive.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      addToArchive(parentDir, "", tarArchive);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }finally{
      try {
        tarArchive.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public static void addToArchive(String filePath, String parent, TarArchiveOutputStream tarArchive) throws IOException {
    File file = new File(filePath);
    // Create entry name relative to parent file path
    //for the archived file
    String entryName = parent + file.getName();
    System.out.println("entryName " + entryName);
    // add tar ArchiveEntry
    tarArchive.putArchiveEntry(new TarArchiveEntry(file, entryName));
    if(file.isFile()){
      FileInputStream fis = new FileInputStream(file);
      BufferedInputStream bis = new BufferedInputStream(fis);
      // Write file content to archive
      IOUtils.copy(bis, tarArchive);
      tarArchive.closeArchiveEntry();
      bis.close();
    }else if(file.isDirectory()){
      // no content to copy so close archive entry
      tarArchive.closeArchiveEntry();
      // if this directory contains more directories and files
      // traverse and archive them
      for(File f : file.listFiles()){
        // recursive call
        addToArchive(f.getAbsolutePath(), entryName+File.separator, tarArchive);
      }
    }
  }
}
