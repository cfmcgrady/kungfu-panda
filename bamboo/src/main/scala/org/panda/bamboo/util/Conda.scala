package org.panda.bamboo.util

import java.io.{File, FileOutputStream, FileWriter, PrintWriter, StringWriter}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.{ArrayList => JArrayList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.sys.process.{Process, ProcessLogger}

import org.yaml.snakeyaml.Yaml

/**
 * @time 2019-08-29 12:06
 * @author fchen <cloud.chenfu@gmail.com>
 */
object Conda {


  def createEnv(name: String,
                yaml: JMap[String, Object],
                basePath: String): Path = {

//    val tmpFile = s"/tmp/${stringToMD5(yaml)}.yaml"
//    new PrintWriter(tmpFile) { write(yaml); close }

    val yamlPath = Paths.get(basePath, Array(s"${name}.yaml"): _*)
    writeYaml(yaml, yamlPath)
    val envPath = Paths.get(basePath + File.separator + name)

    // make sure envPath not exsit!
    if (Files.exists(envPath)) {
      Files.delete(envPath)
    }

    try {
      val cmd = s"conda env create -f ${yamlPath.toString} -p ${envPath}"
      // scalastyle:off println
      val logger = ProcessLogger(println, println)
      Process(
        cmd
      ).!!(logger)
      // scalastyle:on
    } catch {
      case e: RuntimeException =>
        e.printStackTrace()
    }
    envPath
  }

  def normalize(configurations: String): JMap[String, Object] = {
    val yaml = new Yaml()
    val info = yaml.load[JMap[String, Object]](configurations)
    val dependencies = info.get("dependencies")

    val (dep, pip) = extract(dependencies.asInstanceOf[JArrayList[Object]].asScala)

    val total = dep ++ pip

    val nname = stringToMD5(total.sortBy(x => x).mkString(","))
    info.put("name", nname)
    info
//    yaml.dump(info, new FileWriter(new File(s"/tmp/${nname}.yaml")))

  }

  /**
   * extract conda.yaml `dependencies` and `pip` from given configurations.
   */
  def extract(conf: Buffer[Object]): (Buffer[String], Buffer[String]) = {
    val dependencies = conf.collect {case s: String => s}

    val pip = conf.collect {
      case map: java.util.LinkedHashMap[_, _] => map
    }.headOption
      .map( pip => {
        pip.get("pip")
          .asInstanceOf[JArrayList[Object]]
          .asScala
          .collect {case s: String => s}
      }).getOrElse(Buffer.empty)
    (dependencies, pip)
  }

  def main(args: Array[String]): Unit = {
//    println(MurmurHash3.stringHash("aaa"))
//    println(new String(MessageDigest.getInstance("MD5").digest("aaa".getBytes("UTF-8"))))
//    println(stringToMD5("aaa"))
//    println(stringToMD5("1234"))
//    println(stringToMD5("bbb"))
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

    val y2 =
      """
        |name: tutorial
        |channels:
        |  - defaults
        |dependencies:
        |  - python=3.6
        |  - scikit-learn=0.19.1
        |  - pip:
        |    - mlflow>=1.0
        |    - pyspark==2.4.3
      """.stripMargin
    normalize(yaml)
//    createEnv(yaml)
  }

  def stringToMD5(string: String): String = {
    MessageDigest.getInstance("MD5")
      .digest(string.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString
  }

  private def writeYaml(conf: JMap[String, Object],
                        filePath: Path): Unit = {
    val yaml = new Yaml()

    val p = filePath.getParent
    if (!p.toFile.exists()) {
      Files.createDirectories(p)
    }

    yaml.dump(conf, new FileWriter(filePath.toFile))
  }

  /**
   * set conda download proxy.
   * @param conf
   */
  def setProxy(conf: JMap[String, Object]): Unit = {

  }

}

case class PythonPackage(module: String, version: String = "unk")
