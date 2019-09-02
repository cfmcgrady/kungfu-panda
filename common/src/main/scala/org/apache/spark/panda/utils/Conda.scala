package org.apache.spark.panda.utils

import java.io.{File, FileWriter}
import java.nio.file.{Files, Path, Paths}
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

    val yamlPath = Paths.get(basePath, Array(s"${name}.yaml"): _*)
    // set proxy before we write yaml configuration file to disk.
    writeYaml(yaml, yamlPath)
    val envPath = Paths.get(basePath + File.separator + name)

    // make sure envPath not exists!
    if (Files.exists(envPath)) {
      Files.delete(envPath)
    }

    try {
      val cmd = s"conda env create -f ${yamlPath.toString} -p ${envPath}"
      // scalastyle:off println
      val logger = ProcessLogger(println, println)
      // scalastyle:on
      Process(
        cmd
      ).!!(logger)
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

    val nname = Util.stringToMD5(total.sortBy(x => x).mkString(","))
    info.put("name", nname)
    info

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
  def setProxy(conf: JMap[String, Object]): JMap[String, Object] = {
    // TODO:(fchen) set conda proxy.
    conf
  }

  def addPyarrow(configurations: JMap[String, Object]): Unit = {
//    pyarrow==0.12.1
  }

}

case class PythonPackage(module: String, version: String = "unk")
