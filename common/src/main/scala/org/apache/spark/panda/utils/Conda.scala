package org.apache.spark.panda.utils

import java.io.{File, FileWriter}
import java.nio.file.{Files, Path, Paths}
import java.util.{ArrayList => JArrayList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.sys.process.{Process, ProcessLogger}

import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml

/**
 * @time 2019-08-29 12:06
 * @author fchen <cloud.chenfu@gmail.com>
 */
object Conda {

  val logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  private val CONDA_COMMAND = sys.env.getOrElse("CONDA_PATH", "conda")

  logger.info(s"using conda path = ${CONDA_COMMAND}.")

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
      val cmd = s"${CONDA_COMMAND} env create -f ${yamlPath.toString} -p ${envPath}"
      logger.info(s"running command [ ${cmd} ].")
      // scalastyle:off println
      val processLogger = ProcessLogger(logger.info, logger.info)
      // scalastyle:on
      Process(
        cmd
      ).!!(processLogger)
    } catch {
      case e: RuntimeException =>
        e.printStackTrace()
    }
    logger.info(s"finished create environment $name.")
    envPath
  }

  def normalize(configurations: String, withPyarrow: Boolean = true): JMap[String, Object] = {
    val yaml = new Yaml()
    val info = yaml.load[JMap[String, Object]](configurations)
    val dependencies = info.get("dependencies")

    if (withPyarrow) {
      // add pyarrow dependency for pyspark runtime.
      addPyarrow(dependencies.asInstanceOf[JArrayList[Object]])
    }

    val (dep, pip) = extract(dependencies.asInstanceOf[JArrayList[Object]].asScala)
    val nname = generateEnvironmentName(dep, pip)
    info.put("name", nname)

    // remove user define channels.
    info.remove("channels")

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

  def addPyarrow(pip: Buffer[String]): Buffer[String] = {
    pip.filter(!_.startsWith("pyarrow")) += PYARROW
  }

  def addPyarrow(dependencies: JArrayList[Object]): Unit = {
    dependencies.asScala
      .collect { case map: java.util.LinkedHashMap[_, _] => map}
      .headOption
      .orElse {
        val pip = new java.util.LinkedHashMap[Object, Object]()
        pip.put("pip", new JArrayList[Object]())
        dependencies.add(pip)
        Option(pip)
      }.foreach(pip => {
        pip.get("pip")
          .asInstanceOf[JArrayList[Object]]
          .add(PYARROW)
      })
  }

  // todo: (fchen) read from system configurations.
  val PYARROW = "pyarrow==0.12.1"

  private def generateEnvironmentName(dependencies: Buffer[String], pip: Buffer[String]): String = {
    // We should sort the package first so that we can generate the unique id for the same Conda environment,
    // in which the environment dependencies have a shuffled order.
    Util.stringToMD5(
      dependencies.sortBy(o => o).mkString("dependencies: [", ",", "]") +
        pip.sortBy(o => o).mkString("pip: [", ",", "]")
    )
  }

}

case class PythonPackage(module: String, version: String = "unk")
