package org.apache.spark.panda.utils

import java.util.{ArrayList => JArrayList, Map => JMap}

import org.yaml.snakeyaml.Yaml

/**
 * @time 2019-09-16 15:47
 * @author fchen <cloud.chenfu@gmail.com>
 */
object MLFlow {

  def env(content: String): Unit = {
//    val yaml = new Yaml()
//    val mlmodel = yaml.load[JMap[String, Object]](content)
//    val flavors = mlmodel.get("flavors").asInstanceOf[JMap[String, Object]]
//    println(flavors.get("python_function").getClass)
//    println(mlmodel.get("flavors").getClass)
    val mlmodel = new MLmodelParser(content)
  }

  def main(args: Array[String]): Unit = {
    val yaml =
      """
        |artifact_path: model
        |flavors:
        |  python_function:
        |    data: model.pkl
        |    env: conda.yaml
        |    loader_module: mlflow.sklearn
        |    python_version: 3.6.9
        |  sklearn:
        |    pickled_model: model.pkl
        |    serialization_format: cloudpickle
        |    sklearn_version: 0.19.1
        |run_id: 9c6c59d0f57f40dfbbded01816896687
        |utc_time_created: '2019-08-21 06:37:27.408296'
      """.stripMargin

    env(yaml)
  }

}

/**
 * a MLmodel file parser.
 * here is a MLmodel example:
 * --------------------------------------------------------
 *  artifact_path: model
 *  flavors:
 *    python_function:
 *      data: model.pkl
 *      env: conda.yaml
 *      loader_module: mlflow.sklearn
 *      python_version: 3.6.9
 *    sklearn:
 *      pickled_model: model.pkl
 *      serialization_format: cloudpickle
 *      sklearn_version: 0.19.1
 *  run_id: 9c6c59d0f57f40dfbbded01816896687
 *  utc_time_created: '2019-08-21 06:37:27.408296'
 * --------------------------------------------------------
 * @param content
 */
class MLmodelParser(content: String) {

  val mlmodel = {
    new Yaml().load[JMap[String, Object]](content)
  }

  val artifactPath = typed[String](mlmodel.get("artifact_path"))

  val flavors = typed[JMap[String, Object]](mlmodel.get("flavors"))

  val pythonFunction = typed[JMap[String, Object]](flavors.get("python_function"))

  val env = typed[String](pythonFunction.get("env"))

  def typed[T](obj: Any): T = {
    obj.asInstanceOf[T]
  }

}
