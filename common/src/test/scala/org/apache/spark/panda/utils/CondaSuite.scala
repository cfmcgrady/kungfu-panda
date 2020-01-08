package org.apache.spark.panda.utils

import org.scalatest.FunSuite

/**
 * @time 2020/1/3 下午1:52
 * @author fchen <cloud.chenfu@gmail.com>
 */
class CondaSuite extends FunSuite {
  test("basic - the same libary in dependencies and pip should return different environment name.") {
    val yaml1 =
      """
        |channels:
        |- defaults
        |dependencies:
        |- python=3.7.4
        |- lightgbm==2.2.3
        |- pip:
        |  - mlflow
        |  - cloudpickle==1.2.2
        |name: mlflow-env
        |""".stripMargin

    val yaml2 =
      """
        |channels:
        |- defaults
        |dependencies:
        |- python=3.7.4
        |- pip:
        |  - mlflow
        |  - lightgbm==2.2.3
        |  - cloudpickle==1.2.2
        |name: mlflow-env
        |""".stripMargin

    val name1 = Conda.normalize(yaml1).get("name")
    val name2 = Conda.normalize(yaml2).get("name")
    assert(name1 != name2)

    // case 2
    val yaml3 =
      """
        |dependencies:
        |- python=3.7.4
        |""".stripMargin
    val yaml4 =
      """
        |dependencies:
        |- pip:
        |  - python=3.7.4
        |""".stripMargin
    val name3 = Conda.normalize(yaml3, false).get("name")
    val name4 = Conda.normalize(yaml4, false).get("name")
    assert(name3 != name4)
  }

}
