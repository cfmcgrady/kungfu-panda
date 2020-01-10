package org.apache.spark.sql.catalyst.rules

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.python.{ArrowEvalPython, KFArrowEvalPython, KFArrowEvalPythonExec, OneRowRelationWithSchema}

/**
 * @time 2020/1/9 1:32 下午
 * @author fchen <cloud.chenfu@gmail.com>
 */

class ChangePythonUDFStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
        // todo 把project去掉
//      case OneRowRelationWithSchema(output, data) =>
//        val singleRowRdd = SparkSession.active
//          .sparkContext
////          .parallelize(data, 1)
//          .parallelize(Seq(InternalRow()), 1)
//        RDDScanExec(output, singleRowRdd, "OneRowRelation") :: Nil
      case proj@ Project(_, child: OneRowRelationWithSchema) =>
        val ids = proj.output.map(_.exprId)
        val data = child.data.filter{
          case (id, _) => ids.contains(id)
        }.map {
          case (_, value) => value
        }
        val singleRowRdd = SparkSession.active
          .sparkContext
          .parallelize(Seq(InternalRow(data: _*)), 1)
        RDDScanExec(proj.output, singleRowRdd, "OneRowRelation") :: Nil
      case ArrowEvalPython(udfs, output, child) =>
        KFArrowEvalPythonExec(udfs, null, output, planLater(child)) :: Nil
      case _ =>
        Nil
    }
  }
}
