package org.apache.spark.sql.catalyst.rules

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{ProjectExec, RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.python.{ArrowEvalPython, KFArrowEvalPython, KFArrowEvalPythonExec}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * @time 2020/1/9 1:32 下午
 * @author fchen <cloud.chenfu@gmail.com>
 */

class ChangePythonUDFStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case ArrowEvalPython(udfs, output, child) =>
        val inputSchema = {
          val is = findInputSchema2(udfs)
          if (is.size == 0) None else Option(StructType(is.distinct))
        }
        val s = findInputSchema(udfs)
        KFArrowEvalPythonExec(udfs, inputSchema, output, planLater(child)) :: Nil
      case _ =>
        Nil
    }
  }

  def findInputSchema(expressions: Seq[Expression]): Option[StructType] = {
    if (expressions == null || expressions.size == 0) {
      None
    } else if (expressions.head.children.forall(_.isInstanceOf[PythonUDF])) {
      findInputSchema(expressions.head.children)
    } else {
      if (expressions.head.children.forall(_.isInstanceOf[NamedExpression])) {
        Option(StructType(
          expressions.head.children.map(_.asInstanceOf[NamedExpression]).map(ne => {
            StructField(ne.name, ne.dataType)
          })
        ))
      } else {
        None
      }
    }

    Option(StructType(
      Seq(StructField("x", DataTypes.IntegerType),
        StructField("y", DataTypes.IntegerType)
      )
    ))
  }

  def findInputSchema2(expressions: Seq[Expression]): Seq[StructField] = {
    if (expressions == null || expressions.size == 0) {
      Seq.empty
    } else {
      expressions.flatMap(expression => {
        if (expression.children.forall(_.isInstanceOf[PythonUDF])) {
          findInputSchema2(expression.children)
        } else {
          if (expression.children.forall(_.isInstanceOf[NamedExpression])) {
            expression.children.map(_.asInstanceOf[NamedExpression]).map(ne => {
              StructField(ne.name, ne.dataType)
            })
          } else {
            Seq.empty[StructField]
          }
        }
      })
    }
  }
}
