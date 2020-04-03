package org.apache.spark.sql.execution.python

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LeafNode, LocalRelation, LogicalPlan, OneRowRelation, Project, Statistics, SubqueryAlias, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.arrow.ArrowUtils
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * @time 2020/1/9 1:18 下午
 * @author fchen <cloud.chenfu@gmail.com>
 *
 * copy from spark.
 */
abstract class KFEvalPythonExec(udfs: Seq[PythonUDF],
                                inputSchema: Option[StructType],
                                output: Seq[Attribute],
                                child: SparkPlan) extends EvalPythonExec(udfs, output, child) {
  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())

    inputRDD.mapPartitions { iter =>
      val context = TaskContext.get()

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), child.output.length)
      context.addTaskCompletionListener[Unit] { ctx =>
        queue.close()
      }

      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argOffsets = inputs.map { input =>
        input.map { e =>
          if (allInputs.exists(_.semanticEquals(e))) {
            allInputs.indexWhere(_.semanticEquals(e))
          } else {
            allInputs += e
            dataTypes += e.dataType
            allInputs.length - 1
          }
        }.toArray
      }.toArray
      val projection = newMutableProjection(allInputs, child.output)

//      val schema = if (child.schema == null) {
//        StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
//          StructField(s"_$i", dt)
//        })
//      } else {
//        child.schema
//      }

      val schema = inputSchema.getOrElse {
        StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
          StructField(s"_$i", dt)
        })
      }
      // 为什么这里能拿到正确的output？
      schema.printTreeString()

      // Add rows to queue to join later with the result.
      val projectedRowIter = iter.map { inputRow =>
        queue.add(inputRow.asInstanceOf[UnsafeRow])
        projection(inputRow)
      }

      val outputRowIterator = evaluate(
        pyFuncs, argOffsets, projectedRowIter, schema, context)

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      outputRowIterator.map { outputRow =>
        resultProj(joined(queue.remove(), outputRow))
      }
    }
  }
  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(_.find(_.isInstanceOf[PythonUDF]).isEmpty))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

}

/**
 * A physical plan that evaluates a [[PythonUDF]].
 */
case class KFArrowEvalPythonExec(udfs: Seq[PythonUDF],
                                 inputSchema: Option[StructType],
                                 output: Seq[Attribute],
                                 child: SparkPlan)
  extends KFEvalPythonExec(udfs, inputSchema, output, child) {

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

  protected override def evaluate(funcs: Seq[ChainedPythonFunctions],
                                  argOffsets: Array[Array[Int]],
                                  iter: Iterator[InternalRow],
                                  schema: StructType,
                                  context: TaskContext): Iterator[InternalRow] = {

    val outputTypes = output.drop(child.output.length).map(_.dataType)

    // DO NOT use iter.grouped(). See BatchIterator.
    val batchIter = if (batchSize > 0) new BatchIterator(iter, batchSize) else Iterator(iter)

    val columnarBatchIter = new ArrowPythonRunner(
      funcs,
      PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      argOffsets,
      schema,
      sessionLocalTimeZone,
      pythonRunnerConf).compute(batchIter, context.partitionId(), context)

    new Iterator[InternalRow] {

      private var currentIter = if (columnarBatchIter.hasNext) {
        val batch = columnarBatchIter.next()
        val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
        assert(outputTypes == actualDataTypes, "Invalid schema from pandas_udf: " +
          s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
        batch.rowIterator.asScala
      } else {
        Iterator.empty
      }

      override def hasNext: Boolean = currentIter.hasNext || {
        if (columnarBatchIter.hasNext) {
          currentIter = columnarBatchIter.next().rowIterator.asScala
          hasNext
        } else {
          false
        }
      }

      override def next(): InternalRow = currentIter.next()
    }
  }
}

case class OneRowRelationToLocalRelation() extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case proj@ Project(projectList, relation: OneRowRelation) if projectList.forall(_.resolved) =>
        val output = projectList.map(_.toAttribute)
        val data = projectList.map {
          case as: Alias =>
            as.child.eval()
        }
        Project(projectList, LocalRelation(output, Seq(InternalRow(data))))
    }
  }
}

/**
 * A logical plan that evaluates a [[PythonUDF]].
 */
case class KFArrowEvalPython(udfs: Seq[PythonUDF],
                             inputSchema: StructType,
                             output: Seq[Attribute], child: LogicalPlan)
  extends UnaryNode
