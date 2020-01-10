package org.apache.spark.sql.execution.python

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LeafNode, LogicalPlan, OneRowRelation, Project, Statistics, SubqueryAlias, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.arrow.ArrowUtils
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * @time 2020/1/9 1:18 下午
 * @author fchen <cloud.chenfu@gmail.com>
 *
 * copy from spark.
 */
abstract class KFEvalPythonExec(udfs: Seq[PythonUDF],
                                inputSchema: StructType,
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

      val schema = if (child.schema == null) {
        StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
          StructField(s"_$i", dt)
        })
      } else {
        child.schema
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
                                 inputSchema: StructType,
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

case class OneRowRelationWithSchema(val output: Seq[Attribute],
                                    data: Seq[(ExprId, Any)]) extends LeafNode {
  override def maxRows: Option[Long] = Some(1)
  override def computeStats(): Statistics = Statistics(sizeInBytes = 1)
  //  /** [[org.apache.spark.sql.catalyst.trees.TreeNode.makeCopy()]] does not support 0-arg ctor. */
  //  override def makeCopy(newArgs: Array[AnyRef]): OneRowRelationWithSchema = OneRowRelationWithSchema(outp)
}

case class EmptyRDDScanExec(output: Seq[Attribute],
                        name: String,
                        override val outputPartitioning: Partitioning = UnknownPartitioning(0),
                        override val outputOrdering: Seq[SortOrder] = Nil) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] =
    sparkContext.parallelize(Seq(InternalRow()), 1)
//  val singleRowRdd = SparkSession.active
//    .sparkContext
//    .parallelize(Seq(InternalRow()), 1)
}

case class AddOneRowRelationSchema() extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case project@ Project(projectList, child) if child.isInstanceOf[OneRowRelation] =>
        val output = projectList.map(_.toAttribute)
        val data = projectList.map {
          case as@ Alias(child: Literal, name) =>
            (as.exprId, child.value)
        }
        Project(projectList, OneRowRelationWithSchema(output, data))
//      case project@ Project(projectList, child) if child.isInstanceOf[OneRowRelation] =>
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

//object KFExtractPythonUDFs extends Rule[LogicalPlan] with PredicateHelper {
//  private type EvalType = Int
//  private type EvalTypeChecker = EvalType => Boolean
//
//  private def hasScalarPythonUDF(e: Expression): Boolean = {
//    e.find(PythonUDF.isScalarPythonUDF).isDefined
//  }
//
//  private def canEvaluateInPython(e: PythonUDF): Boolean = {
//    e.children match {
//      // single PythonUDF child could be chained and evaluated in Python
//      case Seq(u: PythonUDF) => e.evalType == u.evalType && canEvaluateInPython(u)
//      // Python UDF can't be evaluated directly in JVM
//      case children => !children.exists(hasScalarPythonUDF)
//    }
//  }
//
//  private def collectEvaluableUDFsFromExpressions(expressions: Seq[Expression]): Seq[PythonUDF] = {
//    // Eval type checker is set once when we find the first evaluable UDF and its value
//    // shouldn't change later.
//    // Used to check if subsequent UDFs are of the same type as the first UDF. (since we can only
//    // extract UDFs of the same eval type)
//    var evalTypeChecker: Option[EvalTypeChecker] = None
//
//    def collectEvaluableUDFs(expr: Expression): Seq[PythonUDF] = expr match {
//      case udf: PythonUDF if PythonUDF.isScalarPythonUDF(udf) && canEvaluateInPython(udf)
//        && evalTypeChecker.isEmpty =>
//        evalTypeChecker = Some((otherEvalType: EvalType) => otherEvalType == udf.evalType)
//        Seq(udf)
//      case udf: PythonUDF if PythonUDF.isScalarPythonUDF(udf) && canEvaluateInPython(udf)
//        && evalTypeChecker.get(udf.evalType) =>
//        Seq(udf)
//      case e => e.children.flatMap(collectEvaluableUDFs)
//    }
//
//    expressions.flatMap(collectEvaluableUDFs)
//  }
//
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case plan: LogicalPlan => {
//      println(plan)
//      val result = extract(plan)
//      result match {
//        case ArrowEvalPython(udfs, output, child) =>
//          plan.children.flatMap(_.expressions.collect{case ne: NamedExpression => ne})
//              .foreach(println)
//          plan.children.foreach(_.expressions.foreach(println))
////          KFArrowEvalPython(udfs, )
//          result
//        case _ => result
//      }
//    }
//  }
//
//  /**
//   * Extract all the PythonUDFs from the current operator and evaluate them before the operator.
//   */
//  private def extract(plan: LogicalPlan): LogicalPlan = {
//    val udfs = collectEvaluableUDFsFromExpressions(plan.expressions)
//      // ignore the PythonUDF that come from second/third aggregate, which is not used
//      .filter(udf => udf.references.subsetOf(plan.inputSet))
//    if (udfs.isEmpty) {
//      // If there aren't any, we are done.
//      plan
//    } else {
//      val inputsForPlan = plan.references ++ plan.outputSet
//      val prunedChildren = plan.children.map { child =>
//        val allNeededOutput = inputsForPlan.intersect(child.outputSet).toSeq
//        if (allNeededOutput.length != child.output.length) {
//          Project(allNeededOutput, child)
//        } else {
//          child
//        }
//      }
//      val planWithNewChildren = plan.withNewChildren(prunedChildren)
//
//      val attributeMap = mutable.HashMap[PythonUDF, Expression]()
//      val splitFilter = trySplitFilter(planWithNewChildren)
//      // Rewrite the child that has the input required for the UDF
//      val newChildren = splitFilter.children.map { child =>
//        // Pick the UDF we are going to evaluate
//        val validUdfs = udfs.filter { udf =>
//          // Check to make sure that the UDF can be evaluated with only the input of this child.
//          udf.references.subsetOf(child.outputSet)
//        }
//        if (validUdfs.nonEmpty) {
//          require(
//            validUdfs.forall(PythonUDF.isScalarPythonUDF),
//            "Can only extract scalar vectorized udf or sql batch udf")
//
//          val resultAttrs = udfs.zipWithIndex.map { case (u, i) =>
//            AttributeReference(s"pythonUDF$i", u.dataType)()
//          }
//
//          val evaluation = validUdfs.partition(
//            _.evalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF
//          ) match {
//            case (vectorizedUdfs, plainUdfs) if plainUdfs.isEmpty =>
//              ArrowEvalPython(vectorizedUdfs, child.output ++ resultAttrs, child)
//            case (vectorizedUdfs, plainUdfs) if vectorizedUdfs.isEmpty =>
//              BatchEvalPython(plainUdfs, child.output ++ resultAttrs, child)
//            case _ =>
//              throw new AnalysisException(
//                "Expected either Scalar Pandas UDFs or Batched UDFs but got both")
//          }
//
//          attributeMap ++= validUdfs.zip(resultAttrs)
//          evaluation
//        } else {
//          child
//        }
//      }
//      // Other cases are disallowed as they are ambiguous or would require a cartesian
//      // product.
//      udfs.filterNot(attributeMap.contains).foreach { udf =>
//        sys.error(s"Invalid PythonUDF $udf, requires attributes from more than one child.")
//      }
//
//      val rewritten = splitFilter.withNewChildren(newChildren).transformExpressions {
//        case p: PythonUDF if attributeMap.contains(p) =>
//          attributeMap(p)
//      }
//
//      // extract remaining python UDFs recursively
//      val newPlan = extract(rewritten)
//      if (newPlan.output != plan.output) {
//        // Trim away the new UDF value if it was only used for filtering or something.
//        Project(plan.output, newPlan)
//      } else {
//        newPlan
//      }
//    }
//  }
//
//  // Split the original FilterExec to two FilterExecs. Only push down the first few predicates
//  // that are all deterministic.
//  private def trySplitFilter(plan: LogicalPlan): LogicalPlan = {
//    plan match {
//      case filter: Filter =>
//        val (candidates, nonDeterministic) =
//          splitConjunctivePredicates(filter.condition).partition(_.deterministic)
//        val (pushDown, rest) = candidates.partition(!hasScalarPythonUDF(_))
//        if (pushDown.nonEmpty) {
//          val newChild = Filter(pushDown.reduceLeft(And), filter.child)
//          Filter((rest ++ nonDeterministic).reduceLeft(And), newChild)
//        } else {
//          filter
//        }
//      case o => o
//    }
//  }
//}
