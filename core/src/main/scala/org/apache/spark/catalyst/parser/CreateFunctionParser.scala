package org.apache.spark.catalyst.parser

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.catalyst.parser.CreateFunctionParser.ExtensionsBuilder
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, AstBuilder, ParseException, ParserInterface, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{CreateFunctionContext, QualifiedNameContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.{CreateFunctionCommand, CreateMLFlowFunctionCommand}
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}

/**
 * @time 2019-09-05 14:00
 * @author fchen <cloud.chenfu@gmail.com>
 */
class CreateFunctionParser(conf: SQLConf) extends AbstractSqlParser {

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }

  private val substitutor = new VariableSubstitution(conf)

  override protected def astBuilder: AstBuilder = new PandaAstBuider(conf)
}

object CreateFunctionParser {
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type ExtensionsBuilder = SparkSessionExtensions => Unit
  val parserBuilder: ParserBuilder = (_, _) => new CreateFunctionParser(new SQLConf)
  val extBuilder: ExtensionsBuilder = { e => e.injectParser(parserBuilder)}
}

class PandaSparkExtensions extends ExtensionsBuilder {
  override def apply(sessionExtensions: SparkSessionExtensions): Unit = {
    sessionExtensions.injectParser((_, _) => new CreateFunctionParser(new SQLConf))
  }
}

class PandaAstBuider(conf: SQLConf) extends SparkSqlAstBuilder(conf) {

  override def visitCreateFunction(ctx: CreateFunctionContext): LogicalPlan = withOrigin(ctx) {
    val options = ctx.resource.asScala.map { resource =>
      val key = resource.identifier.getText.toLowerCase(Locale.ROOT)
      (key, string(resource.STRING()))
    }.toMap
    val tpe = options.get("type")
    if (tpe.isDefined && tpe.get == "mlflow") {
      // Extract database, name & alias.
      val functionIdentifier = visitFunctionName(ctx.qualifiedName)
      CreateMLFlowFunctionCommand(
        functionIdentifier.database,
        functionIdentifier.funcName,
        string(ctx.className),
        options,
        ctx.TEMPORARY != null,
        ctx.EXISTS != null,
        ctx.REPLACE != null)

    } else {
      super.visitCreateFunction(ctx).asInstanceOf[LogicalPlan]
    }
  }
}
