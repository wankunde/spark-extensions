package com.wankun.sql.optimizer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.Decimal

/**
 * @author kun.wan
 * @date 2020-03-03.
 */
case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    logInfo("开始应用 MyRule 优化规则")
    plan transformAllExpressions {
      case Multiply(left, right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.isInstanceOf[Decimal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Decimal].toDouble == 1.0 =>
        logInfo("MyRule 优化规则生效")
        left
    }
  }
}

class MyExtensions extends (SparkSessionExtensions => Unit) with Logging {
  def apply(e: SparkSessionExtensions): Unit = {
    logInfo("进入MyExtensions扩展点")
    e.injectResolutionRule(MyRule)
  }
}
