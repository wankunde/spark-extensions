package com.wankun.sql.optimizer

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Repartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

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

case class InsertHiveTableRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  import MyExtensions._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    logInfo("开始应用 InsertHiveTableRule 优化规则")
    plan transform {
      case InsertIntoTable(r: HiveTableRelation, partSpec, query, overwrite,
      ifPartitionNotExists) if !query.isInstanceOf[Repartition] && r.isPartitioned =>
        logInfo(s"${r}")
        logInfo(s"${partSpec}")
        logInfo(s"${query}")
        logInfo(s"${overwrite}")
        logInfo(s"${ifPartitionNotExists}")

        val catalog = SparkSession.active.sessionState.catalog
        repartitionNumbers(catalog.listPartitions(r.tableMeta.identifier)) match {
          case Some(numPartitions) =>
            val shuffle = true
            InsertIntoTable(r, partSpec, Repartition(numPartitions, shuffle, query), overwrite,
              ifPartitionNotExists)
          case None => plan
        }
    }
  }


  /**
   * 1. 根据分区创建时间倒排序，取最近创建的分区
   * 2. sample 采样10个分区元数据来计算分区个数，取结果中位数
   * @param partitions
   * @return
   */
  def repartitionNumbers(partitions: Seq[CatalogTablePartition]): Option[Int] = {

    val stats = new DescriptiveStatistics

    if (log.isDebugEnabled) {
      partitions.foreach(p => logInfo(p.simpleString))
    }
    partitions.sortWith((p1, p2) => p1.createTime > p2.createTime)
      .slice(0, SAMPLING_PARTITIONS)
      .foreach { p =>
        stats.addValue(p.parameters.get(StatsSetupConst.TOTAL_SIZE).get.toLong
          / DEFAULT_PARTITION_SIZE)
      }
    if (stats.getPercentile(50).isNaN) {
      None
    } else {
      val number = stats.getPercentile(50).toInt + 1
      if (number > 0) {
        Some(number)
      } else {
        None
      }
    }
  }
}

object MyExtensions {
  val DEFAULT_PARTITION_SIZE = 64 * 1024 * 1024L
  val SAMPLING_PARTITIONS = 10
}

class MyExtensions extends (SparkSessionExtensions => Unit) with Logging {
  def apply(e: SparkSessionExtensions): Unit = {
    logInfo("进入MyExtensions扩展点")
    e.injectResolutionRule(InsertHiveTableRule)
  }
}
