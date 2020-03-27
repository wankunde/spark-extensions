package com.wankun.sql.optimizer

import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition}
import org.mockito.Mockito.mock
import org.scalatest.{FunSuite, Matchers}

import scala.language.implicitConversions

object MyExtensionsSuite {
  implicit def longToString(size: Long) = size.toString

  val storageFormat = CatalogStorageFormat(
    locationUri = None,
    inputFormat = None,
    outputFormat = None,
    serde = None,
    compressed = false,
    properties = Map.empty)

  implicit def mockPartition(parameters: Map[String, String],
                             createTime: Long): CatalogTablePartition =
    CatalogTablePartition(Map("dt" -> "20200323"), storageFormat, parameters, createTime)

}

/**
 * @author kun.wan
 * @date 2020-03-03.
 */
class MyExtensionsSuite extends FunSuite with Matchers {

  import MyExtensions._
  import MyExtensionsSuite._

  test("test reparition numbers") {
    val session = mock(classOf[SparkSession])
    val rule = InsertHiveTableRule(session)

    rule.repartitionNumbers(Array[CatalogTablePartition]()) should be(None)

    rule.repartitionNumbers(Seq[CatalogTablePartition](
      mockPartition(Map(StatsSetupConst.TOTAL_SIZE -> (2 * DEFAULT_PARTITION_SIZE)), 100)
    )) should be(Some(3))

    rule.repartitionNumbers(Array(
      mockPartition(Map(StatsSetupConst.TOTAL_SIZE -> (2 * DEFAULT_PARTITION_SIZE)), 100),
      mockPartition(Map(StatsSetupConst.TOTAL_SIZE -> (1 * DEFAULT_PARTITION_SIZE)), 101),
      mockPartition(Map(StatsSetupConst.TOTAL_SIZE -> (5 * DEFAULT_PARTITION_SIZE)), 102),
      mockPartition(Map(StatsSetupConst.TOTAL_SIZE -> (4 * DEFAULT_PARTITION_SIZE)), 103),
      mockPartition(Map(StatsSetupConst.TOTAL_SIZE -> (3 * DEFAULT_PARTITION_SIZE)), 104)
    )) should be(Some(4))

    rule.repartitionNumbers(
      (101 to 115).map { i =>
        mockPartition(Map(StatsSetupConst.TOTAL_SIZE -> ((i - 100) * DEFAULT_PARTITION_SIZE)), i)
      }
    ) should be(Some(11))
  }
}
