// test my rule
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("Test for MyRule").master("local[2]").getOrCreate()

val df = spark.range(10).selectExpr("id", "concat('wankun-',id) as name")
val multipliedDF = df.selectExpr("id * cast(1.0 as double) as id2")
println(multipliedDF.queryExecution.optimizedPlan.numberedTreeString)

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object MultiplyOptimizationRule extends Rule[LogicalPlan] with Logging {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Multiply(left,right) if right.isInstanceOf[Literal] &&
      right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
      logInfo("MyRule 优化规则生效")
      left
  }
}

spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)

val multipliedDFWithOptimization = df.selectExpr("id * cast(1.0 as double) as id2")
println("after optimization")
println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)

// test MyExtensions

/**
explain extended
with stu as (
  select 1 as id, 'wankun-1' as name
  union
  select 2 as id, 'wankun-2' as name
  union
  select 3 as id, 'wankun-3' as name
)
select id * 1.0
from stu;
 */

val sqlText = "explain extended\nwith stu as (\n  select 1 as id, 'wankun-1' as name\n  union\n  " +
  "select 2 as id, 'wankun-2' as name\n  union\n  select 3 as id, 'wankun-3' as name\n)\nselect " +
  "id * 1.0\nfrom stu;"
spark.sql(sqlText)


