# MyExtensions

自定义的Spark 优化规则

## InsertHiveTableRule

Spark Sql 在插入数据表时进行自动合并文件优化。

* 判断 LogicPlan 为插入Hive操作，且当前查询没有进行手动repartition操作，且目标表为hive分区表
* 获取目标表的已经存在的分区统计信息，取最近的10个分区的信息进行估算出目标的分区个数，修改 LogicPlan 的逻辑计划
* 按照64M每个分区文件的大小进行预估目标分区个数，取采样分区的预估值的中位数