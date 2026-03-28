package spark

import model.SensorRecord
import index.{Z3DEncoder, TimeBucket, UnifiedIndexKey}
import storage.HBaseTableManager
import query.SelectivityEstimator.{SpatialBBox, SensorIdEquals, TimeRange, TypeEquals, QueryCondition}

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Spark cluster version unified index spatial query module
 *
 * Execution flow:
 * 1. Driver constructs ChunkTask list (reuse phase 0 logic)
 * 2. Parallelize into RDD, distribute to executors
 * 3. Each partition executes idx_unified scan on executor, returns dataRowKey
 * 4. Merge, deduplicate, lookup table, and precise filter on Driver
 * 5. Return Seq[SensorRecord]
 */
object SparkUnifiedSpatialQuery {

  /**
   * case class representing a task unit (same structure as IncrementalFilterQuery.UnifiedChunkTask)
   */
  case class ChunkTask(
    sensorId: String,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  )

  /**
   * Diagnose cluster configuration information
   */
  private def diagnoseClusterConfig(sc: org.apache.spark.SparkContext): Unit = {
    println("\n[SparkUnified] ===== Cluster Configuration Complete Diagnosis =====")

    // Read all relevant Spark configurations
    val allConfigs = sc.getConf.getAll
    val relevantKeys = allConfigs.filter { case (k, _) =>
      k.contains("executor") || k.contains("cores") ||
      k.contains("parallelism") || k.contains("instances")
    }

    println("[SparkUnified] Related Spark Configuration:")
    relevantKeys.foreach { case (k, v) =>
      println(s"  $k = $v")
    }

    println(s"[SparkUnified] Executor count: ${sc.getExecutorMemoryStatus.size}")
    println(s"[SparkUnified] Default parallelism: ${sc.defaultParallelism}")
    println(s"[SparkUnified] Master: ${sc.master}")
    println("[SparkUnified] ===== Diagnosis Complete =====\n")
  }

  /**
   * Execute unified index spatial query on Spark cluster
   *
   * @param zkQuorum HBase ZooKeeper address
   * @param sensorId Sensor ID
   * @param startTs Start timestamp (milliseconds)
   * @param endTs End timestamp (milliseconds)
   * @param lonMin Longitude minimum
   * @param latMin Latitude minimum
   * @param altMin Altitude minimum
   * @param lonMax Longitude maximum
   * @param latMax Latitude maximum
   * @param altMax Altitude maximum
   * @return Seq[SensorRecord] Query results
   */
  def runSpatialQueryWithSpark(
    zkQuorum: String,
    sensorId: String,
    startTs: Long,
    endTs: Long,
    lonMin: Double, latMin: Double, altMin: Double,
    lonMax: Double, latMax: Double, altMax: Double,
    dataset: Option[String] = None,
    unifiedLevel: Option[Int] = None
  ): Seq[SensorRecord] = {

    val globalStartTime = System.currentTimeMillis()

    // 1. Enumerate multi-level grid blocks (consistent parameters with existing unified query)
    val lvl = unifiedLevel.getOrElse(Z3DEncoder.UNIFIED_BLOCK_LEVEL)
    val zCells = Z3DEncoder.cellsAtLevel(
      lonMin, latMin, altMin,
      lonMax, latMax, altMax,
      level = lvl,
      maxCells = 1000000
    )

    // 2. Calculate dayBucket and timeOfDay for overall time range
    val startDayBucket = TimeBucket.dayBucket(startTs)
    val startTimeOfDay = TimeBucket.millisOfDay(startTs)
    val endDayBucket = TimeBucket.dayBucket(endTs)
    val endTimeOfDay = TimeBucket.millisOfDay(endTs)

    // 3. Construct ChunkTask list
    val tasks: Seq[ChunkTask] = zCells.map { zCell =>
      ChunkTask(sensorId, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
    }

    // 4. Get or create SparkSession
    val spark = SparkSession.builder()
      .appName("UnifiedSpatialQuerySpark")
      .getOrCreate()
    val sc = spark.sparkContext

    // 5. Calculate partition count and diagnostic information
    val executorInstances = sc.getConf.getInt("spark.executor.instances", 1)
    val executorCores = sc.getConf.getInt("spark.executor.cores", 1)
    val cores = executorInstances * executorCores
    val defaultParallelism = sc.defaultParallelism
    val basePartitions = math.max(cores * 2, 4)
    val numPartitions = math.min(tasks.size, basePartitions)

    // 6. Parallelize task list into RDD
    val tasksRdd = sc.parallelize(tasks, numPartitions)

    // 7. Execute scan on each partition (initial filter)
    val step1StartTime = System.currentTimeMillis()
    val rowKeyRdd = tasksRdd.mapPartitions { iter =>
      // Each partition: establish HBase connection and scan
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", zkQuorum)
      val connection = ConnectionFactory.createConnection(conf)

      try {
        val table = connection.getTable(TableName.valueOf(HBaseTableManager.unifiedIdxTableName(dataset, unifiedLevel)))
        val localRowKeys = mutable.Set[String]()

        try {
          iter.foreach { task =>
            val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForCellTimeSpan(
              task.sensorId, task.zCell,
              task.startDayBucket, task.startTimeOfDay,
              task.endDayBucket, task.endTimeOfDay
            )

            val scan = new Scan()
            scan.withStartRow(startRowBytes)
            scan.withStopRow(stopRowBytes)

            val scanner = table.getScanner(scan)
            try {
              scanner.asScala.foreach { result =>
                val rowKeyBytes = result.getRow
                // Extract dataRowKey (skip first 22 bytes prefix, consistent with IncrementalFilterQuery)
                if (rowKeyBytes.length > 22) {
                  val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                  localRowKeys += dataKey
                }
              }
            } finally {
              scanner.close()
            }
          }
        } finally {
          table.close()
        }

        localRowKeys.iterator
      } finally {
        connection.close()
      }
    }

    // 8. Collect and deduplicate
    val rowKeys: Set[String] = rowKeyRdd.distinct().collect().toSet
    val step1Time = System.currentTimeMillis() - step1StartTime

    // 9. Lookup table and precise filter on Driver
    if (rowKeys.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime

      // After all timing is complete, output logs
      println("\n" + "=" * 80)
      println("[Spark-Unified-Query] Execution plan:")
      println("  Initial filter: SensorId + Time + Space (distributed unified index scan)")
      println("  Secondary filter: Spatial precise check (coordinate range validation)")
      println("=" * 80)
      println(s"\n[Execution Parameters]")
      println(s"  → Block task count: ${tasks.size}")
      println(s"  → Task partition count: ${numPartitions}")
      println(s"  → Executor count: ${executorInstances}")
      println(s"  → Core count: ${cores}")
      println(s"\n[Initial Filter] SensorId + Time + Space distributed unified index scan")
      println(s"  → Result: ${rowKeys.size} records | Time: ${step1Time}ms")
      println("=" * 80)
      println(s"[Spark-Unified-Query-Summary]")
      println(s"  → Initial filter time: ${step1Time}ms")
      println(s"  → Secondary filter time: 0ms")
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      Seq()
    } else {
      val step2StartTime = System.currentTimeMillis()

      // Create HBase connection for lookup table
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", zkQuorum)
      val connection = ConnectionFactory.createConnection(conf)

      try {
        val dataTable = connection.getTable(TableName.valueOf(HBaseTableManager.dataTableName(dataset)))
        try {
          val gets = rowKeys.map(k => new Get(Bytes.toBytes(k))).toList.asJava
          val results: Array[Result] = dataTable.get(gets)

          val cf = HBaseTableManager.CF_BYTES
          val acceptedRecords = mutable.ListBuffer[SensorRecord]()

          results.foreach { r =>
            if (r != null && !r.isEmpty) {
              val record = SensorRecord(
                rowKey = Bytes.toString(r.getRow),
                time = Bytes.toLong(r.getValue(cf, Bytes.toBytes("time"))),
                sensorId = Bytes.toString(r.getValue(cf, Bytes.toBytes("sensor_id"))),
                longitude = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lon"))),
                latitude = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lat"))),
                altitude = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("alt"))),
                sensorType = Bytes.toString(r.getValue(cf, Bytes.toBytes("type")))
              )

              // Precise filter: check if all conditions are satisfied
              val matchesAll = (
                record.sensorId == sensorId &&
                record.time >= startTs && record.time <= endTs &&
                record.longitude >= lonMin && record.longitude <= lonMax &&
                record.latitude >= latMin && record.latitude <= latMax &&
                record.altitude >= altMin && record.altitude <= altMax
              )

              if (matchesAll) {
                acceptedRecords += record
              }
            }
          }

          val step2Time = System.currentTimeMillis() - step2StartTime
          val totalTime = System.currentTimeMillis() - globalStartTime

          // Output logs after all timing is complete
          println("\n" + "=" * 80)
          println("[Spark-Unified-Query] Execution plan:")
          println("  Initial filter: SensorId + Time + Space (distributed unified index scan)")
          println("  Secondary filter: Spatial precise check (coordinate range validation)")
          println("=" * 80)
          println(s"\n[Execution Parameters]")
          println(s"  → Block task count: ${tasks.size}")
          println(s"  → Task partition count: ${numPartitions}")
          println(s"  → Executor count: ${executorInstances}")
          println(s"  → Core count: ${cores}")
          println(s"\n[Initial Filter] SensorId + Time + Space distributed unified index scan")
          println(s"  → Result: ${rowKeys.size} records | Time: ${step1Time}ms")
          println(s"\n[Secondary Filter] Spatial precise check (coordinate range validation)")
          println(s"  → Result: ${acceptedRecords.size} records | Time: ${step2Time}ms")
          println("=" * 80)
          println(s"[Spark-Unified-Query-Summary]")
          println(s"  → Initial filter time: ${step1Time}ms")
          println(s"  → Secondary filter time: ${step2Time}ms (includes data read and coordinate validation)")
          println(s"  → Total query time: ${totalTime}ms")
          println("=" * 80 + "\n")

          acceptedRecords.toSeq
        } finally {
          dataTable.close()
        }
      } finally {
        connection.close()
      }
    }
  }
}
