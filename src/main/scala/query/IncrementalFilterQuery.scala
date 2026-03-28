// src/main/scala/query/IncrementalFilterQuery.scala
package query

import model.SensorRecord
import index.{Z3DEncoder, TimeBucket, UnifiedIndexKey}
import storage.HBaseTableManager
import query.SelectivityEstimator._

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.concurrent.Executors

class IncrementalFilterQuery(
  zkQuorum: String,
  enableUnifiedIndex: Boolean = true,       // New: whether to allow using unified index
  useUnifiedParallel: Boolean = false,      // Existing: whether to use local multi-threading for unified
  unifiedThreadPoolSize: Int = 8,           // Existing: thread pool size
  dataset: Option[String] = None,           // New: dataset ID
  unifiedLevel: Option[Int] = None          // New: unified index level
) {

  /**
   * Task unit: represents a unified index scan on a spatial block (zCell) for a time interval
   */
  case class UnifiedChunkTask(
    sensorId: String,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  )

  /**
   * Detailed log information for spatial filtering
   */
  case class SpatialFilterLog(
    roughFilterLog: String,
    refinedFilterLog: String,
    totalLog: String
  )

  private var lastSpatialFilterLog: Option[SpatialFilterLog] = None

  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", zkQuorum)
  private val connection = ConnectionFactory.createConnection(conf)

  // ====== New: Get batching only for "distributed query/incremental filter path" ======
  private val distributedGetBatchSize: Int = 500

  private def forEachResultByBatchedGet(
    table: Table,
    rowKeys: Iterable[String],
    batchSize: Int = distributedGetBatchSize
  )(buildGet: String => Get)(handle: Result => Unit): Unit = {
    val it = rowKeys.iterator
    while (it.hasNext) {
      val batch = new java.util.ArrayList[Get](batchSize)
      var i = 0
      while (i < batchSize && it.hasNext) {
        batch.add(buildGet(it.next()))
        i += 1
      }
      val results: Array[Result] = table.get(batch)
      results.foreach(handle)
    }
  }

  def query(conditions: Seq[SelectivityEstimator.QueryCondition]): Iterator[SensorRecord] = {
    if (conditions.isEmpty) return Iterator.empty

    // === Unified index priority path: detect SensorId + Time + Spatial conditions ===
    val sensorCond = conditions.collectFirst { case s: SelectivityEstimator.SensorIdEquals => s }
    val timeCond = conditions.collectFirst { case t: SelectivityEstimator.TimeRange => t }
    val spatialCond = conditions.collectFirst { case s: SelectivityEstimator.SpatialBBox => s }

    if (enableUnifiedIndex && sensorCond.isDefined && timeCond.isDefined && spatialCond.isDefined) {
      return queryUsingUnifiedIndex(sensorCond.get, timeCond.get, spatialCond.get, conditions)
    }

    // === Original incremental filter path ===
    // Modified order: Attribute → Spatial → Time → Secondary filter
    val sortedConditions = sortConditionsForIncrementalFilter(conditions)

    val globalStartTime = System.currentTimeMillis()

    // Step 1: Execute first condition query
    val step1StartTime = System.currentTimeMillis()
    var currentIds: Set[String] = executeFirstQuery(sortedConditions.head)
    val step1Time = System.currentTimeMillis() - step1StartTime
    val step1ResultCount = currentIds.size

    if (currentIds.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      println("\n" + "=" * 80)
      println("[Incremental-Query] Execution plan:")
      println("  Step 1: Attribute Conditions") (Attribute Conditions)")
      println("  Step 2: Spatial Condition") (Spatial Condition)")
      println("  Step 3: Time Condition") (Time Condition)")
      println("  Step 4: Refined Filter") (Refined Filter)")
      println("=" * 80)
      println(s"[Step 1] ${sortedConditions.head.getClass.getSimpleName}")
      println(s"  → Result: ${currentIds.size} records | Time: ${step1Time}ms")
      println("=" * 80)
      println(s"[Incremental-Query] Query completed - no results | Total time: ${totalTime}ms")
      println("=" * 80 + "\n")
      return Iterator.empty
    }

    // Subsequent steps: apply other conditions one by one
    var stepTimes = scala.collection.mutable.Map[Int, Long]()
    var stepResults = scala.collection.mutable.Map[Int, Int]()
    var stepSpatialLogs = scala.collection.mutable.Map[Int, SpatialFilterLog]()

    sortedConditions.tail.zipWithIndex.foreach { case (condition, idx) =>
      if (currentIds.isEmpty) {
        val totalTime = System.currentTimeMillis() - globalStartTime
        println("\n" + "=" * 80)
        println("[Incremental-Query] Execution plan:")
        println("  Step 1: Attribute Conditions") (Attribute Conditions)")
        println("  Step 2: Spatial Condition") (Spatial Condition)")
        println("  Step 3: Time Condition") (Time Condition)")
        println("  Step 4: Refined Filter") (Refined Filter)")
        println("=" * 80)
        println(s"[Step 1] ${sortedConditions.head.getClass.getSimpleName}")
        println(s"  → Result: ${currentIds.size} records | Time: ${step1Time}ms")
        sortedConditions.tail.zipWithIndex.take(idx).foreach { case (cond, sidx) =>
          println(s"\n[Step ${sidx + 2}] ${cond.getClass.getSimpleName}")
          println(s"  → Result: ${stepResults(sidx + 2)} records | Total time: ${stepTimes(sidx + 2)}ms")
        }
        println("=" * 80)
        println(s"[Incremental-Query] Query aborted - Step ${idx + 2} has no candidate records | Total time: ${totalTime}ms")
        println("=" * 80 + "\n")
        return Iterator.empty
      }

      val stepStartTime = System.currentTimeMillis()
      currentIds = executeIncrementalQuery(condition, currentIds)
      val stepTime = System.currentTimeMillis() - stepStartTime

      stepTimes(idx + 2) = stepTime
      stepResults(idx + 2) = currentIds.size
      if (lastSpatialFilterLog.isDefined) {
        stepSpatialLogs(idx + 2) = lastSpatialFilterLog.get
        lastSpatialFilterLog = None
      }
    }

    // Fetch final records
    val fetchStartTime = System.currentTimeMillis()
    val records = if (currentIds.isEmpty) {
      Iterator.empty
    } else {
      fetchRecords(currentIds)
    }
    val fetchTime = System.currentTimeMillis() - fetchStartTime

    val globalTime = System.currentTimeMillis() - globalStartTime

    // ========== Output logs after all timing is complete ==========
    println("\n" + "=" * 80)
    println("[Incremental-Query] Execution plan:")
    println("  Step 1: Attribute Conditions") (Attribute Conditions)")
    println("  Step 2: Spatial Condition") (Spatial Condition)")
    println("  Step 3: Time Condition") (Time Condition)")
    println("  Step 4: Refined Filter") (Refined Filter)")
    println("=" * 80)

    println(s"\n[Step 1] ${sortedConditions.head.getClass.getSimpleName}")
    println(s"  → Result: ${step1ResultCount} records | Time: ${step1Time}ms")

    stepTimes.keys.toSeq.sorted.foreach { stepNum =>
      println(s"\n[Step ${stepNum}] ${sortedConditions(stepNum - 1).getClass.getSimpleName}")
      println(s"  → Result: ${stepResults(stepNum)} records | Total time: ${stepTimes(stepNum)}ms")

      if (stepSpatialLogs.contains(stepNum)) {
        val log = stepSpatialLogs(stepNum)
        println(s"    ${log.roughFilterLog}")
        println(s"    ${log.refinedFilterLog}")
        println(s"    ${log.totalLog}")
      }
    }

    println("\n" + "=" * 80)
    println(s"[Incremental-Query-Summary]")
    println(s"  → Step 1 Attribute condition time: ${step1Time}ms")
    if (stepTimes.contains(2)) println(s"  → Step 2 Spatial condition time: ${stepTimes(2)}ms")
    if (stepTimes.contains(3)) println(s"  → Step 3 Time condition time: ${stepTimes(3)}ms")
    println(s"  → Secondary filter time: ${fetchTime}ms")
    println(s"  → Total query time: ${globalTime}ms")
    println("=" * 80 + "\n")

    records
  }

  private def executeFirstQuery(condition: SelectivityEstimator.QueryCondition): Set[String] = {
    condition match {
      case SelectivityEstimator.SensorIdEquals(sensorId) => querySensorIdIndex(sensorId)
      case SelectivityEstimator.TimeRange(startTime, endTime) => queryTimeIndex(startTime, endTime)
      case bbox: SelectivityEstimator.SpatialBBox => querySpatialIndex(bbox)
      case SelectivityEstimator.TypeEquals(t) => queryByType(t)
    }
  }

  private def executeIncrementalQuery(condition: SelectivityEstimator.QueryCondition,
                                       currentIds: Set[String]): Set[String] = {
    condition match {
      // [Improved] Handle spatial condition separately, ensure both coarse and refined filter steps
      case bbox: SelectivityEstimator.SpatialBBox =>
        executeIncrementalSpatialQuery(bbox, currentIds)

      case timeRange: SelectivityEstimator.TimeRange =>
        // [Improved] Add coarse and refined filter timing logs for time condition
        executeIncrementalTimeQuery(timeRange, currentIds)

      case sensorId: SelectivityEstimator.SensorIdEquals =>
        // [Improved] Add coarse and refined filter timing logs for attribute condition
        executeIncrementalAttributeQuery(sensorId, currentIds)

      case typeEq: SelectivityEstimator.TypeEquals =>
        // Type condition
        executeIncrementalAttributeQuery(typeEq, currentIds)

      case _ =>
        Set.empty
    }
  }

  /**
   * [New] Handle time condition in incremental query, add timing logs
   * Coarse filter: Time index range scan
   * Refined filter: Time range precise check
   */
  private def executeIncrementalTimeQuery(timeRange: SelectivityEstimator.TimeRange,
                                          currentIds: Set[String]): Set[String] = {
    if (currentIds.size <= 100) {
      filterByConditionFromData(timeRange, currentIds)
    } else {
      val indexResult = executeFirstQuery(timeRange)
      currentIds.intersect(indexResult)
    }
  }

  /**
   * [New] Handle attribute condition in incremental query, add timing logs
   * Coarse filter: Attribute index scan
   * Refined filter: Exact match
   */
  private def executeIncrementalAttributeQuery(condition: SelectivityEstimator.QueryCondition,
                                               currentIds: Set[String]): Set[String] = {
    if (currentIds.size <= 100) {
      filterByConditionFromData(condition, currentIds)
    } else {
      val indexResult = executeFirstQuery(condition)
      currentIds.intersect(indexResult)
    }
  }

  /**
   * [New] Handle spatial condition in incremental query
   * Ensure both coarse and refined filter steps (consistent with unified index)
   * Coarse filter: Block encoding ID iteration
   * Refined filter: Coordinate range precise check
   */
  private def executeIncrementalSpatialQuery(bbox: SelectivityEstimator.SpatialBBox,
                                              currentIds: Set[String]): Set[String] = {
    val totalStartTime = System.currentTimeMillis()

    val result = mutable.Set[String]()

    if (currentIds.isEmpty) {
      return result.toSet
    }

    // Step 1: [Coarse filter] Use block encoding ID to iteratively filter candidate records at block level
    val roughStartTime = System.currentTimeMillis()
    val blockCells = Z3DEncoder.cells(
      bbox.lonMin, bbox.latMin, bbox.altMin,
      bbox.lonMax, bbox.latMax, bbox.altMax,
      maxDepth = Z3DEncoder.UNIFIED_BLOCK_LEVEL,
      maxCells = 1000000
    )

    // Read candidate records from data table
    val dataTable = connection.getTable(TableName.valueOf(HBaseTableManager.dataTableName(dataset)))
    try {
      val cf = HBaseTableManager.CF_BYTES
      var roughPassCount = 0
      var refinedCount = 0

      forEachResultByBatchedGet(dataTable, currentIds) { id =>
        new Get(Bytes.toBytes(id))
      } { r =>
        if (r != null && !r.isEmpty) {
          val lon = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lon")))
          val lat = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lat")))
          val alt = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("alt")))

          // Coarse filter: Calculate block encoding for this record, check if in block set
          val recordBlockId = Z3DEncoder.blockIdForUnifiedIndex(lon, lat, alt)
          if (blockCells.contains(recordBlockId)) {
            roughPassCount += 1

            // Refined filter: Coordinate range precise check
            if (lon >= bbox.lonMin && lon <= bbox.lonMax &&
                lat >= bbox.latMin && lat <= bbox.latMax &&
                alt >= bbox.altMin && alt <= bbox.altMax) {
              val rowKey = Bytes.toString(r.getRow)
              result += rowKey
              refinedCount += 1
            }
          }
        }
      }

      val roughTime = System.currentTimeMillis() - roughStartTime
      val roughFilterLog = s"[Spatial-RoughFilter] Block encoding iteration ${blockCells.size} blocks | ${roughPassCount} records | Time: ${roughTime}ms"

      val refinementRate = if (roughPassCount > 0) f"${refinedCount * 100.0 / roughPassCount}%.1f" else "N/A"
      val refinedFilterLog = s"[Spatial-RefinedFilter] Coordinate range validation | ${refinedCount} records | Precision rate: $refinementRate%"

      val totalTime = System.currentTimeMillis() - totalStartTime
      val totalLog = s"[Spatial-Total] Total time: ${totalTime}ms"

      // Save log for main loop to use
      lastSpatialFilterLog = Some(SpatialFilterLog(roughFilterLog, refinedFilterLog, totalLog))

    } finally {
      dataTable.close()
    }

    result.toSet
  }

  private def querySensorIdIndex(sensorId: String): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.sensorIdxTableName(dataset)))
    val ids = mutable.Set[String]()

    try {
      val scan = new Scan()
      scan.setRowPrefixFilter(Bytes.toBytes(s"${sensorId}_"))

      val scanner = table.getScanner(scan)
      try {
        scanner.asScala.foreach { result =>
          val rowKey = Bytes.toString(result.getRow)
          val dataKey = rowKey.substring(sensorId.length + 1)
          ids += dataKey
        }
      } finally {
        scanner.close()
      }
    } finally {
      table.close()
    }

    ids.toSet
  }

  private def queryTimeIndex(startTime: Long, endTime: Long): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.timeIdxTableName(dataset)))
    val ids = mutable.Set[String]()

    try {
      val buckets = TimeBucket.dayBuckets(startTime, endTime)

      buckets.foreach { bucket =>
        val startRow = f"${bucket}_${startTime}%013d_"
        val endRow = f"${bucket}_${endTime}%013d_~"

        val scan = new Scan()
        scan.withStartRow(Bytes.toBytes(startRow))
        scan.withStopRow(Bytes.toBytes(endRow))

        val scanner = table.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val rowKey = Bytes.toString(result.getRow)
            val parts = rowKey.split("_")
            if (parts.length >= 3) {
              ids += parts(2)
            }
          }
        } finally {
          scanner.close()
        }
      }
    } finally {
      table.close()
    }

    ids.toSet
  }

  /**
   * Query spatial index using block-level encoding (consistent with unified index)
   *
   * Query flow:
   * 1. Enumerate all blocks covered by BBox (block level)
   * 2. Scan idx_spatial based on block encoding ID (coarse filter)
   * 3. Perform precise coordinate range validation on scan results (refined filter)
   */
  private def querySpatialIndex(bbox: SelectivityEstimator.SpatialBBox): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.spatialIdxTableName(dataset)))
    val ids = mutable.Set[String]()

    try {
      // Step 1: [Coarse filter] Enumerate all blocks covered by BBox and scan idx_spatial using block encoding ID
      val blockCells = Z3DEncoder.cells(
        bbox.lonMin, bbox.latMin, bbox.altMin,
        bbox.lonMax, bbox.latMax, bbox.altMax,
        maxDepth = Z3DEncoder.UNIFIED_BLOCK_LEVEL,
        maxCells = 1000000
      )

      println(s"  [Spatial-RoughFilter] Enumerated ${blockCells.size} blocks for coarse filtering")

      // Convert block encoding to byte array range, scan idx_spatial
      val candidateKeys = mutable.Set[String]()

      blockCells.foreach { blockZ3D =>
        // Convert block encoding to big-endian byte array
        val blockBytes = Z3DEncoder.longToBytes(blockZ3D)

        val scan = new Scan()
        scan.withStartRow(blockBytes)
        // Construct stopRow = blockZ3D + 1 (but block encodings are not contiguous, need special handling)
        // For simplicity, directly scan all points corresponding to this block encoding
        // idx_spatial rowKey format: [8-byte Z3D point encoding] + "_" + [dataKey]
        // Point encoding range within block is [blockZ3D, blockZ3D + blockSize-1] (rough), use range scan

        val blockSize = Z3DEncoder.UNIFIED_BLOCK_SIZE
        // Calculate approximate max point encoding within block (conservative estimate, slightly loose)
        val nextBlockBytes = if (blockZ3D == scala.Long.MaxValue) {
          null
        } else {
          // Number of points in block is approximately blockSize^3, but due to Z3D encoding characteristics, need to be loose
          val maxPointInBlock = blockZ3D + (blockSize * blockSize * blockSize).toLong
          Z3DEncoder.longToBytes(maxPointInBlock)
        }

        scan.withStartRow(blockBytes)
        if (nextBlockBytes != null) {
          scan.withStopRow(nextBlockBytes)
        }

        val scanner = table.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val rowKeyBytes = result.getRow
            // rowKey format: [8-byte Z3D] + "_" + [business rowKey]
            if (rowKeyBytes.length > 9) {
              val dataKey = Bytes.toString(rowKeyBytes, 9, rowKeyBytes.length - 9)
              candidateKeys += dataKey
            }
          }
        } finally {
          scanner.close()
        }
      }

      println(s"  [Spatial-RoughFilter] Coarse filter returned ${candidateKeys.size} candidate records")

      // Step 2: [Refined filter] Perform precise coordinate range validation on candidate records
      if (candidateKeys.nonEmpty) {
        val dataTable = connection.getTable(TableName.valueOf(HBaseTableManager.dataTableName(dataset)))
        try {
          val cf = HBaseTableManager.CF_BYTES
          var refinedCount = 0

          forEachResultByBatchedGet(dataTable, candidateKeys) { k =>
            new Get(Bytes.toBytes(k))
          } { r =>
            if (r != null && !r.isEmpty) {
              val lon = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lon")))
              val lat = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lat")))
              val alt = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("alt")))

              // Refined filter: Coordinate range precise check
              if (lon >= bbox.lonMin && lon <= bbox.lonMax &&
                  lat >= bbox.latMin && lat <= bbox.latMax &&
                  alt >= bbox.altMin && alt <= bbox.altMax) {
                val rowKey = Bytes.toString(r.getRow)
                ids += rowKey
                refinedCount += 1
              }
            }
          }

          val refinementRate = if (candidateKeys.size > 0) f"${refinedCount * 100.0 / candidateKeys.size}%.1f" else "N/A"
          println(s"  [Spatial-RefinedFilter] Refined filter completed: ${refinedCount} records passed coordinate range validation (precision rate=$refinementRate%)")
        } finally {
          dataTable.close()
        }
      }

    } finally {
      table.close()
    }

    ids.toSet
  }

  private def queryByType(sensorType: String): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.dataTableName(dataset)))
    val ids = mutable.Set[String]()

    try {
      val scan = new Scan()
      scan.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("type"))

      val scanner = table.getScanner(scan)
      try {
        scanner.asScala.foreach { result =>
          val typeValue = Bytes.toString(result.getValue(
            HBaseTableManager.CF_BYTES, Bytes.toBytes("type")))
          if (typeValue == sensorType) {
            ids += Bytes.toString(result.getRow)
          }
        }
      } finally {
        scanner.close()
      }
    } finally {
      table.close()
    }

    ids.toSet
  }

  private def filterByConditionFromData(condition: SelectivityEstimator.QueryCondition,
                                         ids: Set[String]): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.dataTableName(dataset)))
    val resultSet = mutable.Set[String]()

    try {
      forEachResultByBatchedGet(table, ids) { id =>
        new Get(Bytes.toBytes(id))
      } { r =>
        if (r != null && !r.isEmpty) {
          val matches = condition match {
            case SelectivityEstimator.TimeRange(startTime, endTime) =>
              val time = Bytes.toLong(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("time")))
              time >= startTime && time <= endTime

            case bbox: SelectivityEstimator.SpatialBBox =>
              val lon = Bytes.toDouble(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("lon")))
              val lat = Bytes.toDouble(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("lat")))
              val alt = Bytes.toDouble(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("alt")))

              // [Refined filter] Precise filtering for spatial condition: coordinate range check
              lon >= bbox.lonMin && lon <= bbox.lonMax &&
              lat >= bbox.latMin && lat <= bbox.latMax &&
              alt >= bbox.altMin && alt <= bbox.altMax

            case SelectivityEstimator.SensorIdEquals(sensorId) =>
              val id = Bytes.toString(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("sensor_id")))
              id == sensorId

            case SelectivityEstimator.TypeEquals(t) =>
              val typeValue = Bytes.toString(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("type")))
              typeValue == t
          }

          if (matches) {
            resultSet += Bytes.toString(r.getRow)
          }
        }
      }
    } finally {
      table.close()
    }

    resultSet.toSet
  }

  /**
   * Execute unified index tasks serially
   * Execute scan for each task in order, add result dataRowKey to rowKeys
   */
  private def executeUnifiedSerial(
    tasks: Seq[UnifiedChunkTask],
    rowKeys: mutable.Set[String]
  ): Unit = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.unifiedIdxTableName(dataset, unifiedLevel)))
    try {
      tasks.foreach { task =>
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
            // Extract dataRowKey (skip first 22 bytes of prefix)
            if (rowKeyBytes.length > 22) {
              val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
              rowKeys += dataKey
            }
          }
        } finally {
          scanner.close()
        }
      }
    } finally {
      table.close()
    }
  }

  /**
   * Execute unified index tasks in parallel (using thread pool)
   * Each thread independently gets a Table, executes assigned tasks, and finally merges results
   */
  private def executeUnifiedParallel(
    tasks: Seq[UnifiedChunkTask],
    rowKeys: mutable.Set[String]
  ): Unit = {
    val threadPool = Executors.newFixedThreadPool(unifiedThreadPoolSize)
    val localRowKeysList = scala.collection.mutable.ListBuffer[mutable.Set[String]]()
    val lockObj = new Object()

    try {
      val tasksPerThread = (tasks.length + unifiedThreadPoolSize - 1) / unifiedThreadPoolSize
      val futures = (0 until unifiedThreadPoolSize).map { threadIdx =>
        val startIdx = threadIdx * tasksPerThread
        val endIdx = math.min(startIdx + tasksPerThread, tasks.length)

        threadPool.submit(new Runnable {
          override def run(): Unit = {
            if (startIdx < tasks.length) {
              val assignedTasks = tasks.slice(startIdx, endIdx)
              val localRowKeys = mutable.Set[String]()

              val table = connection.getTable(TableName.valueOf(HBaseTableManager.unifiedIdxTableName(dataset, unifiedLevel)))
              try {
                assignedTasks.foreach { task =>
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

              // Thread-safely merge results
              lockObj.synchronized {
                localRowKeysList += localRowKeys
              }
            }
          }
        })
      }

      // Wait for all tasks to complete
      futures.foreach(_.get())

      // Merge all local result sets
      localRowKeysList.foreach { localSet =>
        rowKeys ++= localSet
      }
    } finally {
      threadPool.shutdown()
    }
  }

  /**
   * Execute query using unified index (idx_unified) (multi-level grid block version)
   * Only called when SensorIdEquals + TimeRange + SpatialBBox conditions all exist
   *
   * Improvements:
   * - Adopt multi-level 3D grid block strategy, replacing original Z3D continuous interval scheme
   * - Adopt continuous time range scan, perform only one Scan per zCell, covering entire query time period (instead of looping multiple times by dayBucket)
   *
   * Flow:
   * 1. Extract sensorId, time range, spatial range from conditions
   * 2. Use Z3DEncoder.cells(...) to enumerate multi-level grid blocks covering BBox
   * 3. Calculate startDayBucket, endDayBucket, startTimeOfDay, endTimeOfDay for overall time range
   * 4. For each zCell, construct one precise Scan with continuous time range, covering entire query time period
   * 5. Read rowKey from unified index, then read complete record from main table
   * 6. Perform final filtering on all records (avoid boundary errors)
   * 7. Return precise results
   */
  private def queryUsingUnifiedIndex(
    sensorCond: SelectivityEstimator.SensorIdEquals,
    timeCond: SelectivityEstimator.TimeRange,
    spatialCond: SelectivityEstimator.SpatialBBox,
    allConds: Seq[SelectivityEstimator.QueryCondition]
  ): Iterator[SensorRecord] = {
    val globalStartTime = System.currentTimeMillis()
    val rowKeys = mutable.Set[String]()

    val sensorId = sensorCond.sensorId
    val fromTs = timeCond.startTime
    val toTs = timeCond.endTime
    val lonMin = spatialCond.lonMin
    val latMin = spatialCond.latMin
    val altMin = spatialCond.altMin
    val lonMax = spatialCond.lonMax
    val latMax = spatialCond.latMax
    val altMax = spatialCond.altMax

    // 1. Enumerate multi-level grid blocks (replacing original Z3DEncoder.ranges)
    val lvl = unifiedLevel.getOrElse(Z3DEncoder.UNIFIED_BLOCK_LEVEL)
    val zCells = Z3DEncoder.cellsAtLevel(
      lonMin, latMin, altMin,
      lonMax, latMax, altMax,
      level = lvl,
      maxCells = 1000000
    )

    // 2. Calculate dayBucket and timeOfDay for overall time range
    val startDayBucket = TimeBucket.dayBucket(fromTs)
    val startTimeOfDay = TimeBucket.millisOfDay(fromTs)
    val endDayBucket = TimeBucket.dayBucket(toTs)
    val endTimeOfDay = TimeBucket.millisOfDay(toTs)

    // 3. Phase 0: Generate task unit list
    val tasks: Seq[UnifiedChunkTask] = zCells.map { zCell =>
      UnifiedChunkTask(sensorId, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
    }

    // 4. Phase 1: Choose serial or parallel execution based on switch (initial filter)
    val step1StartTime = System.currentTimeMillis()
    if (!useUnifiedParallel) {
      executeUnifiedSerial(tasks, rowKeys)
    } else {
      executeUnifiedParallel(tasks, rowKeys)
    }
    val step1Time = System.currentTimeMillis() - step1StartTime

    // 5. Read complete records from main table, and perform secondary filter
    if (rowKeys.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime

      // Output logs after all timing is complete
      println("\n" + "=" * 80)
      println("[Unified-Query] Execution plan:")
      println("  Initial filter: SensorId + Time + Space (unified index scan)")
      println("  Secondary filter: Spatial precise check (coordinate range validation)"))
      println("=" * 80)
      println(s"\n[Execution Parameters]")
      println(s"  → Block task count: ${tasks.size}")
      println(s"\n[Initial Filter] SensorId + Time + Space unified index scan")
      println(s"  → Result: ${rowKeys.size} records | Time: ${step1Time}ms")
      println("=" * 80)
      println(s"[Unified-Query-Summary]")
      println(s"  → Initial filter time: ${step1Time}ms")
      println(s"  → Secondary filter time: 0ms")
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      Iterator.empty
    } else {
      val step2StartTime = System.currentTimeMillis()
      val dataTable = connection.getTable(TableName.valueOf(HBaseTableManager.dataTableName(dataset)))
      try {
        val gets = rowKeys.map(k => new Get(Bytes.toBytes(k))).toList.asJava
        val results: Array[Result] = dataTable.get(gets)

        val cf = HBaseTableManager.CF_BYTES
        val acceptedRecords = mutable.ListBuffer[SensorRecord]()
        val rejectedRecords = mutable.ListBuffer[SensorRecord]()
        val missingRecords = mutable.ListBuffer[String]()

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

            // Perform final filtering on all conditions
            val matchesAll = allConds.forall { cond =>
              cond match {
                case SelectivityEstimator.SensorIdEquals(id) => record.sensorId == id
                case SelectivityEstimator.TimeRange(startTime, endTime) => record.time >= startTime && record.time <= endTime
                case bbox: SelectivityEstimator.SpatialBBox =>
                  record.longitude >= bbox.lonMin && record.longitude <= bbox.lonMax &&
                    record.latitude >= bbox.latMin && record.latitude <= bbox.latMax &&
                    record.altitude >= bbox.altMin && record.altitude <= bbox.altMax
                case SelectivityEstimator.TypeEquals(t) => record.sensorType == t
              }
            }

            if (matchesAll) {
              acceptedRecords += record
            } else {
              rejectedRecords += record
            }
          } else {
            // Record empty or non-existent rows
            val rowKey = if (r != null) Bytes.toString(r.getRow) else "unknown"
            missingRecords += rowKey
          }
        }

        // Export data eliminated by precise filtering
        if (rejectedRecords.nonEmpty) {
          exportRejectedToCsv(rejectedRecords.toSeq)
        }

        // Export data that cannot be read from main table
        if (missingRecords.nonEmpty) {
          exportMissingRecordKeys(missingRecords.toSeq)
        }

        val step2Time = System.currentTimeMillis() - step2StartTime
        val totalTime = System.currentTimeMillis() - globalStartTime

        // Output logs after all timing is complete
        println("\n" + "=" * 80)
        println("[Unified-Query] Execution plan:")
        println("  Initial filter: SensorId + Time + Space (unified index scan)")
        println("  Secondary filter: Spatial precise check (coordinate range validation)"))
        println("=" * 80)
        println(s"\n[Execution Parameters]")
        println(s"  → Block task count: ${tasks.size}")
        println(s"\n[Initial Filter] SensorId + Time + Space unified index scan")
        println(s"  → Result: ${rowKeys.size} records | Time: ${step1Time}ms")
        println(s"\n[Secondary Filter] Spatial precise check (coordinate range validation)"))
        println(s"  → Result: ${acceptedRecords.size} records | Time: ${step2Time}ms")
        println("=" * 80)
        println(s"[Unified-Query-Summary]")
        println(s"  → Initial filter time: ${step1Time}ms")
        println(s"  → Secondary filter time: ${step2Time}ms (includes data read and coordinate validation)"))
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        acceptedRecords.iterator
      } finally {
        dataTable.close()
      }
    }
  }

  /**
   * Fetch complete records by ID set
   * Fix: Use Array instead of Iterator
   */
  private def fetchRecords(ids: Set[String]): Iterator[SensorRecord] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.dataTableName(dataset)))

    try {
      val recordsBuf = mutable.ArrayBuffer[SensorRecord]()
      val cf = HBaseTableManager.CF_BYTES

      forEachResultByBatchedGet(table, ids) { id =>
        new Get(Bytes.toBytes(id))
      } { r =>
        if (r != null && !r.isEmpty) {
          recordsBuf += SensorRecord(
            rowKey = Bytes.toString(r.getRow),
            time = Bytes.toLong(r.getValue(cf, Bytes.toBytes("time"))),
            sensorId = Bytes.toString(r.getValue(cf, Bytes.toBytes("sensor_id"))),
            longitude = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lon"))),
            latitude = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lat"))),
            altitude = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("alt"))),
            sensorType = Bytes.toString(r.getValue(cf, Bytes.toBytes("type")))
          )
        }
      }

      recordsBuf.iterator

    } finally {
      table.close()
    }
  }

  /**
   * Export data eliminated by precise filtering to CSV file
   * These are records that passed unified index initial filter but don't satisfy all conditions
   *
   * @param rejectedRecords List of rejected records
   */
  private def exportRejectedToCsv(rejectedRecords: Seq[SensorRecord]): Unit = {
    try {
      val debugDir = "/test/sensor-spatial-index/debug"
      val dir = new java.io.File(debugDir)
      if (!dir.exists()) {
        dir.mkdirs()
      }

      val timestamp = System.currentTimeMillis()
      val filePath = s"$debugDir/unified_rejected_$timestamp.csv"
      val writer = new java.io.PrintWriter(new java.io.File(filePath))

      try {
        // Write CSV header
        writer.println("RowKey,Time,TimeStamp,SensorId,Longitude,Latitude,Altitude,Type")

        // Write data rows
        rejectedRecords.foreach { record =>
          val timeStr = formatTimestamp(record.time)
          writer.println(s"${record.rowKey},$timeStr,${record.time},${record.sensorId},${record.longitude},${record.latitude},${record.altitude},${record.sensorType}")
        }

        println(s"[Unified] Exported ${rejectedRecords.size} rejected records to $filePath")
      } finally {
        writer.close()
      }
    } catch {
      case ex: Exception =>
        println(s"[Unified] Warning: Failed to export rejected records to CSV: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }

  /**
   * Export rowKeys that cannot be read from main table to CSV file
   * These are records that passed unified index initial filter but cannot be found in main data table
   * Possible reasons: data inconsistency, stale index, main table deletion, etc.
   *
   * @param missingRowKeys List of missing row keys
   */
  private def exportMissingRecordKeys(missingRowKeys: Seq[String]): Unit = {
    try {
      val debugDir = "/test/sensor-spatial-index/debug"
      val dir = new java.io.File(debugDir)
      if (!dir.exists()) {
        dir.mkdirs()
      }

      val timestamp = System.currentTimeMillis()
      val filePath = s"$debugDir/unified_missing_$timestamp.csv"
      val writer = new java.io.PrintWriter(new java.io.File(filePath))

      try {
        // Write CSV header
        writer.println("RowKey,Status")

        // Write data rows
        missingRowKeys.foreach { rowKey =>
          writer.println(s"$rowKey,MISSING_IN_DATA_TABLE")
        }

        println(s"[Unified] Exported ${missingRowKeys.size} missing rowKeys to $filePath")
      } finally {
        writer.close()
      }
    } catch {
      case ex: Exception =>
        println(s"[Unified] Warning: Failed to export missing rowKeys to CSV: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }

  /**
   * Format timestamp to string (yyyy-MM-dd'T'HH:mm:ss)
   */
  private def formatTimestamp(timestamp: Long): String = {
    val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    java.time.Instant.ofEpochMilli(timestamp)
      .atZone(java.time.ZoneId.systemDefault())
      .toLocalDateTime
      .format(formatter)
  }

  /**
   * Sort conditions for incremental filter: Attribute → Spatial → Time
   * This is the sorting method specific to incremental filtering
   */
  private def sortConditionsForIncrementalFilter(conditions: Seq[QueryCondition]): Seq[QueryCondition] = {
    val sorted = scala.collection.mutable.ListBuffer[QueryCondition]()

    // Step 1: Add all attribute conditions (SensorId and Type)
    conditions.foreach {
      case sensor: SelectivityEstimator.SensorIdEquals => sorted += sensor
      case typeEq: SelectivityEstimator.TypeEquals => sorted += typeEq
      case _ =>
    }

    // Step 2: Add spatial condition
    conditions.foreach {
      case spatial: SelectivityEstimator.SpatialBBox  => sorted += spatial
      case _ =>
    }

    // Step 3: Add time condition
    conditions.foreach {
      case time: SelectivityEstimator.TimeRange => sorted += time
      case _ =>
    }

    sorted.toSeq
  }

  def close(): Unit = {
    connection.close()
  }
}