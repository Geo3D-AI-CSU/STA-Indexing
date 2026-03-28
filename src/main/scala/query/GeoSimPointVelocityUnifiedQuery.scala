package query

import index.{Z3DEncoder, TimeBucket, UnifiedIndexKey, GeoSimCoordMapper, GeoSimVelocityBucket}
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.concurrent.Executors

class GeoSimPointVelocityUnifiedQuery(
  zkQuorum: String,
  useUnifiedParallel: Boolean = false,
  unifiedThreadPoolSize: Int = 8,
  dataset: Option[String] = None,
  unifiedLevel: Option[Int] = None
) {

  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", zkQuorum)
  private val connection = ConnectionFactory.createConnection(conf)

  private var metaDelimiterName: String = ""
  private var metaHeaderLine: String = ""

  private case class ScanTask(bucketEnc: Int, zCell: Long)

  def queryRawLinesByVelocity(
    vxMin: Double, vxMax: Double,
    vyMin: Double, vyMax: Double,
    vzMin: Double, vzMax: Double,
    startMs: Long, endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double
  ): (Seq[String], String, String) = {

    val globalStartTime = System.currentTimeMillis()

    val (bounds, delimiterName, headerLine, velParams) = GeoSimCoordMapper.readMetaWithVelocity(connection, dataset)
    metaDelimiterName = delimiterName
    metaHeaderLine = headerLine

    val (mappedLonMin, mappedLatMin, mappedAltMin, mappedLonMax, mappedLatMax, mappedAltMax) =
      GeoSimCoordMapper.mapBBox(bounds, xMin, yMin, zMin, xMax, yMax, zMax)

    val lvl = unifiedLevel.getOrElse(Z3DEncoder.UNIFIED_BLOCK_LEVEL)

    val (vxBucketMin, vxBucketMax) = GeoSimVelocityBucket.bucketRange(vxMin, vxMax, velParams.v0, velParams.dvx, velParams.method)
    val (vyBucketMin, vyBucketMax) = GeoSimVelocityBucket.bucketRange(vyMin, vyMax, velParams.v0, velParams.dvy, velParams.method)
    val (vzBucketMin, vzBucketMax) = GeoSimVelocityBucket.bucketRange(vzMin, vzMax, velParams.v0, velParams.dvz, velParams.method)

    val vxBuckets = (vxBucketMin to vxBucketMax).toSeq
    val vyBuckets = (vyBucketMin to vyBucketMax).toSeq
    val vzBuckets = (vzBucketMin to vzBucketMax).toSeq

    val vxBucketsCount = vxBuckets.size
    val vyBucketsCount = vyBuckets.size
    val vzBucketsCount = vzBuckets.size

    val zCells = Z3DEncoder.cellsAtLevel(
      mappedLonMin, mappedLatMin, mappedAltMin,
      mappedLonMax, mappedLatMax, mappedAltMax,
      level = lvl,
      maxCells = 1000000
    )
    val zCellsCount = zCells.size

    val tasksCountX = vxBucketsCount * zCellsCount
    val tasksCountY = vyBucketsCount * zCellsCount
    val tasksCountZ = vzBucketsCount * zCellsCount

    val startDayBucket = TimeBucket.dayBucket(startMs)
    val startTimeOfDay = TimeBucket.millisOfDay(startMs)
    val endDayBucket = TimeBucket.dayBucket(endMs)
    val endTimeOfDay = TimeBucket.millisOfDay(endMs)

    val step1StartTime = System.currentTimeMillis()

    val scanVxStart = System.currentTimeMillis()
    val sx = scanVelocityUnifiedIndex(
      HBaseTableManager.geoSimPointVxUnifiedIdxTableName(dataset, unifiedLevel),
      vxBucketMin, vxBucketMax, zCells,
      startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay,
      UnifiedIndexKey.buildScanRangeForVxCellTimeSpan
    ).toSet
    val scanVxMs = System.currentTimeMillis() - scanVxStart

    val scanVyStart = System.currentTimeMillis()
    val sy = scanVelocityUnifiedIndex(
      HBaseTableManager.geoSimPointVyUnifiedIdxTableName(dataset, unifiedLevel),
      vyBucketMin, vyBucketMax, zCells,
      startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay,
      UnifiedIndexKey.buildScanRangeForVyCellTimeSpan
    ).toSet
    val scanVyMs = System.currentTimeMillis() - scanVyStart

    val scanVzStart = System.currentTimeMillis()
    val sz = scanVelocityUnifiedIndex(
      HBaseTableManager.geoSimPointVzUnifiedIdxTableName(dataset, unifiedLevel),
      vzBucketMin, vzBucketMax, zCells,
      startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay,
      UnifiedIndexKey.buildScanRangeForVzCellTimeSpan
    ).toSet
    val scanVzMs = System.currentTimeMillis() - scanVzStart

    val scanTotalMs = scanVxMs + scanVyMs + scanVzMs

    val step1Time = System.currentTimeMillis() - step1StartTime

    val intersection = sx.intersect(sy).intersect(sz)
    val intersectionSize = intersection.size

    if (intersection.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      println("\n" + "=" * 80)
      println("[Unified-Query] Execution plan:")
      println("  Initial filter: VxBucket + VyBucket + VzBucket + Time + Space (velocity unified index scan)")
      println("  Secondary filter: Spatial precise check + Velocity precise check (coordinate range + velocity range validation)")
      println("=" * 80)
      println(s"\n[Execution Parameters]")
      println(s"  → dvx: ${velParams.dvx}, dvy: ${velParams.dvy}, dvz: ${velParams.dvz}")
      println(s"  → method: ${velParams.method}")
      println(s"  → mode: ${if (useUnifiedParallel) "parallel" else "serial"}")
      if (useUnifiedParallel) {
        println(s"  → unifiedThreadPoolSize: $unifiedThreadPoolSize")
      }
      println(s"  → vx bucket count: ${vxBucketsCount}")
      println(s"  → vy bucket count: ${vyBucketsCount}")
      println(s"  → vz bucket count: ${vzBucketsCount}")
      println(s"  → Spatial grid count: ${zCellsCount}")
      println(s"  → Block task count: X=${tasksCountX}, Y=${tasksCountY}, Z=${tasksCountZ}")
      println(s"\n[Initial Filter] VxIndex scan time: ${scanVxMs}ms")
      println(s"  → VyIndex scan time: ${scanVyMs}ms")
      println(s"  → VzIndex scan time: ${scanVzMs}ms")
      println(s"  → scanTotalMs: ${scanTotalMs}ms")
      println(s"  → Sx.size: ${sx.size}")
      println(s"  → Sy.size: ${sy.size}")
      println(s"  → Sz.size: ${sz.size}")
      println(s"  → Intersection candidate count: ${intersectionSize}")
      println("=" * 80)
      println(s"[Unified-Query-Summary]")
      println(s"  → Initial filter time: ${step1Time}ms")
      println(s"  → Secondary filter time: 0ms")
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      return (Seq.empty, delimiterName, headerLine)
    }

    val step2StartTime = System.currentTimeMillis()
    val acceptedRawLines = fetchRawLines(intersection, vxMin, vxMax, vyMin, vyMax, vzMin, vzMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
    val step2Time = System.currentTimeMillis() - step2StartTime
    val acceptedCount = acceptedRawLines.size

    val totalTime = System.currentTimeMillis() - globalStartTime
    println("\n" + "=" * 80)
    println("[Unified-Query] Execution plan:")
    println("  Initial filter: VxBucket + VyBucket + VzBucket + Time + Space (velocity unified index scan)")
    println("  Secondary filter: Spatial precise check + Velocity precise check (coordinate range + velocity range validation)")
    println("=" * 80)
    println(s"\n[Execution Parameters]")
    println(s"  → dvx: ${velParams.dvx}, dvy: ${velParams.dvy}, dvz: ${velParams.dvz}")
    println(s"  → method: ${velParams.method}")
    println(s"  → mode: ${if (useUnifiedParallel) "parallel" else "serial"}")
    if (useUnifiedParallel) {
      println(s"  → unifiedThreadPoolSize: $unifiedThreadPoolSize")
    }
    println(s"  → vx bucket count: ${vxBucketsCount}")
    println(s"  → vy bucket count: ${vyBucketsCount}")
    println(s"  → vz bucket count: ${vzBucketsCount}")
    println(s"  → Spatial grid count: ${zCellsCount}")
    println(s"  → Block task count: X=${tasksCountX}, Y=${tasksCountY}, Z=${tasksCountZ}")
    println(s"\n[Initial Filter] VxIndex scan time: ${scanVxMs}ms")
    println(s"  → VyIndex scan time: ${scanVyMs}ms")
    println(s"  → VzIndex scan time: ${scanVzMs}ms")
    println(s"  → scanTotalMs: ${scanTotalMs}ms")
    println(s"  → Sx.size: ${sx.size}")
    println(s"  → Sy.size: ${sy.size}")
    println(s"  → Sz.size: ${sz.size}")
    println(s"  → Intersection candidate count: ${intersectionSize}")
    println(s"\n[Secondary Filter] Spatial precise check + Velocity precise check (coordinate range + velocity range validation)")
    println(s"  → Result: ${acceptedCount} records | Time: ${step2Time}ms")
    println("=" * 80)
    println(s"[Unified-Query-Summary]")
    println(s"  → Initial filter time: ${step1Time}ms")
    println(s"  → Secondary filter time: ${step2Time}ms (includes data read and coordinate/velocity validation)")
    println(s"  → Total query time: ${totalTime}ms")
    println("=" * 80 + "\n")

    (acceptedRawLines, delimiterName, headerLine)
  }

  private def scanVelocityUnifiedIndex(
    tableName: String,
    bucketRawMin: Int,
    bucketRawMax: Int,
    zCells: Seq[Long],
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int,
    buildRange: (Int, Long, Int, Int, Int, Int) => (Array[Byte], Array[Byte])
  ): mutable.Set[String] = {
    if (useUnifiedParallel) {
      val tasks = for {
        bucketRaw <- bucketRawMin to bucketRawMax
        zCell <- zCells
      } yield ScanTask(GeoSimVelocityBucket.encodeBucket(bucketRaw), zCell)

      val tasksPerThread = (tasks.size + unifiedThreadPoolSize - 1) / unifiedThreadPoolSize
      val pool = Executors.newFixedThreadPool(unifiedThreadPoolSize)

      try {
        val localRowKeysList = scala.collection.mutable.ListBuffer[mutable.Set[String]]()
        val lockObj = new Object()

        val futures = (0 until unifiedThreadPoolSize).map { threadIdx =>
          val startIdx = threadIdx * tasksPerThread
          val endIdx = math.min(startIdx + tasksPerThread, tasks.length)

          pool.submit(new Runnable {
            override def run(): Unit = {
              if (startIdx < tasks.length) {
                val assignedTasks = tasks.slice(startIdx, endIdx)
                val localSet = mutable.Set[String]()

                val table = connection.getTable(TableName.valueOf(tableName))
                try {
                  assignedTasks.foreach { task =>
                    val (startRowBytes, stopRowBytes) = buildRange(
                      task.bucketEnc, task.zCell,
                      startDayBucket, startTimeOfDay,
                      endDayBucket, endTimeOfDay
                    )

                    val scan = new Scan()
                    scan.withStartRow(startRowBytes)
                    scan.withStopRow(stopRowBytes)

                    val scanner = table.getScanner(scan)
                    try {
                      scanner.asScala.foreach { result =>
                        val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
                        if (dkBytes != null) {
                          val dataKey = Bytes.toString(dkBytes)
                          localSet += dataKey
                        } else {
                          val rowKeyBytes = result.getRow
                          if (rowKeyBytes.length > 22) {
                            val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                            localSet += dataKey
                          }
                        }
                      }
                    } finally {
                      scanner.close()
                    }
                  }
                } finally {
                  table.close()
                }

                lockObj.synchronized {
                  localRowKeysList += localSet
                }
              }
            }
          })
        }

        futures.foreach(_.get())
        mutable.Set.empty[String] ++ localRowKeysList.flatten
      } finally {
        pool.shutdown()
      }
    } else {
      val rowKeys = mutable.Set[String]()
      val table = connection.getTable(TableName.valueOf(tableName))

      try {
        for (bucketRaw <- bucketRawMin to bucketRawMax; zCell <- zCells) {
          val bucketEnc = GeoSimVelocityBucket.encodeBucket(bucketRaw)
          val (startRowBytes, stopRowBytes) = buildRange(
            bucketEnc, zCell,
            startDayBucket, startTimeOfDay,
            endDayBucket, endTimeOfDay
          )

          val scan = new Scan()
          scan.withStartRow(startRowBytes)
          scan.withStopRow(stopRowBytes)

          val scanner = table.getScanner(scan)
          try {
            scanner.asScala.foreach { result =>
              val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
              if (dkBytes != null) {
                val dataKey = Bytes.toString(dkBytes)
                rowKeys += dataKey
              } else {
                val rowKeyBytes = result.getRow
                if (rowKeyBytes.length > 22) {
                  val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                  rowKeys += dataKey
                }
              }
            }
          } finally {
            scanner.close()
          }
        }
      } finally {
        table.close()
      }

      rowKeys
    }
  }

  private def executeUnifiedSerialVx(vxBuckets: Seq[Int], zCells: Seq[Long],
                                     startDayBucket: Int, startTimeOfDay: Int,
                                     endDayBucket: Int, endTimeOfDay: Int,
                                     velParams: GeoSimVelocityBucket.VelBucketParams): Set[String] = {
    val rowKeys = mutable.Set[String]()
    val unifiedTable = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVxUnifiedIdxTableName(dataset, unifiedLevel)))

    try {
      for (vxBucket <- vxBuckets; zCell <- zCells) {
        val vxEnc = GeoSimVelocityBucket.encodeBucket(vxBucket)
        val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForVxCellTimeSpan(
          vxEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
        )

        val scan = new Scan()
        scan.withStartRow(startRowBytes)
        scan.withStopRow(stopRowBytes)

        val scanner = unifiedTable.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
            if (dkBytes != null) {
              val dataKey = Bytes.toString(dkBytes)
              rowKeys += dataKey
            } else {
              val rowKeyBytes = result.getRow
              if (rowKeyBytes.length > 22) {
                val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                rowKeys += dataKey
              }
            }
          }
        } finally {
          scanner.close()
        }
      }
    } finally {
      unifiedTable.close()
    }

    rowKeys.toSet
  }

  private def executeUnifiedSerialVy(vyBuckets: Seq[Int], zCells: Seq[Long],
                                     startDayBucket: Int, startTimeOfDay: Int,
                                     endDayBucket: Int, endTimeOfDay: Int,
                                     velParams: GeoSimVelocityBucket.VelBucketParams): Set[String] = {
    val rowKeys = mutable.Set[String]()
    val unifiedTable = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVyUnifiedIdxTableName(dataset, unifiedLevel)))

    try {
      for (vyBucket <- vyBuckets; zCell <- zCells) {
        val vyEnc = GeoSimVelocityBucket.encodeBucket(vyBucket)
        val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForVyCellTimeSpan(
          vyEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
        )

        val scan = new Scan()
        scan.withStartRow(startRowBytes)
        scan.withStopRow(stopRowBytes)

        val scanner = unifiedTable.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
            if (dkBytes != null) {
              val dataKey = Bytes.toString(dkBytes)
              rowKeys += dataKey
            } else {
              val rowKeyBytes = result.getRow
              if (rowKeyBytes.length > 22) {
                val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                rowKeys += dataKey
              }
            }
          }
        } finally {
          scanner.close()
        }
      }
    } finally {
      unifiedTable.close()
    }

    rowKeys.toSet
  }

  private def executeUnifiedSerialVz(vzBuckets: Seq[Int], zCells: Seq[Long],
                                     startDayBucket: Int, startTimeOfDay: Int,
                                     endDayBucket: Int, endTimeOfDay: Int,
                                     velParams: GeoSimVelocityBucket.VelBucketParams): Set[String] = {
    val rowKeys = mutable.Set[String]()
    val unifiedTable = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVzUnifiedIdxTableName(dataset, unifiedLevel)))

    try {
      for (vzBucket <- vzBuckets; zCell <- zCells) {
        val vzEnc = GeoSimVelocityBucket.encodeBucket(vzBucket)
        val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForVzCellTimeSpan(
          vzEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
        )

        val scan = new Scan()
        scan.withStartRow(startRowBytes)
        scan.withStopRow(stopRowBytes)

        val scanner = unifiedTable.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
            if (dkBytes != null) {
              val dataKey = Bytes.toString(dkBytes)
              rowKeys += dataKey
            } else {
              val rowKeyBytes = result.getRow
              if (rowKeyBytes.length > 22) {
                val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                rowKeys += dataKey
              }
            }
          }
        } finally {
          scanner.close()
        }
      }
    } finally {
      unifiedTable.close()
    }

    rowKeys.toSet
  }

  private def executeUnifiedParallelVx(vxBuckets: Seq[Int], zCells: Seq[Long],
                                        startDayBucket: Int, startTimeOfDay: Int,
                                        endDayBucket: Int, endTimeOfDay: Int,
                                        velParams: GeoSimVelocityBucket.VelBucketParams): Set[String] = {
    val threadPool = java.util.concurrent.Executors.newFixedThreadPool(unifiedThreadPoolSize)
    val localRowKeysList = scala.collection.mutable.ListBuffer[mutable.Set[String]]()
    val lockObj = new Object()

    try {
      val allTasks = for (vxBucket <- vxBuckets; zCell <- zCells) yield (vxBucket, zCell)
      val tasksPerThread = (allTasks.length + unifiedThreadPoolSize - 1) / unifiedThreadPoolSize

      val futures = (0 until unifiedThreadPoolSize).map { threadIdx =>
        val startIdx = threadIdx * tasksPerThread
        val endIdx = math.min(startIdx + tasksPerThread, allTasks.length)

        threadPool.submit(new Runnable {
          override def run(): Unit = {
            if (startIdx < allTasks.length) {
              val assignedTasks = allTasks.slice(startIdx, endIdx)
              val localRowKeys = mutable.Set[String]()

              val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVxUnifiedIdxTableName(dataset, unifiedLevel)))
              try {
                assignedTasks.foreach { case (vxBucket, zCell) =>
                  val vxEnc = GeoSimVelocityBucket.encodeBucket(vxBucket)
                  val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForVxCellTimeSpan(
                    vxEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
                  )

                  val scan = new Scan()
                  scan.withStartRow(startRowBytes)
                  scan.withStopRow(stopRowBytes)

                  val scanner = table.getScanner(scan)
                  try {
                    scanner.asScala.foreach { result =>
                      val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
                      if (dkBytes != null) {
                        val dataKey = Bytes.toString(dkBytes)
                        localRowKeys += dataKey
                      } else {
                        val rowKeyBytes = result.getRow
                        if (rowKeyBytes.length > 22) {
                          val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                          localRowKeys += dataKey
                        }
                      }
                    }
                  } finally {
                    scanner.close()
                  }
                }
              } finally {
                table.close()
              }

              lockObj.synchronized {
                localRowKeysList += localRowKeys
              }
            }
          }
        })
      }

      futures.foreach(_.get())

    } finally {
      threadPool.shutdown()
    }

    localRowKeysList.flatten.toSet
  }

  private def executeUnifiedParallelVy(vyBuckets: Seq[Int], zCells: Seq[Long],
                                        startDayBucket: Int, startTimeOfDay: Int,
                                        endDayBucket: Int, endTimeOfDay: Int,
                                        velParams: GeoSimVelocityBucket.VelBucketParams): Set[String] = {
    val threadPool = java.util.concurrent.Executors.newFixedThreadPool(unifiedThreadPoolSize)
    val localRowKeysList = scala.collection.mutable.ListBuffer[mutable.Set[String]]()
    val lockObj = new Object()

    try {
      val allTasks = for (vyBucket <- vyBuckets; zCell <- zCells) yield (vyBucket, zCell)
      val tasksPerThread = (allTasks.length + unifiedThreadPoolSize - 1) / unifiedThreadPoolSize

      val futures = (0 until unifiedThreadPoolSize).map { threadIdx =>
        val startIdx = threadIdx * tasksPerThread
        val endIdx = math.min(startIdx + tasksPerThread, allTasks.length)

        threadPool.submit(new Runnable {
          override def run(): Unit = {
            if (startIdx < allTasks.length) {
              val assignedTasks = allTasks.slice(startIdx, endIdx)
              val localRowKeys = mutable.Set[String]()

              val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVyUnifiedIdxTableName(dataset, unifiedLevel)))
              try {
                assignedTasks.foreach { case (vyBucket, zCell) =>
                  val vyEnc = GeoSimVelocityBucket.encodeBucket(vyBucket)
                  val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForVyCellTimeSpan(
                    vyEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
                  )

                  val scan = new Scan()
                  scan.withStartRow(startRowBytes)
                  scan.withStopRow(stopRowBytes)

                  val scanner = table.getScanner(scan)
                  try {
                    scanner.asScala.foreach { result =>
                      val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
                      if (dkBytes != null) {
                        val dataKey = Bytes.toString(dkBytes)
                        localRowKeys += dataKey
                      } else {
                        val rowKeyBytes = result.getRow
                        if (rowKeyBytes.length > 22) {
                          val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                          localRowKeys += dataKey
                        }
                      }
                    }
                  } finally {
                    scanner.close()
                  }
                }
              } finally {
                table.close()
              }

              lockObj.synchronized {
                localRowKeysList += localRowKeys
              }
            }
          }
        })
      }

      futures.foreach(_.get())

    } finally {
      threadPool.shutdown()
    }

    localRowKeysList.flatten.toSet
  }

  private def executeUnifiedParallelVz(vzBuckets: Seq[Int], zCells: Seq[Long],
                                        startDayBucket: Int, startTimeOfDay: Int,
                                        endDayBucket: Int, endTimeOfDay: Int,
                                        velParams: GeoSimVelocityBucket.VelBucketParams): Set[String] = {
    val threadPool = java.util.concurrent.Executors.newFixedThreadPool(unifiedThreadPoolSize)
    val localRowKeysList = scala.collection.mutable.ListBuffer[mutable.Set[String]]()
    val lockObj = new Object()

    try {
      val allTasks = for (vzBucket <- vzBuckets; zCell <- zCells) yield (vzBucket, zCell)
      val tasksPerThread = (allTasks.length + unifiedThreadPoolSize - 1) / unifiedThreadPoolSize

      val futures = (0 until unifiedThreadPoolSize).map { threadIdx =>
        val startIdx = threadIdx * tasksPerThread
        val endIdx = math.min(startIdx + tasksPerThread, allTasks.length)

        threadPool.submit(new Runnable {
          override def run(): Unit = {
            if (startIdx < allTasks.length) {
              val assignedTasks = allTasks.slice(startIdx, endIdx)
              val localRowKeys = mutable.Set[String]()

              val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVzUnifiedIdxTableName(dataset, unifiedLevel)))
              try {
                assignedTasks.foreach { case (vzBucket, zCell) =>
                  val vzEnc = GeoSimVelocityBucket.encodeBucket(vzBucket)
                  val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForVzCellTimeSpan(
                    vzEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
                  )

                  val scan = new Scan()
                  scan.withStartRow(startRowBytes)
                  scan.withStopRow(stopRowBytes)

                  val scanner = table.getScanner(scan)
                  try {
                    scanner.asScala.foreach { result =>
                      val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
                      if (dkBytes != null) {
                        val dataKey = Bytes.toString(dkBytes)
                        localRowKeys += dataKey
                      } else {
                        val rowKeyBytes = result.getRow
                        if (rowKeyBytes.length > 22) {
                          val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                          localRowKeys += dataKey
                        }
                      }
                    }
                  } finally {
                    scanner.close()
                  }
                }
              } finally {
                table.close()
              }

              lockObj.synchronized {
                localRowKeysList += localRowKeys
              }
            }
          }
        })
      }

      futures.foreach(_.get())

    } finally {
      threadPool.shutdown()
    }

    localRowKeysList.flatten.toSet
  }

  private def fetchRawLines(rowKeys: Set[String],
                             vxMin: Double, vxMax: Double,
                             vyMin: Double, vyMax: Double,
                             vzMin: Double, vzMax: Double,
                             startMs: Long, endMs: Long,
                             xMin: Double, yMin: Double, zMin: Double,
                             xMax: Double, yMax: Double, zMax: Double): Seq[String] = {
    val dataTable = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointDataTableName(dataset)))
    val cf = HBaseTableManager.CF_BYTES
    val results = mutable.ArrayBuffer[String]()

    try {
      val gets = rowKeys.map { key =>
        new Get(Bytes.toBytes(key))
          .addColumn(cf, Bytes.toBytes("time"))
          .addColumn(cf, Bytes.toBytes("lon"))
          .addColumn(cf, Bytes.toBytes("lat"))
          .addColumn(cf, Bytes.toBytes("alt"))
          .addColumn(cf, Bytes.toBytes("vx"))
          .addColumn(cf, Bytes.toBytes("vy"))
          .addColumn(cf, Bytes.toBytes("vz"))
          .addColumn(cf, Bytes.toBytes("raw_line"))
      }.toList

      val resultList = dataTable.get(gets.asJava)

      resultList.foreach { result =>
        if (result != null && !result.isEmpty) {
          val time = Bytes.toLong(result.getValue(cf, Bytes.toBytes("time")))
          val x = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("lon")))
          val y = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("lat")))
          val z = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("alt")))
          val vx = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("vx")))
          val vy = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("vy")))
          val vz = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("vz")))
          val rawLine = Bytes.toString(result.getValue(cf, Bytes.toBytes("raw_line")))

          if (time >= startMs && time <= endMs &&
              x >= xMin && x <= xMax &&
              y >= yMin && y <= yMax &&
              z >= zMin && z <= zMax &&
              vx >= vxMin && vx <= vxMax &&
              vy >= vyMin && vy <= vyMax &&
              vz >= vzMin && vz <= vzMax) {
            results += rawLine
          }
        }
      }
    } finally {
      dataTable.close()
    }

    results.toSeq
  }

  def close(): Unit = {
    connection.close()
  }

  def getMeta: (String, String) = (metaDelimiterName, metaHeaderLine)
}
