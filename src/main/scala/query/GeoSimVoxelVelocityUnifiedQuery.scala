package query

import index.{GeoSimVoxelGrid, GeoSimVoxelVelocityUnifiedIndexKey, VolumeTimeBucketUtc}
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.concurrent.Executors

class GeoSimVoxelVelocityUnifiedQuery(
  zkQuorum: String,
  useUnifiedParallel: Boolean = false,
  unifiedThreadPoolSize: Int = 8,
  dataset: Option[String] = None,
  unifiedLevel: Option[Int] = None
) {

  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", zkQuorum)
  private val connection = ConnectionFactory.createConnection(conf)

  private var metaHeaderLine: String = ""

  private case class ScanTask(bucketEnc: Int, zCell: Long)

  private def scanOneComponent(
    tableName: String,
    bucketRawMin: Int,
    bucketRawMax: Int,
    zCells: Seq[Long],
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int,
    buildRange: (Int, Long, Int, Int, Int, Int) => (Array[Byte], Array[Byte])
  ): (Set[String], Long, Int) = {
    val rowKeys = mutable.Set[String]()
    var unifiedRowCount = 0
    val startTime = System.currentTimeMillis()

    if (useUnifiedParallel) {
      val threadPool = Executors.newFixedThreadPool(unifiedThreadPoolSize)
      val localRowKeysList = scala.collection.mutable.ListBuffer[mutable.Set[String]]()
      val lockObj = new Object()

      try {
        val allTasks = for (bucketRaw <- bucketRawMin to bucketRawMax; zCell <- zCells) yield {
          val bucketEnc = bucketRaw ^ 0x80000000
          ScanTask(bucketEnc, zCell)
        }
        val tasksPerThread = (allTasks.length + unifiedThreadPoolSize - 1) / unifiedThreadPoolSize

        val futures = (0 until unifiedThreadPoolSize).map { threadIdx =>
          val startIdx = threadIdx * tasksPerThread
          val endIdx = math.min(startIdx + tasksPerThread, allTasks.length)

          threadPool.submit(new Runnable {
            override def run(): Unit = {
              if (startIdx < allTasks.length) {
                val assignedTasks = allTasks.slice(startIdx, endIdx)
                val localRowKeys = mutable.Set[String]()
                var localUnifiedCount = 0

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
                          val brickRowKey = Bytes.toString(dkBytes)
                          localRowKeys += brickRowKey
                          localUnifiedCount += 1
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
                  unifiedRowCount += localUnifiedCount
                }
              }
            }
          })
        }

        futures.foreach(_.get())

      } finally {
        threadPool.shutdown()
      }

      rowKeys ++= localRowKeysList.flatten
      rowKeys.toSet
    } else {
      val unifiedTable = connection.getTable(TableName.valueOf(tableName))

      try {
        for (bucketRaw <- bucketRawMin to bucketRawMax; zCell <- zCells) {
          val bucketEnc = bucketRaw ^ 0x80000000
          val (startRowBytes, stopRowBytes) = buildRange(
            bucketEnc, zCell,
            startDayBucket, startTimeOfDay,
            endDayBucket, endTimeOfDay
          )

          val scan = new Scan()
          scan.withStartRow(startRowBytes)
          scan.withStopRow(stopRowBytes)

          val scanner = unifiedTable.getScanner(scan)
          try {
            scanner.asScala.foreach { result =>
              val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
              if (dkBytes != null) {
                val brickRowKey = Bytes.toString(dkBytes)
                rowKeys += brickRowKey
                unifiedRowCount += 1
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

    (rowKeys.toSet, System.currentTimeMillis() - startTime, unifiedRowCount)
  }

  def queryRawLinesByVelocity(
    vxMin: Double, vxMax: Double,
    vyMin: Double, vyMax: Double,
    vzMin: Double, vzMax: Double,
    startMs: Long, endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double
  ): (Seq[String], String) = {

    val globalStartTime = System.currentTimeMillis()

    val (gridMeta, headerLine, _, velParams) = GeoSimVoxelGrid.readMetaWithHeaderTempVel(connection, dataset)
    metaHeaderLine = headerLine

    val lvl = unifiedLevel.getOrElse(4)

    val v0 = velParams._1
    val method = velParams._2
    val dvx = velParams._3
    val dvy = velParams._4
    val dvz = velParams._5

    val (vxBucketRawMin, vxBucketRawMax) = (
      math.floor((vxMin - v0) / dvx).toInt,
      math.floor((vxMax - v0) / dvx).toInt
    )
    val (vyBucketRawMin, vyBucketRawMax) = (
      math.floor((vyMin - v0) / dvy).toInt,
      math.floor((vyMax - v0) / dvy).toInt
    )
    val (vzBucketRawMin, vzBucketRawMax) = (
      math.floor((vzMin - v0) / dvz).toInt,
      math.floor((vzMax - v0) / dvz).toInt
    )

    val vxBuckets = (vxBucketRawMin to vxBucketRawMax).toSeq
    val vyBuckets = (vyBucketRawMin to vyBucketRawMax).toSeq
    val vzBuckets = (vzBucketRawMin to vzBucketRawMax).toSeq

    val vxBucketsCount = vxBuckets.size
    val vyBucketsCount = vyBuckets.size
    val vzBucketsCount = vzBuckets.size

    val zCells = GeoSimVoxelGrid.enumerateZCellsForBBox(
      gridMeta, xMin, yMin, zMin, xMax, yMax, zMax, lvl, maxCells = 1000000
    )
    val zCellsCount = zCells.size

    val tasksCountX = vxBucketsCount * zCellsCount
    val tasksCountY = vyBucketsCount * zCellsCount
    val tasksCountZ = vzBucketsCount * zCellsCount

    val startDayBucket = VolumeTimeBucketUtc.dayBucket(startMs)
    val startTimeOfDay = VolumeTimeBucketUtc.timeOfDay(startMs)
    val endDayBucket = VolumeTimeBucketUtc.dayBucket(endMs)
    val endTimeOfDay = VolumeTimeBucketUtc.timeOfDay(endMs)

    val step1StartTime = System.currentTimeMillis()

    val vxTableName = HBaseTableManager.geoSimVoxelVxUnifiedIdxTableName(dataset, Some(lvl))
    val vyTableName = HBaseTableManager.geoSimVoxelVyUnifiedIdxTableName(dataset, Some(lvl))
    val vzTableName = HBaseTableManager.geoSimVoxelVzUnifiedIdxTableName(dataset, Some(lvl))

    val (sx, scanVxMs, unifiedRowsVx) = scanOneComponent(
      vxTableName, vxBucketRawMin, vxBucketRawMax, zCells,
      startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay,
      GeoSimVoxelVelocityUnifiedIndexKey.vxScanRangeTimeSpan
    )

    val (sy, scanVyMs, unifiedRowsVy) = scanOneComponent(
      vyTableName, vyBucketRawMin, vyBucketRawMax, zCells,
      startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay,
      GeoSimVoxelVelocityUnifiedIndexKey.vyScanRangeTimeSpan
    )

    val (sz, scanVzMs, unifiedRowsVz) = scanOneComponent(
      vzTableName, vzBucketRawMin, vzBucketRawMax, zCells,
      startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay,
      GeoSimVoxelVelocityUnifiedIndexKey.vzScanRangeTimeSpan
    )

    val brickRowKeys = sx.intersect(sy).intersect(sz)

    val scanTotalMs = scanVxMs + scanVyMs + scanVzMs
    val totalUnifiedRows = unifiedRowsVx + unifiedRowsVy + unifiedRowsVz
    val totalDkCount = sx.size + sy.size + sz.size
    val tasksCountVx = vxBucketsCount * zCellsCount
    val tasksCountVy = vyBucketsCount * zCellsCount
    val tasksCountVz = vzBucketsCount * zCellsCount

    if (brickRowKeys.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      val mode = if (useUnifiedParallel) "parallel" else "serial"
      println("\n" + "=" * 80)
      println("[GeoSim-Voxel-Velocity-Unified-Query] Execution plan:")
      println("  Initial filter: VxBucket + VyBucket + VzBucket + Time + Space (velocity unified index scan)")
      println("  Secondary filter: Time range + Spatial bbox intersection + Velocity range (lookup table for precise filtering)")
      println("=" * 80)
      println(s"\n[Execution Parameters]")
      println(s"  → mode: $mode")
      if (useUnifiedParallel) {
        println(s"  → unifiedThreadPoolSize: $unifiedThreadPoolSize")
      }
      println(s"  → dvx: $dvx")
      println(s"  → dvy: $dvy")
      println(s"  → dvz: $dvz")
      println(s"  → vxBucket count: ${vxBucketsCount}")
      println(s"  → vyBucket count: ${vyBucketsCount}")
      println(s"  → vzBucket count: ${vzBucketsCount}")
      println(s"  → Spatial grid count: ${zCellsCount}")
      println(s"  → tasksCountVx: ${tasksCountVx}")
      println(s"  → tasksCountVy: ${tasksCountVy}")
      println(s"  → tasksCountVz: ${tasksCountVz}")
      println(s"\n[Initial Filter] Vx + Vy + Vz velocity unified index scan")
      println(s"  → scanVxMs: ${scanVxMs}ms")
      println(s"  → scanVyMs: ${scanVyMs}ms")
      println(s"  → scanVzMs: ${scanVzMs}ms")
      println(s"  → scanTotalMs: ${scanTotalMs}ms")
      println("=" * 80)
      println(s"[GeoSim-Voxel-Velocity-Unified-Query-Summary]")
      println(s"  → Initial filter time: ${scanTotalMs}ms")
      println(s"  → Secondary filter time: 0ms")
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      return (Seq.empty, headerLine)
    }

    val step2StartTime = System.currentTimeMillis()
    val acceptedRawLines = fetchAndRefineBricks(brickRowKeys, totalDkCount, vxMin, vxMax, vyMin, vyMax, vzMin, vzMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
    val step2Time = System.currentTimeMillis() - step2StartTime
    val resultCount = acceptedRawLines.size

    val totalTime = System.currentTimeMillis() - globalStartTime
    val mode = if (useUnifiedParallel) "parallel" else "serial"
    println("\n" + "=" * 80)
    println("[GeoSim-Voxel-Velocity-Unified-Query] Execution plan:")
    println("  Initial filter: VxBucket + VyBucket + VzBucket + Time + Space (velocity unified index scan)")
    println("  Secondary filter: Time range + Spatial bbox intersection + Velocity range (lookup table for precise filtering)")
    println("=" * 80)
    println(s"\n[Execution Parameters]")
    println(s"  → mode: $mode")
    if (useUnifiedParallel) {
      println(s"  → unifiedThreadPoolSize: $unifiedThreadPoolSize")
    }
    println(s"  → dvx: $dvx")
    println(s"  → dvy: $dvy")
    println(s"  → dvz: $dvz")
    println(s"  → vxBucket count: ${vxBucketsCount}")
    println(s"  → vyBucket count: ${vyBucketsCount}")
    println(s"  → vzBucket count: ${vzBucketsCount}")
    println(s"  → Spatial grid count: ${zCellsCount}")
    println(s"  → tasksCountVx: ${tasksCountVx}")
    println(s"  → tasksCountVy: ${tasksCountVy}")
    println(s"  → tasksCountVz: ${tasksCountVz}")
    println(s"\n[Initial Filter] Vx + Vy + Vz velocity unified index scan")
    println(s"  → scanVxMs: ${scanVxMs}ms")
    println(s"  → scanVyMs: ${scanVyMs}ms")
    println(s"  → scanVzMs: ${scanVzMs}ms")
    println(s"  → scanTotalMs: ${scanTotalMs}ms")
    println(s"\n[Secondary Filter] Time range + Spatial bbox intersection + Velocity range (lookup table for precise filtering)")
    println(s"  → Result: ${resultCount} records | Time: ${step2Time}ms")
    println("=" * 80)
    println(s"[GeoSim-Voxel-Velocity-Unified-Query-Summary]")
    println(s"  → Initial filter time: ${scanTotalMs}ms")
    println(s"  → Secondary filter time: ${step2Time}ms (includes data read and coordinate/velocity validation)")
    println(s"  → Total query time: ${totalTime}ms")
    println("=" * 80 + "\n")

    (acceptedRawLines, headerLine)
  }

  private def fetchAndRefineBricks(brickRowKeys: Set[String],
                                   totalDkCount: Int,
                                   vxMin: Double, vxMax: Double,
                                   vyMin: Double, vyMax: Double,
                                   vzMin: Double, vzMax: Double,
                                   startMs: Long, endMs: Long,
                                   xMin: Double, yMin: Double, zMin: Double,
                                   xMax: Double, yMax: Double, zMax: Double): Seq[String] = {
    val brickTable = connection.getTable(TableName.valueOf(HBaseTableManager.volumeBrickTableName(dataset)))
    val cf = HBaseTableManager.CF_BYTES
    val results = mutable.ArrayBuffer[String]()
    var totalBrickSuccess = 0
    var timeFilteredOut = 0
    var spatialFilteredOut = 0
    var vxFilteredOut = 0
    var vyFilteredOut = 0
    var vzFilteredOut = 0

    try {
      val gets = brickRowKeys.map { key =>
        new Get(Bytes.toBytes(key))
          .addColumn(cf, Bytes.toBytes("time_millis"))
          .addColumn(cf, Bytes.toBytes("x_min"))
          .addColumn(cf, Bytes.toBytes("x_max"))
          .addColumn(cf, Bytes.toBytes("y_min"))
          .addColumn(cf, Bytes.toBytes("y_max"))
          .addColumn(cf, Bytes.toBytes("z_min"))
          .addColumn(cf, Bytes.toBytes("z_max"))
          .addColumn(cf, Bytes.toBytes("vx"))
          .addColumn(cf, Bytes.toBytes("vy"))
          .addColumn(cf, Bytes.toBytes("vz"))
          .addColumn(cf, Bytes.toBytes("raw_line"))
      }.toList

      val resultList = brickTable.get(gets.asJava)

      resultList.foreach { result =>
        if (result != null && !result.isEmpty) {
          val time = Bytes.toLong(result.getValue(cf, Bytes.toBytes("time_millis")))
          val xMinBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("x_min")))
          val xMaxBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("x_max")))
          val yMinBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("y_min")))
          val yMaxBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("y_max")))
          val zMinBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("z_min")))
          val zMaxBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("z_max")))
          val vx = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("vx")))
          val vy = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("vy")))
          val vz = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("vz")))
          val rawLine = Bytes.toString(result.getValue(cf, Bytes.toBytes("raw_line")))

          var accepted = true

          if (time < startMs || time > endMs) {
            timeFilteredOut += 1
            accepted = false
          }

          if (accepted && (xMaxBrick < xMin || xMinBrick > xMax ||
                           yMaxBrick < yMin || yMinBrick > yMax ||
                           zMaxBrick < zMin || zMinBrick > zMax)) {
            spatialFilteredOut += 1
            accepted = false
          }

          if (accepted && (vx < vxMin || vx > vxMax)) {
            vxFilteredOut += 1
            accepted = false
          }

          if (accepted && (vy < vyMin || vy > vyMax)) {
            vyFilteredOut += 1
            accepted = false
          }

          if (accepted && (vz < vzMin || vz > vzMax)) {
            vzFilteredOut += 1
            accepted = false
          }

          if (accepted) {
            results += rawLine
            totalBrickSuccess += 1
          }
        }
      }
    } finally {
      brickTable.close()
    }

    println(s"[Secondary Filter] Scan statistics:")
    println(s"  → totalDkCount: $totalDkCount")
    println(s"  → totalBrickSuccess: $totalBrickSuccess")
    println(s"  → timeFilteredOut: $timeFilteredOut")
    println(s"  → spatialFilteredOut: $spatialFilteredOut")
    println(s"  → vxFilteredOut: $vxFilteredOut")
    println(s"  → vyFilteredOut: $vyFilteredOut")
    println(s"  → vzFilteredOut: $vzFilteredOut")

    results.toSeq
  }

  def close(): Unit = {
    connection.close()
  }

  def getHeaderLine: String = metaHeaderLine
}
