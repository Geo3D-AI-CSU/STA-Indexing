package query

import index.{GeoSimVoxelGrid, GeoSimTempBucket, GeoSimVoxelTempUnifiedIndexKey, VolumeTimeBucketUtc}
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.concurrent.Executors

class GeoSimVoxelTempUnifiedQuery(
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

  def queryRawLinesByTemp(
    tMin: Double, tMax: Double,
    startMs: Long, endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double
  ): Seq[String] = {

    val globalStartTime = System.currentTimeMillis()

    val (gridMeta, headerLine, tempParams) = GeoSimVoxelGrid.readMetaWithHeaderAndTemp(connection, dataset)
    metaHeaderLine = headerLine

    val lvl = unifiedLevel.getOrElse(4)

    val (tempBucketMin, tempBucketMax) = GeoSimTempBucket.bucketRange(tMin, tMax, tempParams)
    val tempBuckets = (tempBucketMin to tempBucketMax).toSeq
    val tempBucketsCount = tempBuckets.size

    val zCells = GeoSimVoxelGrid.enumerateZCellsForBBox(
      gridMeta, xMin, yMin, zMin, xMax, yMax, zMax, lvl, maxCells = 1000000
    )
    val zCellsCount = zCells.size

    val tasksCount = tempBucketsCount * zCellsCount

    val startDayBucket = VolumeTimeBucketUtc.dayBucket(startMs)
    val startTimeOfDay = VolumeTimeBucketUtc.timeOfDay(startMs)
    val endDayBucket = VolumeTimeBucketUtc.dayBucket(endMs)
    val endTimeOfDay = VolumeTimeBucketUtc.timeOfDay(endMs)

    val step1StartTime = System.currentTimeMillis()
    val brickRowKeys = if (useUnifiedParallel) {
      executeUnifiedParallel(tempBuckets, zCells, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
    } else {
      executeUnifiedSerial(tempBuckets, zCells, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
    }
    val step1Time = System.currentTimeMillis() - step1StartTime
    val totalUnifiedRows = brickRowKeys.size

    if (brickRowKeys.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      println("\n" + "=" * 80)
      println("[GeoSim-Voxel-Temp-Unified-Query] Execution plan:")
      println("  Initial filter: TempBucket + Time + Space (temperature unified index scan)")
      println("  Secondary filter: Time range + Spatial bbox intersection + Temperature range (lookup table for precise filtering)")
      println("=" * 80)
      println(s"\n[Execution Parameters]")
      println(s"  → Temperature bucket width: ${tempParams.widthK}K")
      println(s"  → Temperature bucket count: ${tempBucketsCount}")
      println(s"  → Spatial grid count: ${zCellsCount}")
      println(s"  → Block task count: ${tasksCount}")
      println(s"\n[Initial Filter] TempBucket + Time + Space temperature unified index scan")
      println(s"  → Result: ${totalUnifiedRows} records | Time: ${step1Time}ms")
      println("=" * 80)
      println(s"[GeoSim-Voxel-Temp-Unified-Query-Summary]")
      println(s"  → Initial filter time: ${step1Time}ms")
      println(s"  → Secondary filter time: 0ms")
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      return Seq.empty
    }

    val step2StartTime = System.currentTimeMillis()
    val acceptedRawLines = fetchAndRefineBricks(brickRowKeys, tMin, tMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
    val step2Time = System.currentTimeMillis() - step2StartTime
    val resultCount = acceptedRawLines.size

    val totalTime = System.currentTimeMillis() - globalStartTime
    println("\n" + "=" * 80)
    println("[GeoSim-Voxel-Temp-Unified-Query] Execution plan:")
    println("  Initial filter: TempBucket + Time + Space (temperature unified index scan)")
    println("  Secondary filter: Time range + Spatial bbox intersection + Temperature range (lookup table for precise filtering)")
    println("=" * 80)
    println(s"\n[Execution Parameters]")
    println(s"  → Temperature bucket width: ${tempParams.widthK}K")
    println(s"  → Temperature bucket count: ${tempBucketsCount}")
    println(s"  → Spatial grid count: ${zCellsCount}")
    println(s"  → Block task count: ${tasksCount}")
    println(s"\n[Initial Filter] TempBucket + Time + Space temperature unified index scan")
    println(s"  → Result: ${totalUnifiedRows} records | Time: ${step1Time}ms")
    println(s"\n[Secondary Filter] Time range + Spatial bbox intersection + Temperature range (lookup table for precise filtering)")
    println(s"  → Result: ${resultCount} records | Time: ${step2Time}ms")
    println("=" * 80)
    println(s"[GeoSim-Voxel-Temp-Unified-Query-Summary]")
    println(s"  → Initial filter time: ${step1Time}ms")
    println(s"  → Secondary filter time: ${step2Time}ms (includes data read and coordinate/temperature validation)")
    println(s"  → Total query time: ${totalTime}ms")
    println("=" * 80 + "\n")

    acceptedRawLines
  }

  private def executeUnifiedSerial(tempBuckets: Seq[Int], zCells: Seq[Long],
                                   startDayBucket: Int, startTimeOfDay: Int,
                                   endDayBucket: Int, endTimeOfDay: Int): Set[String] = {
    val rowKeys = mutable.Set[String]()
    val unifiedTable = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelTempUnifiedIdxTableName(dataset, Some(unifiedLevel.getOrElse(4)))))

    try {
      for (tempBucket <- tempBuckets; zCell <- zCells) {
        val (startRowBytes, stopRowBytes) = GeoSimVoxelTempUnifiedIndexKey.buildScanRangeForTempCellTimeSpan(
          tempBucket, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
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
            } else {
              val rowKeyBytes = result.getRow
              if (rowKeyBytes.length > 22) {
                val brickRowKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                rowKeys += brickRowKey
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

  private def executeUnifiedParallel(tempBuckets: Seq[Int], zCells: Seq[Long],
                                     startDayBucket: Int, startTimeOfDay: Int,
                                     endDayBucket: Int, endTimeOfDay: Int): Set[String] = {
    val threadPool = Executors.newFixedThreadPool(unifiedThreadPoolSize)
    val localRowKeysList = scala.collection.mutable.ListBuffer[mutable.Set[String]]()
    val lockObj = new Object()

    try {
      val allTasks = for (tempBucket <- tempBuckets; zCell <- zCells) yield (tempBucket, zCell)
      val tasksPerThread = (allTasks.length + unifiedThreadPoolSize - 1) / unifiedThreadPoolSize

      val futures = (0 until unifiedThreadPoolSize).map { threadIdx =>
        val startIdx = threadIdx * tasksPerThread
        val endIdx = math.min(startIdx + tasksPerThread, allTasks.length)

        threadPool.submit(new Runnable {
          override def run(): Unit = {
            if (startIdx < allTasks.length) {
              val assignedTasks = allTasks.slice(startIdx, endIdx)
              val localRowKeys = mutable.Set[String]()

              val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelTempUnifiedIdxTableName(dataset, Some(unifiedLevel.getOrElse(4)))))
              try {
                assignedTasks.foreach { case (tempBucket, zCell) =>
                  val (startRowBytes, stopRowBytes) = GeoSimVoxelTempUnifiedIndexKey.buildScanRangeForTempCellTimeSpan(
                    tempBucket, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
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
                      } else {
                        val rowKeyBytes = result.getRow
                        if (rowKeyBytes.length > 22) {
                          val brickRowKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                          localRowKeys += brickRowKey
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

  private def fetchAndRefineBricks(brickRowKeys: Set[String], tMin: Double, tMax: Double,
                                   startMs: Long, endMs: Long,
                                   xMin: Double, yMin: Double, zMin: Double,
                                   xMax: Double, yMax: Double, zMax: Double): Seq[String] = {
    val brickTable = connection.getTable(TableName.valueOf(HBaseTableManager.volumeBrickTableName(dataset)))
    val cf = HBaseTableManager.CF_BYTES
    val results = mutable.ArrayBuffer[String]()
    var totalDkCount = 0
    var totalBrickSuccess = 0
    var timeFilteredOut = 0
    var spatialFilteredOut = 0
    var tempFilteredOut = 0

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
          .addColumn(cf, Bytes.toBytes("T"))
          .addColumn(cf, Bytes.toBytes("raw_line"))
      }.toList

      val resultList = brickTable.get(gets.asJava)

      resultList.foreach { result =>
        if (result != null && !result.isEmpty) {
          totalDkCount += 1
          val time = Bytes.toLong(result.getValue(cf, Bytes.toBytes("time_millis")))
          val xMinBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("x_min")))
          val xMaxBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("x_max")))
          val yMinBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("y_min")))
          val yMaxBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("y_max")))
          val zMinBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("z_min")))
          val zMaxBrick = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("z_max")))
          val t = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("T")))
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

          if (accepted && (t < tMin || t > tMax)) {
            tempFilteredOut += 1
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
    println(s"  → tempFilteredOut: $tempFilteredOut")

    results.toSeq
  }

  def close(): Unit = {
    connection.close()
  }

  def getHeaderLine: String = metaHeaderLine
}
