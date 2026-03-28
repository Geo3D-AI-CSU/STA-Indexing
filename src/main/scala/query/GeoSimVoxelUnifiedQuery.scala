// src/main/scala/query/GeoSimVoxelUnifiedQuery.scala
package query

import index.{GeoSimVoxelGrid, VolumeTimeBucketUtc, GeoSimVolumeUnifiedScanKey, GridMeta}
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.concurrent.Executors

case class ScanStats(
  var totalUnifiedRows: Int = 0,
  var totalDkCount: Int = 0,
  var totalBrickSuccess: Int = 0,
  var timeFilteredOut: Int = 0,
  var spatialFilteredOut: Int = 0,
  var spatialRefinedPass: Int = 0
)

class GeoSimVoxelUnifiedQuery(
  zkQuorum: String,
  useUnifiedParallel: Boolean = false,
  unifiedThreadPoolSize: Int = 8,
  dataset: Option[String] = None,
  unifiedLevel: Option[Int] = None
) {

  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", zkQuorum)
  private val connection = ConnectionFactory.createConnection(conf)

  private val level = unifiedLevel match {
    case Some(lvl) if lvl == 4 || lvl == 5 || lvl == 6 || lvl == 7 || lvl == 8 => lvl
    case Some(lvl) => throw new IllegalArgumentException(s"GeoSimVoxel only supports unifiedLevel 4/5/6/7/8, got: $lvl")
    case None => 4
  }

  private var metaHeaderLine: String = "raw_line"

  def getHeaderLine: String = metaHeaderLine

  def queryRawLines(simId: String, startMs: Long, endMs: Long,
                    xMin: Double, yMin: Double, zMin: Double,
                    xMax: Double, yMax: Double, zMax: Double): Seq[String] = {
    
    val globalStartTime = System.currentTimeMillis()
    
    val unifiedTableName = HBaseTableManager.volumeUnifiedIdxTableName(dataset, Some(level))
    val brickTableName = HBaseTableManager.volumeBrickTableName(dataset)
    val metaTableName = HBaseTableManager.volumeMetaTableName(dataset)
    
    val (bxSize, bySize, bzSize) = GeoSimVoxelGrid.blockSizeForLevel(level)
    
    println("\n" + "=" * 80)
    println("Executing unified index query")
    println("=" * 80)
    println(s"  sim_id: $simId")
    println(s"  time_range: $startMs ~ $endMs")
    println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
    println(s"  unified table: $unifiedTableName")
    println(s"  brick table: $brickTableName")
    println(s"  meta table: $metaTableName")
    println(s"  unifiedLevel: $level")
    println(s"  blockSize($level): ${bxSize}x${bySize}x${bzSize}")
    println(s"  mode: ${if (useUnifiedParallel) "parallel" else "serial"}")
    if (useUnifiedParallel) {
      println(s"  unifiedThreadPoolSize: $unifiedThreadPoolSize")
    }
    
    val (meta, headerLine) = GeoSimVoxelGrid.readMetaWithHeader(connection, dataset)
    metaHeaderLine = headerLine
    
    val zCells = GeoSimVoxelGrid.enumerateZCellsForBBox(meta, xMin, yMin, zMin, xMax, yMax, zMax, level)
    val zCellsCount = zCells.size
    
    val dayBuckets = VolumeTimeBucketUtc.enumerateDays(startMs, endMs)
    val dayBucketsCount = dayBuckets.size
    
    val startDayBucket = VolumeTimeBucketUtc.dayBucket(startMs)
    val startTimeOfDay = VolumeTimeBucketUtc.timeOfDay(startMs)
    val endDayBucket = VolumeTimeBucketUtc.dayBucket(endMs)
    val endTimeOfDay = VolumeTimeBucketUtc.timeOfDay(endMs)
    
    val scanStartTime = System.currentTimeMillis()
    val (brickRowKeys, scanStats) = if (useUnifiedParallel) {
      executeUnifiedParallel(simId, zCells, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
    } else {
      executeUnifiedSerial(simId, zCells, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
    }
    val scanDurationMs = System.currentTimeMillis() - scanStartTime
    
    val scanTaskCount = zCellsCount
    
    if (brickRowKeys.isEmpty) {
      println("\n" + "-" * 80)
      println("Query statistics:")
      println(s"  Enumerated zCells count: ${zCellsCount}")
      println(s"  Enumerated dayBuckets count: ${dayBucketsCount}")
      println(s"  day_bucket_range (yyyyMMdd): ${dayBuckets.head} ~ ${dayBuckets.last}")
      println(s"  Total scan tasks: ${scanTaskCount}")
      println(s"  Total scan time: ${scanDurationMs} ms (${scanDurationMs / 1000.0} s)")
      println(s"  Actually scanned unified rows: ${scanStats.totalUnifiedRows}")
      println(s"  Read dk count: ${scanStats.totalDkCount}")
      println(s"  Successful brick lookups: 0")
      println(s"  Time secondary filter rejected: 0")
      println(s"  Spatial secondary filter rejected: 0")
      println(s"  Final result count: 0")
      println("-" * 80)
      println(s"\n[Unified-Query-Summary]")
      println(s"  → Initial filter time: ${scanDurationMs}ms")
      println(s"  → Secondary filter time: 0ms (includes brick lookup)")
      val totalTime = System.currentTimeMillis() - globalStartTime
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      return Seq.empty
    }
    
    val step2StartTime = System.currentTimeMillis()
    val acceptedRawLines = fetchRawLines(brickRowKeys, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax, scanStats)
    val step2Time = System.currentTimeMillis() - step2StartTime
    val resultCount = acceptedRawLines.size
    
    println("\n" + "-" * 80)
    println("Query statistics:")
    println(s"  Enumerated zCells count: ${zCellsCount}")
    println(s"  Enumerated dayBuckets count: ${dayBucketsCount}")
    println(s"  day_bucket_range (yyyyMMdd): ${dayBuckets.head} ~ ${dayBuckets.last}")
    println(s"  Total scan tasks: ${scanTaskCount}")
    println(s"  Total scan time: ${scanDurationMs} ms (${scanDurationMs / 1000.0} s)")
    println(s"  Actually scanned unified rows: ${scanStats.totalUnifiedRows}")
    println(s"  Read dk count: ${scanStats.totalDkCount}")
    println(s"  Successful brick lookups: ${scanStats.totalBrickSuccess}")
    println(s"  Time secondary filter rejected: ${scanStats.timeFilteredOut}")
    println(s"  Spatial secondary filter rejected: ${scanStats.spatialFilteredOut}")
    println(s"  Final result count: ${scanStats.spatialRefinedPass}")
    println("-" * 80)
    println(s"\n[Unified-Query-Summary]")
    println(s"  → Initial filter time: ${scanDurationMs}ms")
    println(s"  → Secondary filter time: ${step2Time}ms (includes brick lookup)")
    val totalTime = System.currentTimeMillis() - globalStartTime
    println(s"  → Total query time: ${totalTime}ms")
    println("=" * 80 + "\n")
    
    acceptedRawLines
  }
  
  private def executeUnifiedSerial(simId: String, zCells: Seq[Long],
                               startDayBucket: Int, startTimeOfDay: Int,
                               endDayBucket: Int, endTimeOfDay: Int): (Set[String], ScanStats) = {
    val rowKeys = mutable.Set[String]()
    val stats = ScanStats()
    val unifiedTable = connection.getTable(TableName.valueOf(HBaseTableManager.volumeUnifiedIdxTableName(dataset, Some(level))))
    val cf = HBaseTableManager.CF_BYTES
    
    try {
      zCells.foreach { zCell =>
        val (startRowBytes, stopRowBytes) = GeoSimVolumeUnifiedScanKey.buildScanRangeForCellTimeSpan(
          simId, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
        )
        
        val scan = new Scan()
        scan.withStartRow(startRowBytes)
        scan.withStopRow(stopRowBytes)
        
        val scanner = unifiedTable.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            stats.totalUnifiedRows += 1
            val dkBytes = result.getValue(cf, Bytes.toBytes("dk"))
            if (dkBytes != null) {
              val brickRowKey = Bytes.toString(dkBytes)
              rowKeys += brickRowKey
              stats.totalDkCount += 1
            } else {
              val rowKeyBytes = result.getRow
              val separatorIndex = rowKeyBytes.indexOf('_'.toByte)
              if (separatorIndex > 0 && rowKeyBytes.length > separatorIndex + 1) {
                val brickRowKey = new String(rowKeyBytes, separatorIndex + 1, rowKeyBytes.length - separatorIndex - 1, "UTF-8")
                rowKeys += brickRowKey
                stats.totalDkCount += 1
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
    
    (rowKeys.toSet, stats)
  }
  
  private def executeUnifiedParallel(simId: String, zCells: Seq[Long],
                                 startDayBucket: Int, startTimeOfDay: Int,
                                 endDayBucket: Int, endTimeOfDay: Int): (Set[String], ScanStats) = {
    val threadPool = Executors.newFixedThreadPool(unifiedThreadPoolSize)
    val localRowKeysList = scala.collection.mutable.ListBuffer[mutable.Set[String]]()
    val localStatsList = scala.collection.mutable.ListBuffer[ScanStats]()
    val lockObj = new Object()
    
    try {
      val tasksPerThread = (zCells.length + unifiedThreadPoolSize - 1) / unifiedThreadPoolSize
      val futures = (0 until unifiedThreadPoolSize).map { threadIdx =>
        val startIdx = threadIdx * tasksPerThread
        val endIdx = math.min(startIdx + tasksPerThread, zCells.length)
        
        threadPool.submit(new Runnable {
          override def run(): Unit = {
            if (startIdx < zCells.length) {
              val assignedCells = zCells.slice(startIdx, endIdx)
              val localRowKeys = mutable.Set[String]()
              val localStats = ScanStats()
              
              val table = connection.getTable(TableName.valueOf(HBaseTableManager.volumeUnifiedIdxTableName(dataset, Some(level))))
              val cf = HBaseTableManager.CF_BYTES
              try {
                assignedCells.foreach { zCell =>
                  val (startRowBytes, stopRowBytes) = GeoSimVolumeUnifiedScanKey.buildScanRangeForCellTimeSpan(
                    simId, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
                  )
                  
                  val scan = new Scan()
                  scan.withStartRow(startRowBytes)
                  scan.withStopRow(stopRowBytes)
                  
                  val scanner = table.getScanner(scan)
                  try {
                    scanner.asScala.foreach { result =>
                      localStats.totalUnifiedRows += 1
                      val dkBytes = result.getValue(cf, Bytes.toBytes("dk"))
                      if (dkBytes != null) {
                        val brickRowKey = Bytes.toString(dkBytes)
                        localRowKeys += brickRowKey
                        localStats.totalDkCount += 1
                      } else {
                        val rowKeyBytes = result.getRow
                        val separatorIndex = rowKeyBytes.indexOf('_'.toByte)
                        if (separatorIndex > 0 && rowKeyBytes.length > separatorIndex + 1) {
                          val brickRowKey = new String(rowKeyBytes, separatorIndex + 1, rowKeyBytes.length - separatorIndex - 1, "UTF-8")
                          localRowKeys += brickRowKey
                          localStats.totalDkCount += 1
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
                localStatsList += localStats
              }
            }
          }
        })
      }
      
      futures.foreach(_.get())
      
    } finally {
      threadPool.shutdown()
    }
    
    val mergedStats = ScanStats()
    localStatsList.foreach { s =>
      mergedStats.totalUnifiedRows += s.totalUnifiedRows
      mergedStats.totalDkCount += s.totalDkCount
    }
    
    (localRowKeysList.flatten.toSet, mergedStats)
  }
  
  private def fetchRawLines(rowKeys: Set[String], startMs: Long, endMs: Long,
                               xMin: Double, yMin: Double, zMin: Double,
                               xMax: Double, yMax: Double, zMax: Double,
                               scanStats: ScanStats): Seq[String] = {
    val brickTable = connection.getTable(TableName.valueOf(HBaseTableManager.volumeBrickTableName(dataset)))
    val cf = HBaseTableManager.CF_BYTES
    val results = mutable.ArrayBuffer[String]()
    
    try {
      val gets = rowKeys.map { key =>
        new Get(Bytes.toBytes(key))
          .addColumn(cf, Bytes.toBytes("time_millis"))
          .addColumn(cf, Bytes.toBytes("raw_line"))
          .addColumn(cf, Bytes.toBytes("x_min"))
          .addColumn(cf, Bytes.toBytes("x_max"))
          .addColumn(cf, Bytes.toBytes("y_min"))
          .addColumn(cf, Bytes.toBytes("y_max"))
          .addColumn(cf, Bytes.toBytes("z_min"))
          .addColumn(cf, Bytes.toBytes("z_max"))
      }.toList
      
      val resultList = brickTable.get(gets.asJava)
      
      resultList.foreach { result =>
        if (result != null && !result.isEmpty) {
          scanStats.totalBrickSuccess += 1
          val timeMillis = Bytes.toLong(result.getValue(cf, Bytes.toBytes("time_millis")))
          val rawLine = Bytes.toString(result.getValue(cf, Bytes.toBytes("raw_line")))
          val bxMin = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("x_min")))
          val bxMax = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("x_max")))
          val byMin = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("y_min")))
          val byMax = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("y_max")))
          val bzMin = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("z_min")))
          val bzMax = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("z_max")))
          
          val timePass = timeMillis >= startMs && timeMillis <= endMs
          if (!timePass) {
            scanStats.timeFilteredOut += 1
          } else {
            val spatialPass = intersects(bxMin, bxMax, byMin, byMax, bzMin, bzMax, xMin, xMax, yMin, yMax, zMin, zMax)
            if (spatialPass) {
              scanStats.spatialRefinedPass += 1
              results += rawLine
            } else {
              scanStats.spatialFilteredOut += 1
            }
          }
        }
      }
    } finally {
      brickTable.close()
    }
    
    results.toSeq
  }
  
  private def intersects(
    bxMin: Double, bxMax: Double,
    byMin: Double, byMax: Double,
    bzMin: Double, bzMax: Double,
    qxMin: Double, qxMax: Double,
    qyMin: Double, qyMax: Double,
    qzMin: Double, qzMax: Double
  ): Boolean = {
    (bxMax >= qxMin && bxMin <= qxMax) &&
    (byMax >= qyMin && byMin <= qyMax) &&
    (bzMax >= qzMin && bzMin <= qzMax)
  }
  
  def close(): Unit = {
    connection.close()
  }
}
