// src/main/scala/query/GeoSimPointUnifiedQuery.scala
package query

import index.{Z3DEncoder, TimeBucket, UnifiedIndexKey, GeoSimCoordMapper}
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.concurrent.Executors

class GeoSimPointUnifiedQuery(
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

  def queryRawLines(simId: String, startMs: Long, endMs: Long,
                    xMin: Double, yMin: Double, zMin: Double,
                    xMax: Double, yMax: Double, zMax: Double): Seq[String] = {
    
    val globalStartTime = System.currentTimeMillis()
    
    val (bounds, delimiterName, headerLine) = GeoSimCoordMapper.readMetaFull(connection, dataset)
    metaDelimiterName = delimiterName
    metaHeaderLine = headerLine
    
    val (mappedLonMin, mappedLatMin, mappedAltMin, mappedLonMax, mappedLatMax, mappedAltMax) =
      GeoSimCoordMapper.mapBBox(bounds, xMin, yMin, zMin, xMax, yMax, zMax)
    
    val lvl = unifiedLevel.getOrElse(Z3DEncoder.UNIFIED_BLOCK_LEVEL)
    
    val zCells = Z3DEncoder.cellsAtLevel(
      mappedLonMin, mappedLatMin, mappedAltMin,
      mappedLonMax, mappedLatMax, mappedAltMax,
      level = lvl,
      maxCells = 1000000
    )
    
    val tasksCount = zCells.size
    
    val startDayBucket = TimeBucket.dayBucket(startMs)
    val startTimeOfDay = TimeBucket.millisOfDay(startMs)
    val endDayBucket = TimeBucket.dayBucket(endMs)
    val endTimeOfDay = TimeBucket.millisOfDay(endMs)
    
    val step1StartTime = System.currentTimeMillis()
    val dataRowKeys = if (useUnifiedParallel) {
      executeUnifiedParallel(simId, zCells, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
    } else {
      executeUnifiedSerial(simId, zCells, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
    }
    val step1Time = System.currentTimeMillis() - step1StartTime
    
    val unifiedHitCount = dataRowKeys.size
    
    if (dataRowKeys.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      println("\n" + "=" * 80)
      println("[Unified-Query] Execution plan:")
      println("  Initial filter: SimId + Time + Space (unified index scan)")
      println("  Secondary filter: Spatial precise check (coordinate range validation)")
      println("=" * 80)
      println(s"\n[Execution Parameters]")
      println(s"  → Block task count: ${tasksCount}")
      println(s"\n[Initial Filter] SimId + Time + Space unified index scan")
      println(s"  → Result: ${unifiedHitCount} records | Time: ${step1Time}ms")
      println("=" * 80)
      println(s"[Unified-Query-Summary]")
      println(s"  → Initial filter time: ${step1Time}ms")
      println(s"  → Secondary filter time: 0ms")
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      return Seq.empty
    }
    
    val step2StartTime = System.currentTimeMillis()
    val acceptedRawLines = fetchRawLines(dataRowKeys, simId, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
    val step2Time = System.currentTimeMillis() - step2StartTime
    val acceptedCount = acceptedRawLines.size
    
    val totalTime = System.currentTimeMillis() - globalStartTime
    println("\n" + "=" * 80)
    println("[Unified-Query] Execution plan:")
    println("  Initial filter: SimId + Time + Space (unified index scan)")
    println("  Secondary filter: Spatial precise check (coordinate range validation)")
    println("=" * 80)
    println(s"\n[Execution Parameters]")
    println(s"  → Block task count: ${tasksCount}")
    println(s"\n[Initial Filter] SimId + Time + Space unified index scan")
    println(s"  → Result: ${unifiedHitCount} records | Time: ${step1Time}ms")
    println(s"\n[Secondary Filter] Spatial precise check (coordinate range validation)")
    println(s"  → Result: ${acceptedCount} records | Time: ${step2Time}ms")
    println("=" * 80)
    println(s"[Unified-Query-Summary]")
    println(s"  → Initial filter time: ${step1Time}ms")
    println(s"  → Secondary filter time: ${step2Time}ms (includes data read and coordinate validation)")
    println(s"  → Total query time: ${totalTime}ms")
    println("=" * 80 + "\n")
    
    acceptedRawLines
  }
  
  private def executeUnifiedSerial(simId: String, zCells: Seq[Long],
                                   startDayBucket: Int, startTimeOfDay: Int,
                                   endDayBucket: Int, endTimeOfDay: Int): Set[String] = {
    val rowKeys = mutable.Set[String]()
    val unifiedTable = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointUnifiedIdxTableName(dataset, unifiedLevel)))
    
    try {
      zCells.foreach { zCell =>
        val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForCellTimeSpan(
          simId, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
        )
        
        val scan = new Scan()
        scan.withStartRow(startRowBytes)
        scan.withStopRow(stopRowBytes)
        
        val scanner = unifiedTable.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val rowKeyBytes = result.getRow
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
      unifiedTable.close()
    }
    
    rowKeys.toSet
  }
  
  private def executeUnifiedParallel(simId: String, zCells: Seq[Long],
                                     startDayBucket: Int, startTimeOfDay: Int,
                                     endDayBucket: Int, endTimeOfDay: Int): Set[String] = {
    val threadPool = Executors.newFixedThreadPool(unifiedThreadPoolSize)
    val localRowKeysList = scala.collection.mutable.ListBuffer[mutable.Set[String]]()
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
              
              val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointUnifiedIdxTableName(dataset, unifiedLevel)))
              try {
                assignedCells.foreach { zCell =>
                  val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForCellTimeSpan(
                    simId, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay
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
  
  private def fetchRawLines(rowKeys: Set[String], simId: String,
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
          .addColumn(cf, Bytes.toBytes("sensor_id"))
          .addColumn(cf, Bytes.toBytes("lon"))
          .addColumn(cf, Bytes.toBytes("lat"))
          .addColumn(cf, Bytes.toBytes("alt"))
          .addColumn(cf, Bytes.toBytes("raw_line"))
      }.toList
      
      val resultList = dataTable.get(gets.asJava)
      
      resultList.foreach { result =>
        if (result != null && !result.isEmpty) {
          val time = Bytes.toLong(result.getValue(cf, Bytes.toBytes("time")))
          val sensorId = Bytes.toString(result.getValue(cf, Bytes.toBytes("sensor_id")))
          val x = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("lon")))
          val y = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("lat")))
          val z = Bytes.toDouble(result.getValue(cf, Bytes.toBytes("alt")))
          val rawLine = Bytes.toString(result.getValue(cf, Bytes.toBytes("raw_line")))
          
          if (sensorId == simId &&
              time >= startMs && time <= endMs &&
              x >= xMin && x <= xMax &&
              y >= yMin && y <= yMax &&
              z >= zMin && z <= zMax) {
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
