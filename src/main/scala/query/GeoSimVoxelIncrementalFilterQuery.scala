// src/main/scala/query/GeoSimVoxelIncrementalFilterQuery.scala
package query

import index.{GeoSimVoxelGrid, VolumeTimeBucketUtc, GridMeta}
import model.GeoSimVoxelLine
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, CompareOperator}
import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter.{PrefixFilter, RowFilter, BinaryComparator}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable

class GeoSimVoxelIncrementalFilterQuery(
  zkQuorum: String,
  dataset: Option[String] = None,
  unifiedLevel: Option[Int] = None,
  threadPoolSize: Int = 8,
  getBatchSize: Int = 500
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
    
    val simIdxTableName = HBaseTableManager.geoSimVoxelSimIdxTableName(dataset)
    val spatialIdxTableName = HBaseTableManager.geoSimVoxelSpatialIdxTableName(dataset, Some(level))
    val timeIdxTableName = HBaseTableManager.geoSimVoxelTimeIdxTableName(dataset)
    val brickTableName = HBaseTableManager.volumeBrickTableName(dataset)
    val metaTableName = HBaseTableManager.volumeMetaTableName(dataset)
    
    val (bxSize, bySize, bzSize) = GeoSimVoxelGrid.blockSizeForLevel(level)
    
    println("\n" + "=" * 80)
    println("Executing GeoSim Voxel incremental filter query")
    println("=" * 80)
    println(s"  sim_id: $simId")
    println(s"  time_range: $startMs ~ $endMs")
    println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
    println(s"  sim_idx table: $simIdxTableName")
    println(s"  spatial_idx table: $spatialIdxTableName")
    println(s"  time_idx table: $timeIdxTableName")
    println(s"  brick table: $brickTableName")
    println(s"  meta table: $metaTableName")
    println(s"  unifiedLevel: $level")
    println(s"  blockSize($level): ${bxSize}x${bySize}x${bzSize}")
    
    val step0StartTime = System.currentTimeMillis()
    
    val (meta, headerLine) = GeoSimVoxelGrid.readMetaWithHeader(connection, dataset)
    metaHeaderLine = headerLine
    
    val normalizedSimId = GeoSimVoxelLine.normalizeSimId(simId)
    
    val step0Time = System.currentTimeMillis() - step0StartTime
    println(s"\n[Step 0] Read Meta completed, time: ${step0Time}ms")
    println(s"  normalized sim_id: $normalizedSimId")
    
    val step1StartTime = System.currentTimeMillis()
    
    val simIdxTable = connection.getTable(TableName.valueOf(simIdxTableName))
    val cf = HBaseTableManager.CF_BYTES
    val S1 = mutable.Set[String]()
    
    try {
      val prefix = s"${normalizedSimId}_"
      val scan = new Scan()
      scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)))
      
      val scanner = simIdxTable.getScanner(scan)
      try {
        scanner.asScala.foreach { result =>
          val dkBytes = result.getValue(cf, Bytes.toBytes("dk"))
          if (dkBytes != null) {
            val brickRowKey = Bytes.toString(dkBytes)
            S1 += brickRowKey
          }
        }
      } finally {
        scanner.close()
      }
    } finally {
      simIdxTable.close()
    }
    
    val step1Time = System.currentTimeMillis() - step1StartTime
    println(s"\n[Step 1] Attribute(sim_id) index query completed, time: ${step1Time}ms")
    println(s"  S1.size: ${S1.size}")
    
    if (S1.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      println(s"\n[Incremental-Filter-Query-Summary]")
      println(s"  → Step 0 (Read Meta) time: ${step0Time}ms")
      println(s"  → Step 1 (Attribute) time: ${step1Time}ms")
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      return Seq.empty
    }
    
    val step2StartTime = System.currentTimeMillis()
    
    val zCells = GeoSimVoxelGrid.enumerateZCellsForBBox(meta, xMin, yMin, zMin, xMax, yMax, zMax, level, maxCells = 1000000)
    val zCellsCount = zCells.size
    
    val spatialIdxTable = connection.getTable(TableName.valueOf(spatialIdxTableName))
    val S2 = mutable.Set[String]()
    
    try {
      zCells.foreach { zCell =>
        val zCellBytes = Bytes.toBytes(zCell)
        val scan = new Scan()
        scan.setRowPrefixFilter(zCellBytes)
        
        val scanner = spatialIdxTable.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val dkBytes = result.getValue(cf, Bytes.toBytes("dk"))
            if (dkBytes != null) {
              val brickRowKey = Bytes.toString(dkBytes)
              S2 += brickRowKey
            }
          }
        } finally {
          scanner.close()
        }
      }
    } finally {
      spatialIdxTable.close()
    }
    
    val S12 = S1.intersect(S2)
    
    val step2Time = System.currentTimeMillis() - step2StartTime
    println(s"\n[Step 2] Spatial(block) index query completed, time: ${step2Time}ms")
    println(s"  S2.size: ${S2.size}")
    println(s"  S12.size (S1 ∩ S2): ${S12.size}")
    println(s"  zCellsCount: ${zCellsCount}")
    
    if (S12.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      println(s"\n[Incremental-Filter-Query-Summary]")
      println(s"  → Step 0 (Read Meta) time: ${step0Time}ms")
      println(s"  → Step 1 (Attribute) time: ${step1Time}ms")
      println(s"  → Step 2 (Spatial) time: ${step2Time}ms")
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      return Seq.empty
    }
    
    val step3StartTime = System.currentTimeMillis()
    
    val dayBuckets = VolumeTimeBucketUtc.enumerateDays(startMs, endMs)
    val dayBucketsCount = dayBuckets.size
    
    val startDayBucket = VolumeTimeBucketUtc.dayBucket(startMs)
    val startTimeOfDay = VolumeTimeBucketUtc.timeOfDay(startMs)
    val endDayBucket = VolumeTimeBucketUtc.dayBucket(endMs)
    val endTimeOfDay = VolumeTimeBucketUtc.timeOfDay(endMs)
    
    val timeIdxTable = connection.getTable(TableName.valueOf(timeIdxTableName))
    val S3 = mutable.Set[String]()
    
    try {
      dayBuckets.foreach { dayBucket =>
        val (minTod, maxTod) = (dayBucket, startDayBucket, endDayBucket) match {
          case (d, s, e) if d == s && d == e => (startTimeOfDay, endTimeOfDay)
          case (d, s, _) if d == s => (startTimeOfDay, 86399999)
          case (d, _, e) if d == e => (0, endTimeOfDay)
          case _ => (0, 86399999)
        }
        
        val startRow = f"${dayBucket}%08d_${minTod}%08d_"
        val stopRow  = f"${dayBucket}%08d_${maxTod + 1}%08d_"
        
        val scan = new Scan()
        scan.withStartRow(Bytes.toBytes(startRow))
        scan.withStopRow(Bytes.toBytes(stopRow))
        
        val scanner = timeIdxTable.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val dkBytes = result.getValue(cf, Bytes.toBytes("dk"))
            if (dkBytes != null) {
              val brickRowKey = Bytes.toString(dkBytes)
              S3 += brickRowKey
            }
          }
        } finally {
          scanner.close()
        }
      }
    } finally {
      timeIdxTable.close()
    }
    
    val S123 = S12.intersect(S3)
    
    val step3Time = System.currentTimeMillis() - step3StartTime
    println(s"\n[Step 3] Time index query completed, time: ${step3Time}ms")
    println(s"  S3.size: ${S3.size}")
    println(s"  S123.size (S12 ∩ S3): ${S123.size}")
    println(s"  dayBucketsCount: ${dayBucketsCount}")
    println(s"  day_bucket_range (yyyyMMdd): ${dayBuckets.head} ~ ${dayBuckets.last}")
    
    if (S123.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      println(s"\n[Incremental-Filter-Query-Summary]")
      println(s"  → Step 0 (Read Meta) time: ${step0Time}ms")
      println(s"  → Step 1 (Attribute) time: ${step1Time}ms")
      println(s"  → Step 2 (Spatial) time: ${step2Time}ms")
      println(s"  → Step 3 (Time) time: ${step3Time}ms")
      println(s"  → Total query time: ${totalTime}ms")
      println("=" * 80 + "\n")
      return Seq.empty
    }
    
    val step4StartTime = System.currentTimeMillis()
    
    val brickTable = connection.getTable(TableName.valueOf(brickTableName))
    val results = mutable.ArrayBuffer[String]()
    var timeFilteredOut = 0
    var spatialFilteredOut = 0
    
    try {
      S123.grouped(getBatchSize).foreach { batch =>
        val gets = batch.map { key =>
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
              timeFilteredOut += 1
            } else {
              val spatialPass = intersects(bxMin, bxMax, byMin, byMax, bzMin, bzMax, xMin, xMax, yMin, yMax, zMin, zMax)
              if (spatialPass) {
                results += rawLine
              } else {
                spatialFilteredOut += 1
              }
            }
          }
        }
      }
    } finally {
      brickTable.close()
    }
    
    val step4Time = System.currentTimeMillis() - step4StartTime
    val resultCount = results.size
    
    println(s"\n[Step 4] Refined Filter lookup table precise filtering completed, time: ${step4Time}ms")
    println(s"  Final output count: ${resultCount}")
    println(s"  timeFilteredOut: ${timeFilteredOut}")
    println(s"  spatialFilteredOut: ${spatialFilteredOut}")
    
    val totalTime = System.currentTimeMillis() - globalStartTime
    println(s"\n[Incremental-Filter-Query-Summary]")
    println(s"  → Step 0 (Read Meta) time: ${step0Time}ms")
    println(s"  → Step 1 (Attribute) time: ${step1Time}ms")
    println(s"  → Step 2 (Spatial) time: ${step2Time}ms")
    println(s"  → Step 3 (Time) time: ${step3Time}ms")
    println(s"  → Step 4 (Refined Filter) time: ${step4Time}ms")
    println(s"  → Total query time: ${totalTime}ms")
    println("=" * 80 + "\n")
    
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
