// src/main/scala/spark/SparkGeoSimVoxelUnifiedQuery.scala
package spark

import index.{GeoSimVoxelGrid, VolumeTimeBucketUtc, GeoSimVolumeUnifiedScanKey, GridMeta}
import storage.HBaseTableManager

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkGeoSimVoxelUnifiedQuery {

  case class ScanTask(
    simId: String,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  )

  def runQueryWithSpark(
    zkQuorum: String,
    simId: String,
    startMs: Long,
    endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double,
    dataset: Option[String] = None,
    unifiedLevel: Option[Int] = None
  ): (Seq[String], String) = {

    val globalStartTime = System.currentTimeMillis()

    val connection = HBaseTableManager.createConnection(zkQuorum)

    try {
      val (meta, headerLine) = GeoSimVoxelGrid.readMetaWithHeader(connection, dataset)

      val level = unifiedLevel.getOrElse(4)
      val (bxSize, bySize, bzSize) = GeoSimVoxelGrid.blockSizeForLevel(level)

      val zCells = GeoSimVoxelGrid.enumerateZCellsForBBox(meta, xMin, yMin, zMin, xMax, yMax, zMax, level, maxCells = 1000000)
      val zCellsCount = zCells.size

      val dayBuckets = VolumeTimeBucketUtc.enumerateDays(startMs, endMs)
      val dayBucketsCount = dayBuckets.size

      val startDayBucket = VolumeTimeBucketUtc.dayBucket(startMs)
      val startTimeOfDay = VolumeTimeBucketUtc.timeOfDay(startMs)
      val endDayBucket = VolumeTimeBucketUtc.dayBucket(endMs)
      val endTimeOfDay = VolumeTimeBucketUtc.timeOfDay(endMs)

      val tasks: Seq[ScanTask] = zCells.map { zCell =>
        ScanTask(simId, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
      }

      val spark = SparkSession.builder()
        .appName("GeoSimVoxelUnifiedQuerySpark")
        .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      val executorInstances = sc.getConf.getInt("spark.executor.instances", 1)
      val executorCores = sc.getConf.getInt("spark.executor.cores", 1)
      val cores = executorInstances * executorCores
      val basePartitions = math.max(cores * 2, 4)
      val numPartitions = math.min(tasks.size, basePartitions)

      val tasksRdd = sc.parallelize(tasks, numPartitions)

      val step1StartTime = System.currentTimeMillis()
      val rowKeyRdd = tasksRdd.mapPartitions { iter =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        val connection = ConnectionFactory.createConnection(conf)

        try {
          val table = connection.getTable(TableName.valueOf(HBaseTableManager.volumeUnifiedIdxTableName(dataset, Some(level))))
          val cf = HBaseTableManager.CF_BYTES
          val localRowKeys = mutable.Set[String]()
          var localUnifiedRows = 0
          var localDkCount = 0

          try {
            iter.foreach { task =>
              val (startRowBytes, stopRowBytes) = GeoSimVolumeUnifiedScanKey.buildScanRangeForCellTimeSpan(
                task.simId, task.zCell,
                task.startDayBucket, task.startTimeOfDay,
                task.endDayBucket, task.endTimeOfDay
              )

              val scan = new Scan()
              scan.withStartRow(startRowBytes)
              scan.withStopRow(stopRowBytes)

              val scanner = table.getScanner(scan)
              try {
                scanner.asScala.foreach { result =>
                  localUnifiedRows += 1
                  val dkBytes = result.getValue(cf, Bytes.toBytes("dk"))
                  if (dkBytes != null) {
                    val brickRowKey = Bytes.toString(dkBytes)
                    localRowKeys += brickRowKey
                    localDkCount += 1
                  } else {
                    val rowKeyBytes = result.getRow
                    val separatorIndex = rowKeyBytes.indexOf('_'.toByte)
                    if (separatorIndex > 0 && rowKeyBytes.length > separatorIndex + 1) {
                      val brickRowKey = new String(rowKeyBytes, separatorIndex + 1, rowKeyBytes.length - separatorIndex - 1, "UTF-8")
                      localRowKeys += brickRowKey
                      localDkCount += 1
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

          Iterator((localRowKeys.toSet, localUnifiedRows, localDkCount))
        } finally {
          connection.close()
        }
      }

      val results = rowKeyRdd.collect()
      val brickRowKeys = results.flatMap(_._1).toSet
      val totalUnifiedRows = results.map(_._2).sum
      val totalDkCount = results.map(_._3).sum
      val step1Time = System.currentTimeMillis() - step1StartTime

      if (brickRowKeys.isEmpty) {
        val totalTime = System.currentTimeMillis() - globalStartTime

        println("\n" + "=" * 80)
        println("Execute unified index query")
        println("=" * 80)
        println(s"  sim_id: $simId")
        println(s"  time_range: $startMs ~ $endMs")
        println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
        println(s"  unified table name: ${HBaseTableManager.volumeUnifiedIdxTableName(dataset, Some(level))}")
        println(s"  brick table name: ${HBaseTableManager.volumeBrickTableName(dataset)}")
        println(s"  meta table name: ${HBaseTableManager.volumeMetaTableName(dataset)}")
        println(s"  unifiedLevel: $level")
        println(s"  blockSize($level): ${bxSize}x${bySize}x${bzSize}")
        println(s"  mode: spark")
        println(s"  partitions: $numPartitions")
        println(s"  executorInstances: $executorInstances")
        println(s"  cores: $cores")

        println("\n" + "-" * 80)
        println("Query statistics:")
        println(s"  Enumerated zCells count: ${zCellsCount}")
        println(s"  Enumerated dayBuckets count: ${dayBucketsCount}")
        println(s"  day_bucket_range (yyyyMMdd): ${dayBuckets.head} ~ ${dayBuckets.last}")
        println(s"  Total scan tasks: ${zCellsCount}")
        println(s"  Total scan time: ${step1Time} ms (${step1Time / 1000.0} s)")
        println(s"  Actual unified rows scanned: ${totalUnifiedRows}")
        println(s"  Data keys read: ${totalDkCount}")
        println(s"  Successful brick lookups: 0")
        println(s"  Time secondary filter eliminated: 0")
        println(s"  Spatial secondary filter eliminated: 0")
        println(s"  Final result rows: 0")
        println("-" * 80)
        println(s"\n[Unified-Query-Summary]")
        println(s"  → Initial filter time: ${step1Time}ms")
        println(s"  → Secondary filter time: 0ms")
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        (Seq.empty, headerLine)
      } else {
        val step2StartTime = System.currentTimeMillis()

        val brickTable = connection.getTable(TableName.valueOf(HBaseTableManager.volumeBrickTableName(dataset)))
        val cf = HBaseTableManager.CF_BYTES
        val acceptedRawLines = mutable.ArrayBuffer[String]()
        var brickSuccess = 0
        var timeFilteredOut = 0
        var spatialFilteredOut = 0
        var spatialRefinedPass = 0

        try {
          val gets = brickRowKeys.map { key =>
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
              brickSuccess += 1
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
                  spatialRefinedPass += 1
                  acceptedRawLines += rawLine
                } else {
                  spatialFilteredOut += 1
                }
              }
            }
          }
        } finally {
          brickTable.close()
        }

        val step2Time = System.currentTimeMillis() - step2StartTime
        val totalTime = System.currentTimeMillis() - globalStartTime

        println("\n" + "=" * 80)
        println("Execute unified index query")
        println("=" * 80)
        println(s"  sim_id: $simId")
        println(s"  time_range: $startMs ~ $endMs")
        println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
        println(s"  unified table name: ${HBaseTableManager.volumeUnifiedIdxTableName(dataset, Some(level))}")
        println(s"  brick table name: ${HBaseTableManager.volumeBrickTableName(dataset)}")
        println(s"  meta table name: ${HBaseTableManager.volumeMetaTableName(dataset)}")
        println(s"  unifiedLevel: $level")
        println(s"  blockSize($level): ${bxSize}x${bySize}x${bzSize}")
        println(s"  mode: spark")
        println(s"  partitions: $numPartitions")
        println(s"  executorInstances: $executorInstances")
        println(s"  cores: $cores")

        println("\n" + "-" * 80)
        println("Query statistics:")
        println(s"  Enumerated zCells count: ${zCellsCount}")
        println(s"  Enumerated dayBuckets count: ${dayBucketsCount}")
        println(s"  day_bucket_range (yyyyMMdd): ${dayBuckets.head} ~ ${dayBuckets.last}")
        println(s"  Total scan tasks: ${zCellsCount}")
        println(s"  Total scan time: ${step1Time} ms (${step1Time / 1000.0} s)")
        println(s"  Actual unified rows scanned: ${totalUnifiedRows}")
        println(s"  Data keys read: ${totalDkCount}")
        println(s"  Successful brick lookups: ${brickSuccess}")
        println(s"  Time secondary filter eliminated: ${timeFilteredOut}")
        println(s"  Spatial secondary filter eliminated: ${spatialFilteredOut}")
        println(s"  Final result count: ${spatialRefinedPass}")
        println("-" * 80)
        println(s"\n[Unified-Query-Summary]")
        println(s"  → Initial filter time: ${step1Time}ms")
        println(s"  → Secondary filter time: ${step2Time}ms (includes table lookup)")
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        (acceptedRawLines.toSeq, headerLine)
      }
    } finally {
      connection.close()
    }
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
}
