package spark

import index.{GeoSimVoxelGrid, GeoSimTempBucket, GeoSimVoxelTempUnifiedIndexKey, VolumeTimeBucketUtc}
import storage.HBaseTableManager

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkGeoSimVoxelTempUnifiedQuery {

  case class ChunkTask(
    tempBucket: Int,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  )

  def runQueryWithSpark(
    zkQuorum: String,
    tMin: Double, tMax: Double,
    startMs: Long, endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double,
    dataset: Option[String] = None,
    unifiedLevel: Option[Int] = None
  ): (Seq[String], String) = {

    val globalStartTime = System.currentTimeMillis()

    val connection = HBaseTableManager.createConnection(zkQuorum)

    try {
      val (gridMeta, headerLine, tempParams) = GeoSimVoxelGrid.readMetaWithHeaderAndTemp(connection, dataset)

      val (bMin, bMax) = GeoSimTempBucket.bucketRange(tMin, tMax, tempParams)
      val tempBucketsCount = bMax - bMin + 1

      val lvl = unifiedLevel.getOrElse(4)
      val zCells = GeoSimVoxelGrid.enumerateZCellsForBBox(
        gridMeta, xMin, yMin, zMin, xMax, yMax, zMax, lvl, maxCells = 1000000
      )

      val startDayBucket = VolumeTimeBucketUtc.dayBucket(startMs)
      val startTimeOfDay = VolumeTimeBucketUtc.timeOfDay(startMs)
      val endDayBucket = VolumeTimeBucketUtc.dayBucket(endMs)
      val endTimeOfDay = VolumeTimeBucketUtc.timeOfDay(endMs)

      val tasks: Seq[ChunkTask] = for {
        tempBucket <- bMin to bMax
        zCell <- zCells
      } yield {
        ChunkTask(tempBucket, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
      }

      val spark = SparkSession.builder()
        .appName("GeoSimVoxelTempUnifiedQuerySpark")
        .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      val executorInstances = sc.getConf.getInt("spark.executor.instances", -1)
      val executorCores = sc.getConf.getInt("spark.executor.cores", -1)
      val cores = if (executorInstances > 0 && executorCores > 0) executorInstances * executorCores else sc.defaultParallelism
      val basePartitions = math.max(cores * 2, 4)
      val numPartitions = math.min(tasks.size, basePartitions)

      val tasksRdd = sc.parallelize(tasks, numPartitions)

      val step1StartTime = System.currentTimeMillis()
      val rowKeyRdd = tasksRdd.mapPartitions { iter =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        val connection = ConnectionFactory.createConnection(conf)

        try {
          val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelTempUnifiedIdxTableName(dataset, unifiedLevel)))
          val localRowKeys = mutable.Set[String]()

          try {
            iter.foreach { task =>
              val (startRowBytes, stopRowBytes) = GeoSimVoxelTempUnifiedIndexKey.buildScanRangeForTempCellTimeSpan(
                task.tempBucket, task.zCell,
                task.startDayBucket, task.startTimeOfDay,
                task.endDayBucket, task.endTimeOfDay
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

          localRowKeys.iterator
        } finally {
          connection.close()
        }
      }

      val rowKeys: Set[String] = rowKeyRdd.distinct().collect().toSet
      val step1Time = System.currentTimeMillis() - step1StartTime

      if (rowKeys.isEmpty) {
        val totalTime = System.currentTimeMillis() - globalStartTime

        println("\n" + "=" * 80)
        println("[Spark-GeoSim-Voxel-Temp-Unified-Query] Execution plan:")
        println("  Initial filter: Temp + Time + Space (distributed temperature unified index scan)")
        println("  Secondary filter: Time range + Spatial bbox intersection + Temperature range (lookup table for precise filtering)")
        println("=" * 80)
        println(s"\n[Execution Parameters]")
        println(s"  → Temperature bucket width: ${tempParams.widthK}K")
        println(s"  → Temperature bucket count: $tempBucketsCount")
        println(s"  → zCells count: ${zCells.size}")
        println(s"  → Block task count: ${tasks.size}")
        println(s"  → Task partition count: ${numPartitions}")
        if (executorInstances > 0) println(s"  → Executor count: ${executorInstances}")
        else println(s"  → Executor count: defaultParallelism=${sc.defaultParallelism}")
        if (executorCores > 0) println(s"  → Core count: ${cores}")
        else println(s"  → Core count: defaultParallelism=${sc.defaultParallelism}")
        println(s"\n[Initial Filter] Temp + Time + Space distributed temperature unified index scan")
        println(s"  → Result: ${rowKeys.size} records | Time: ${step1Time}ms")
        println("=" * 80)
        println(s"[Spark-GeoSim-Voxel-Temp-Unified-Query-Summary]")
        println(s"  → Initial filter time: ${step1Time}ms")
        println(s"  → Secondary filter time: 0ms")
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        spark.stop()
        return (Seq.empty, headerLine)
      } else {
        val step2StartTime = System.currentTimeMillis()

        val brickTable = connection.getTable(TableName.valueOf(HBaseTableManager.volumeBrickTableName(dataset)))
        try {
          val gets = rowKeys.map(k => new Get(Bytes.toBytes(k))).toList.asJava
          val results: Array[Result] = brickTable.get(gets)

          val cf = HBaseTableManager.CF_BYTES
          val acceptedRecords = mutable.ListBuffer[String]()
          var totalDkCount = 0
          var totalBrickSuccess = 0
          var timeFilteredOut = 0
          var spatialFilteredOut = 0
          var tempFilteredOut = 0

          results.foreach { r =>
            if (r != null && !r.isEmpty) {
              totalDkCount += 1
              val time = Bytes.toLong(r.getValue(cf, Bytes.toBytes("time_millis")))
              val xMinBrick = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("x_min")))
              val xMaxBrick = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("x_max")))
              val yMinBrick = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("y_min")))
              val yMaxBrick = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("y_max")))
              val zMinBrick = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("z_min")))
              val zMaxBrick = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("z_max")))
              val t = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("T")))
              val rawLine = Bytes.toString(r.getValue(cf, Bytes.toBytes("raw_line")))

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
                acceptedRecords += rawLine
                totalBrickSuccess += 1
              }
            }
          }

          val step2Time = System.currentTimeMillis() - step2StartTime
          val totalTime = System.currentTimeMillis() - globalStartTime

          println("\n" + "=" * 80)
          println("[Spark-GeoSim-Voxel-Temp-Unified-Query] Execution plan:")
          println("  Initial filter: Temp + Time + Space (distributed temperature unified index scan)")
          println("  Secondary filter: Time range + Spatial bbox intersection + Temperature range (lookup table for precise filtering)")
          println("=" * 80)
          println(s"\n[Execution Parameters]")
          println(s"  → Temperature bucket width: ${tempParams.widthK}K")
          println(s"  → Temperature bucket count: $tempBucketsCount")
          println(s"  → zCells count: ${zCells.size}")
          println(s"  → Block task count: ${tasks.size}")
          println(s"  → Task partition count: ${numPartitions}")
          if (executorInstances > 0) println(s"  → Executor count: ${executorInstances}")
          else println(s"  → Executor count: defaultParallelism=${sc.defaultParallelism}")
          if (executorCores > 0) println(s"  → Core count: ${cores}")
          else println(s"  → Core count: defaultParallelism=${sc.defaultParallelism}")
          println(s"\n[Initial Filter] Temp + Time + Space distributed temperature unified index scan")
          println(s"  → Result: ${rowKeys.size} records | Time: ${step1Time}ms")
          println(s"\n[Secondary Filter] Time range + Spatial bbox intersection + Temperature range (lookup table for precise filtering)")
          println(s"  → Result: ${acceptedRecords.size} records | Time: ${step2Time}ms")
          println(s"[Secondary Filter] Scan statistics:")
          println(s"  → totalDkCount: $totalDkCount")
          println(s"  → totalBrickSuccess: $totalBrickSuccess")
          println(s"  → timeFilteredOut: $timeFilteredOut")
          println(s"  → spatialFilteredOut: $spatialFilteredOut")
          println(s"  → tempFilteredOut: $tempFilteredOut")
          println("=" * 80)
          println(s"[Spark-GeoSim-Voxel-Temp-Unified-Query-Summary]")
          println(s"  → Initial filter time: ${step1Time}ms")
          println(s"  → Secondary filter time: ${step2Time}ms (includes data read and coordinate/temperature validation)")
          println(s"  → Total query time: ${totalTime}ms")
          println("=" * 80 + "\n")

          spark.stop()
          (acceptedRecords.toSeq, headerLine)
        } finally {
          brickTable.close()
        }
      }
    } finally {
      connection.close()
    }
  }
}
