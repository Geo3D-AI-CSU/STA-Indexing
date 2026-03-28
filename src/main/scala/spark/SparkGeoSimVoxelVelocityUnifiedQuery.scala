package spark

import index.{GeoSimVoxelGrid, GeoSimVoxelVelocityUnifiedIndexKey, VolumeTimeBucketUtc}
import storage.HBaseTableManager

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkGeoSimVoxelVelocityUnifiedQuery {

  case class ChunkTask(
    bucketEnc: Int,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  )

  def runQueryWithSpark(
    zkQuorum: String,
    vxMin: Double, vxMax: Double,
    vyMin: Double, vyMax: Double,
    vzMin: Double, vzMax: Double,
    startMs: Long, endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double,
    dataset: Option[String] = None,
    unifiedLevel: Option[Int] = None
  ): (Seq[String], String, Long, Long, Long, Long) = {

    val globalStartTime = System.currentTimeMillis()

    val connection = HBaseTableManager.createConnection(zkQuorum)

    try {
      val (gridMeta, headerLine, _, velParams) = GeoSimVoxelGrid.readMetaWithHeaderTempVel(connection, dataset)

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

      val vxBucketsCount = vxBucketRawMax - vxBucketRawMin + 1
      val vyBucketsCount = vyBucketRawMax - vyBucketRawMin + 1
      val vzBucketsCount = vzBucketRawMax - vzBucketRawMin + 1

      val lvl = unifiedLevel.getOrElse(4)
      val zCells = GeoSimVoxelGrid.enumerateZCellsForBBox(
        gridMeta, xMin, yMin, zMin, xMax, yMax, zMax, lvl, maxCells = 1000000
      )

      val startDayBucket = VolumeTimeBucketUtc.dayBucket(startMs)
      val startTimeOfDay = VolumeTimeBucketUtc.timeOfDay(startMs)
      val endDayBucket = VolumeTimeBucketUtc.dayBucket(endMs)
      val endTimeOfDay = VolumeTimeBucketUtc.timeOfDay(endMs)

      val spark = SparkSession.builder()
        .appName("GeoSimVoxelVelocityUnifiedQuerySpark")
        .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      val executorInstances = sc.getConf.getInt("spark.executor.instances", -1)
      val executorCores = sc.getConf.getInt("spark.executor.cores", -1)
      val cores = if (executorInstances > 0 && executorCores > 0) executorInstances * executorCores else sc.defaultParallelism
      val basePartitions = math.max(cores * 2, 4)

      def scanOneComponent(
        tableName: String,
        bucketRawMin: Int,
        bucketRawMax: Int,
        buildRange: (Int, Long, Int, Int, Int, Int) => (Array[Byte], Array[Byte])
      ): (Set[String], Long) = {
        val tasks: Seq[ChunkTask] = for {
          bucketRaw <- bucketRawMin to bucketRawMax
          zCell <- zCells
        } yield {
          val bucketEnc = bucketRaw ^ 0x80000000
          ChunkTask(bucketEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
        }

        val numPartitions = math.min(tasks.size, basePartitions)
        val tasksRdd = sc.parallelize(tasks, numPartitions)

        val scanStartTime = System.currentTimeMillis()
        val rowKeyRdd = tasksRdd.mapPartitions { iter =>
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", zkQuorum)
          val connection = ConnectionFactory.createConnection(conf)

          try {
            val table = connection.getTable(TableName.valueOf(tableName))
            val localRowKeys = mutable.Set[String]()

            try {
              iter.foreach { task =>
                val (startRowBytes, stopRowBytes) = buildRange(
                  task.bucketEnc, task.zCell,
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
        val scanMs = System.currentTimeMillis() - scanStartTime
        (rowKeys, scanMs)
      }

      val vxTableName = HBaseTableManager.geoSimVoxelVxUnifiedIdxTableName(dataset, Some(lvl))
      val vyTableName = HBaseTableManager.geoSimVoxelVyUnifiedIdxTableName(dataset, Some(lvl))
      val vzTableName = HBaseTableManager.geoSimVoxelVzUnifiedIdxTableName(dataset, Some(lvl))

      val (sx, scanVxMs) = scanOneComponent(
        vxTableName, vxBucketRawMin, vxBucketRawMax,
        GeoSimVoxelVelocityUnifiedIndexKey.vxScanRangeTimeSpan
      )

      val (sy, scanVyMs) = scanOneComponent(
        vyTableName, vyBucketRawMin, vyBucketRawMax,
        GeoSimVoxelVelocityUnifiedIndexKey.vyScanRangeTimeSpan
      )

      val (sz, scanVzMs) = scanOneComponent(
        vzTableName, vzBucketRawMin, vzBucketRawMax,
        GeoSimVoxelVelocityUnifiedIndexKey.vzScanRangeTimeSpan
      )

      val brickRowKeys = sx.intersect(sy).intersect(sz)
      val scanTotalMs = scanVxMs + scanVyMs + scanVzMs
      val tasksCountVx = vxBucketsCount * zCells.size
      val tasksCountVy = vyBucketsCount * zCells.size
      val tasksCountVz = vzBucketsCount * zCells.size

      if (brickRowKeys.isEmpty) {
        val totalTime = System.currentTimeMillis() - globalStartTime

        println("\n" + "=" * 80)
        println("[Spark-GeoSim-Voxel-Velocity-Unified-Query] Execution plan:")
        println("  Initial filter: VxBucket + VyBucket + VzBucket + Time + Space (distributed velocity unified index scan)")
        println("  Secondary filter: Time range + Spatial bbox intersection + Velocity range (lookup table for precise filtering)")
        println("=" * 80)
        println(s"\n[Execution Parameters]")
        println(s"  → dvx: $dvx")
        println(s"  → dvy: $dvy")
        println(s"  → dvz: $dvz")
        println(s"  → vxBucket count: $vxBucketsCount")
        println(s"  → vyBucket count: $vyBucketsCount")
        println(s"  → vzBucket count: $vzBucketsCount")
        println(s"  → Spatial grid count: ${zCells.size}")
        println(s"  → tasksCountVx: $tasksCountVx")
        println(s"  → tasksCountVy: $tasksCountVy")
        println(s"  → tasksCountVz: $tasksCountVz")
        if (executorInstances > 0) println(s"  → Executor count: ${executorInstances}")
        else println(s"  → Executor count: defaultParallelism=${sc.defaultParallelism}")
        if (executorCores > 0) println(s"  → Core count: ${cores}")
        else println(s"  → Core count: defaultParallelism=${sc.defaultParallelism}")
        println(s"\n[Initial Filter] Vx + Vy + Vz distributed velocity unified index scan")
        println(s"  → scanVxMs: ${scanVxMs}ms")
        println(s"  → scanVyMs: ${scanVyMs}ms")
        println(s"  → scanVzMs: ${scanVzMs}ms")
        println(s"  → scanTotalMs: ${scanTotalMs}ms")
        println("=" * 80)
        println(s"[Spark-GeoSim-Voxel-Velocity-Unified-Query-Summary]")
        println(s"  → Initial filter time: ${scanTotalMs}ms")
        println(s"  → Secondary filter time: 0ms")
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        spark.stop()
        return (Seq.empty, headerLine, scanVxMs, scanVyMs, scanVzMs, scanTotalMs)
      } else {
        val step2StartTime = System.currentTimeMillis()

        val brickTable = connection.getTable(TableName.valueOf(HBaseTableManager.volumeBrickTableName(dataset)))
        try {
          val gets = brickRowKeys.map(k => new Get(Bytes.toBytes(k))).toList.asJava
          val results: Array[Result] = brickTable.get(gets)

          val cf = HBaseTableManager.CF_BYTES
          val acceptedRecords = mutable.ListBuffer[String]()
          var totalDkCount = 0
          var totalBrickSuccess = 0
          var timeFilteredOut = 0
          var spatialFilteredOut = 0
          var vxFilteredOut = 0
          var vyFilteredOut = 0
          var vzFilteredOut = 0

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
              val vx = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("vx")))
              val vy = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("vy")))
              val vz = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("vz")))
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
                acceptedRecords += rawLine
                totalBrickSuccess += 1
              }
            }
          }

          val step2Time = System.currentTimeMillis() - step2StartTime
          val totalTime = System.currentTimeMillis() - globalStartTime

          println("\n" + "=" * 80)
          println("[Spark-GeoSim-Voxel-Velocity-Unified-Query] Execution plan:")
          println("  Initial filter: VxBucket + VyBucket + VzBucket + Time + Space (distributed velocity unified index scan)")
          println("  Secondary filter: Time range + Spatial bbox intersection + Velocity range (lookup table for precise filtering)")
          println("=" * 80)
          println(s"\n[Execution Parameters]")
          println(s"  → dvx: $dvx")
          println(s"  → dvy: $dvy")
          println(s"  → dvz: $dvz")
          println(s"  → vxBucket count: $vxBucketsCount")
          println(s"  → vyBucket count: $vyBucketsCount")
          println(s"  → vzBucket count: $vzBucketsCount")
          println(s"  → Spatial grid count: ${zCells.size}")
          println(s"  → tasksCountVx: $tasksCountVx")
          println(s"  → tasksCountVy: $tasksCountVy")
          println(s"  → tasksCountVz: $tasksCountVz")
          if (executorInstances > 0) println(s"  → Executor count: ${executorInstances}")
          else println(s"  → Executor count: defaultParallelism=${sc.defaultParallelism}")
          if (executorCores > 0) println(s"  → Core count: ${cores}")
          else println(s"  → Core count: defaultParallelism=${sc.defaultParallelism}")
          println(s"\n[Initial Filter] Vx + Vy + Vz distributed velocity unified index scan")
          println(s"  → scanVxMs: ${scanVxMs}ms")
          println(s"  → scanVyMs: ${scanVyMs}ms")
          println(s"  → scanVzMs: ${scanVzMs}ms")
          println(s"  → scanTotalMs: ${scanTotalMs}ms")
          println(s"\n[Secondary Filter] Time range + Spatial bbox intersection + Velocity range (lookup table for precise filtering)")
          println(s"  → Result: ${acceptedRecords.size} records | Time: ${step2Time}ms")
          println(s"[Secondary Filter] Scan statistics:")
          println(s"  → totalDkCount: $totalDkCount")
          println(s"  → totalBrickSuccess: $totalBrickSuccess")
          println(s"  → timeFilteredOut: $timeFilteredOut")
          println(s"  → spatialFilteredOut: $spatialFilteredOut")
          println(s"  → vxFilteredOut: $vxFilteredOut")
          println(s"  → vyFilteredOut: $vyFilteredOut")
          println(s"  → vzFilteredOut: $vzFilteredOut")
          println("=" * 80)
          println(s"[Spark-GeoSim-Voxel-Velocity-Unified-Query-Summary]")
          println(s"  → Initial filter time: ${scanTotalMs}ms")
          println(s"  → Secondary filter time: ${step2Time}ms (includes data read and coordinate/velocity validation)")
          println(s"  → Total query time: ${totalTime}ms")
          println("=" * 80 + "\n")

          spark.stop()
          (acceptedRecords.toSeq, headerLine, scanVxMs, scanVyMs, scanVzMs, scanTotalMs)
        } finally {
          brickTable.close()
        }
      }
    } finally {
      connection.close()
    }
  }
}
