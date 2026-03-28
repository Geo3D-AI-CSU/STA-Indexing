package spark

import index.{Z3DEncoder, TimeBucket, UnifiedIndexKey, GeoSimCoordMapper, GeoSimVelocityBucket}
import storage.HBaseTableManager

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkGeoSimPointVelocityUnifiedQuery {

  case class VxTask(
    vxBucketRaw: Int,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  )

  case class VyTask(
    vyBucketRaw: Int,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  )

  case class VzTask(
    vzBucketRaw: Int,
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
    dataset: Option[String],
    unifiedLevel: Option[Int]
  ): (Seq[String], String, String, Long, Long, Long, Long) = {

    val globalStartTime = System.currentTimeMillis()

    val connection = HBaseTableManager.createConnection(zkQuorum)

    try {
      val (bounds, delimiterName, headerLine, velParams) = GeoSimCoordMapper.readMetaWithVelocity(connection, dataset)

      val (mappedLonMin, mappedLatMin, mappedAltMin, mappedLonMax, mappedLatMax, mappedAltMax) =
        GeoSimCoordMapper.mapBBox(bounds, xMin, yMin, zMin, xMax, yMax, zMax)

      val lvl = unifiedLevel.getOrElse(Z3DEncoder.UNIFIED_BLOCK_LEVEL)
      val zCells = Z3DEncoder.cellsAtLevel(
        mappedLonMin, mappedLatMin, mappedAltMin,
        mappedLonMax, mappedLatMax, mappedAltMax,
        level = lvl,
        maxCells = 1000000
      )

      val startDayBucket = TimeBucket.dayBucket(startMs)
      val startTimeOfDay = TimeBucket.millisOfDay(startMs)
      val endDayBucket = TimeBucket.dayBucket(endMs)
      val endTimeOfDay = TimeBucket.millisOfDay(endMs)

      val (vxBMin, vxBMax) = GeoSimVelocityBucket.bucketRange(vxMin, vxMax, velParams.v0, velParams.dvx, velParams.method)
      val (vyBMin, vyBMax) = GeoSimVelocityBucket.bucketRange(vyMin, vyMax, velParams.v0, velParams.dvy, velParams.method)
      val (vzBMin, vzBMax) = GeoSimVelocityBucket.bucketRange(vzMin, vzMax, velParams.v0, velParams.dvz, velParams.method)

      val vxBucketCount = vxBMax - vxBMin + 1
      val vyBucketCount = vyBMax - vyBMin + 1
      val vzBucketCount = vzBMax - vzBMin + 1
      val zCellsCount = zCells.length

      val spark = SparkSession.builder()
        .appName("GeoSimPointVelocityUnifiedQuerySpark")
        .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")

      val executorInstances = sc.getConf.getInt("spark.executor.instances", 1)
      val executorCores = sc.getConf.getInt("spark.executor.cores", 1)
      val cores = executorInstances * executorCores

      val vxTasks: Seq[VxTask] = for {
        vxBucketRaw <- vxBMin to vxBMax
        zCell <- zCells
      } yield VxTask(vxBucketRaw, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)

      val vyTasks: Seq[VyTask] = for {
        vyBucketRaw <- vyBMin to vyBMax
        zCell <- zCells
      } yield VyTask(vyBucketRaw, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)

      val vzTasks: Seq[VzTask] = for {
        vzBucketRaw <- vzBMin to vzBMax
        zCell <- zCells
      } yield VzTask(vzBucketRaw, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)

      val basePartitions = math.max(cores * 2, 4)
      val numPartitionsVx = math.min(vxTasks.size, basePartitions)
      val numPartitionsVy = math.min(vyTasks.size, basePartitions)
      val numPartitionsVz = math.min(vzTasks.size, basePartitions)

      val tasksCountX = vxTasks.size
      val tasksCountY = vyTasks.size
      val tasksCountZ = vzTasks.size

      val vxTasksRdd = sc.parallelize(vxTasks, numPartitionsVx)
      val vyTasksRdd = sc.parallelize(vyTasks, numPartitionsVy)
      val vzTasksRdd = sc.parallelize(vzTasks, numPartitionsVz)

      val scanVxStartTime = System.currentTimeMillis()
      val vxRowKeyRdd = vxTasksRdd.mapPartitions { iter =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        val connection = ConnectionFactory.createConnection(conf)

        try {
          val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVxUnifiedIdxTableName(dataset, unifiedLevel)))
          val localRowKeys = mutable.Set[String]()

          try {
            iter.foreach { task =>
              val vxEnc = GeoSimVelocityBucket.encodeBucket(task.vxBucketRaw)
              val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForVxCellTimeSpan(
                vxEnc, task.zCell,
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

          localRowKeys.iterator
        } finally {
          connection.close()
        }
      }

      val Sx: Set[String] = vxRowKeyRdd.distinct().collect().toSet
      val scanVxMs = System.currentTimeMillis() - scanVxStartTime

      if (Sx.isEmpty) {
        val scanTotalMs = scanVxMs
        val totalTime = System.currentTimeMillis() - globalStartTime

        println("\n" + "=" * 80)
        println("[Spark-Velocity-Unified-Query] Execution plan:")
        println("  Initial filter: VX + VY + VZ + Time + Space (distributed unified index scan)")
        println("  Secondary filter: Precise validation (coordinate range + velocity range validation)")
        println("=" * 80)
        println(s"\n[Execution Parameters]")
        println(s"  dvx: ${velParams.dvx}, dvy: ${velParams.dvy}, dvz: ${velParams.dvz}")
        println(s"  method: ${velParams.method}")
        println(s"  v0: ${velParams.v0}")
        println(s"  vxBucketCount: $vxBucketCount")
        println(s"  vyBucketCount: $vyBucketCount")
        println(s"  vzBucketCount: $vzBucketCount")
        println(s"  zCellsCount: $zCellsCount")
        println(s"  tasksCountX: $tasksCountX")
        println(s"  tasksCountY: $tasksCountY")
        println(s"  tasksCountZ: $tasksCountZ")
        println(s"  Executor count: $executorInstances")
        println(s"  Core count: $cores")
        println(s"\n[Initial Filter] VX distributed unified index scan")
        println(s"  → Result: ${Sx.size} records | Time: ${scanVxMs}ms")
        println("=" * 80)
        println(s"[Spark-Velocity-Unified-Query-Summary]")
        println(s"  → scanVxMs: ${scanVxMs}ms")
        println(s"  → scanVyMs: 0ms")
        println(s"  → scanVzMs: 0ms")
        println(s"  → scanTotalMs: ${scanTotalMs}ms")
        println(s"  → Secondary filter time: 0ms")
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        return (Seq.empty, delimiterName, headerLine, scanVxMs, 0L, 0L, scanTotalMs)
      }

      val scanVyStartTime = System.currentTimeMillis()
      val vyRowKeyRdd = vyTasksRdd.mapPartitions { iter =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        val connection = ConnectionFactory.createConnection(conf)

        try {
          val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVyUnifiedIdxTableName(dataset, unifiedLevel)))
          val localRowKeys = mutable.Set[String]()

          try {
            iter.foreach { task =>
              val vyEnc = GeoSimVelocityBucket.encodeBucket(task.vyBucketRaw)
              val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForVyCellTimeSpan(
                vyEnc, task.zCell,
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

          localRowKeys.iterator
        } finally {
          connection.close()
        }
      }

      val Sy: Set[String] = vyRowKeyRdd.distinct().collect().toSet
      val scanVyMs = System.currentTimeMillis() - scanVyStartTime

      val Sxy = Sx.intersect(Sy)

      if (Sxy.isEmpty) {
        val scanTotalMs = scanVxMs + scanVyMs
        val totalTime = System.currentTimeMillis() - globalStartTime

        println("\n" + "=" * 80)
        println("[Spark-Velocity-Unified-Query] Execution plan:")
        println("  Initial filter: VX + VY + VZ + Time + Space (distributed unified index scan)")
        println("  Secondary filter: Precise validation (coordinate range + velocity range validation)")
        println("=" * 80)
        println(s"\n[Execution Parameters]")
        println(s"  dvx: ${velParams.dvx}, dvy: ${velParams.dvy}, dvz: ${velParams.dvz}")
        println(s"  method: ${velParams.method}")
        println(s"  v0: ${velParams.v0}")
        println(s"  vxBucketCount: $vxBucketCount")
        println(s"  vyBucketCount: $vyBucketCount")
        println(s"  vzBucketCount: $vzBucketCount")
        println(s"  zCellsCount: $zCellsCount")
        println(s"  tasksCountX: $tasksCountX")
        println(s"  tasksCountY: $tasksCountY")
        println(s"  tasksCountZ: $tasksCountZ")
        println(s"  Executor count: $executorInstances")
        println(s"  Core count: $cores")
        println(s"\n[Initial Filter] VX distributed unified index scan")
        println(s"  → Result: ${Sx.size} records | Time: ${scanVxMs}ms")
        println(s"\n[Initial Filter] VY distributed unified index scan")
        println(s"  → Result: ${Sy.size} records | Time: ${scanVyMs}ms")
        println(s"  → Intersection Sxy: ${Sxy.size} records")
        println("=" * 80)
        println(s"[Spark-Velocity-Unified-Query-Summary]")
        println(s"  → scanVxMs: ${scanVxMs}ms")
        println(s"  → scanVyMs: ${scanVyMs}ms")
        println(s"  → scanVzMs: 0ms")
        println(s"  → scanTotalMs: ${scanTotalMs}ms")
        println(s"  → Secondary filter time: 0ms")
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        return (Seq.empty, delimiterName, headerLine, scanVxMs, scanVyMs, 0L, scanTotalMs)
      }

      val scanVzStartTime = System.currentTimeMillis()
      val vzRowKeyRdd = vzTasksRdd.mapPartitions { iter =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        val connection = ConnectionFactory.createConnection(conf)

        try {
          val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVzUnifiedIdxTableName(dataset, unifiedLevel)))
          val localRowKeys = mutable.Set[String]()

          try {
            iter.foreach { task =>
              val vzEnc = GeoSimVelocityBucket.encodeBucket(task.vzBucketRaw)
              val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForVzCellTimeSpan(
                vzEnc, task.zCell,
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

          localRowKeys.iterator
        } finally {
          connection.close()
        }
      }

      val Sz: Set[String] = vzRowKeyRdd.distinct().collect().toSet
      val scanVzMs = System.currentTimeMillis() - scanVzStartTime

      val Sfinal = Sxy.intersect(Sz)

      if (Sfinal.isEmpty) {
        val scanTotalMs = scanVxMs + scanVyMs + scanVzMs
        val totalTime = System.currentTimeMillis() - globalStartTime

        println("\n" + "=" * 80)
        println("[Spark-Velocity-Unified-Query] Execution plan:")
        println("  Initial filter: VX + VY + VZ + Time + Space (distributed unified index scan)")
        println("  Secondary filter: Precise validation (coordinate range + velocity range validation)")
        println("=" * 80)
        println(s"\n[Execution Parameters]")
        println(s"  dvx: ${velParams.dvx}, dvy: ${velParams.dvy}, dvz: ${velParams.dvz}")
        println(s"  method: ${velParams.method}")
        println(s"  v0: ${velParams.v0}")
        println(s"  vxBucketCount: $vxBucketCount")
        println(s"  vyBucketCount: $vyBucketCount")
        println(s"  vzBucketCount: $vzBucketCount")
        println(s"  zCellsCount: $zCellsCount")
        println(s"  tasksCountX: $tasksCountX")
        println(s"  tasksCountY: $tasksCountY")
        println(s"  tasksCountZ: $tasksCountZ")
        println(s"  Executor count: $executorInstances")
        println(s"  Core count: $cores")
        println(s"\n[Initial Filter] VX distributed unified index scan")
        println(s"  → Result: ${Sx.size} records | Time: ${scanVxMs}ms")
        println(s"\n[Initial Filter] VY distributed unified index scan")
        println(s"  → Result: ${Sy.size} records | Time: ${scanVyMs}ms")
        println(s"  → Intersection Sxy: ${Sxy.size} records")
        println(s"\n[Initial Filter] VZ distributed unified index scan")
        println(s"  → Result: ${Sz.size} records | Time: ${scanVzMs}ms")
        println(s"  → Intersection Sfinal: ${Sfinal.size} records")
        println("=" * 80)
        println(s"[Spark-Velocity-Unified-Query-Summary]")
        println(s"  → scanVxMs: ${scanVxMs}ms")
        println(s"  → scanVyMs: ${scanVyMs}ms")
        println(s"  → scanVzMs: ${scanVzMs}ms")
        println(s"  → scanTotalMs: ${scanTotalMs}ms")
        println(s"  → Secondary filter time: 0ms")
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        return (Seq.empty, delimiterName, headerLine, scanVxMs, scanVyMs, scanVzMs, scanTotalMs)
      }

      val refineStartTime = System.currentTimeMillis()

      val dataTable = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointDataTableName(dataset)))
      try {
        val gets = Sfinal.map(k => new Get(Bytes.toBytes(k))).toList.asJava
        val results: Array[Result] = dataTable.get(gets)

        val cf = HBaseTableManager.CF_BYTES
        val acceptedRecords = mutable.ListBuffer[String]()

        results.foreach { r =>
          if (r != null && !r.isEmpty) {
            val time = Bytes.toLong(r.getValue(cf, Bytes.toBytes("time")))
            val lon = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lon")))
            val lat = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lat")))
            val alt = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("alt")))
            val vx = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("vx")))
            val vy = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("vy")))
            val vz = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("vz")))
            val rawLine = Bytes.toString(r.getValue(cf, Bytes.toBytes("raw_line")))

            val matchesAll = (
              time >= startMs && time <= endMs &&
              lon >= xMin && lon <= xMax &&
              lat >= yMin && lat <= yMax &&
              alt >= zMin && alt <= zMax &&
              vx >= vxMin && vx <= vxMax &&
              vy >= vyMin && vy <= vyMax &&
              vz >= vzMin && vz <= vzMax
            )

            if (matchesAll) {
              acceptedRecords += rawLine
            }
          }
        }

        val refineTime = System.currentTimeMillis() - refineStartTime
        val scanTotalMs = scanVxMs + scanVyMs + scanVzMs
        val totalTime = System.currentTimeMillis() - globalStartTime

        println("\n" + "=" * 80)
        println("[Spark-Velocity-Unified-Query] Execution plan:")
        println("  Initial filter: VX + VY + VZ + Time + Space (distributed unified index scan)")
        println("  Secondary filter: Precise validation (coordinate range + velocity range validation)")
        println("=" * 80)
        println(s"\n[Execution Parameters]")
        println(s"  dvx: ${velParams.dvx}, dvy: ${velParams.dvy}, dvz: ${velParams.dvz}")
        println(s"  method: ${velParams.method}")
        println(s"  v0: ${velParams.v0}")
        println(s"  vxBucketCount: $vxBucketCount")
        println(s"  vyBucketCount: $vyBucketCount")
        println(s"  vzBucketCount: $vzBucketCount")
        println(s"  zCellsCount: $zCellsCount")
        println(s"  tasksCountX: $tasksCountX")
        println(s"  tasksCountY: $tasksCountY")
        println(s"  tasksCountZ: $tasksCountZ")
        println(s"  Executor count: $executorInstances")
        println(s"  Core count: $cores")
        println(s"\n[Initial Filter] VX distributed unified index scan")
        println(s"  → Result: ${Sx.size} records | Time: ${scanVxMs}ms")
        println(s"\n[Initial Filter] VY distributed unified index scan")
        println(s"  → Result: ${Sy.size} records | Time: ${scanVyMs}ms")
        println(s"  → Intersection Sxy: ${Sxy.size} records")
        println(s"\n[Initial Filter] VZ distributed unified index scan")
        println(s"  → Result: ${Sz.size} records | Time: ${scanVzMs}ms")
        println(s"  → Intersection Sfinal: ${Sfinal.size} records")
        println(s"\n[Secondary Filter] Precise validation (coordinate range + velocity range validation)")
        println(s"  → Result: ${acceptedRecords.size} records | Time: ${refineTime}ms")
        println("=" * 80)
        println(s"[Spark-Velocity-Unified-Query-Summary]")
        println(s"  → scanVxMs: ${scanVxMs}ms")
        println(s"  → scanVyMs: ${scanVyMs}ms")
        println(s"  → scanVzMs: ${scanVzMs}ms")
        println(s"  → scanTotalMs: ${scanTotalMs}ms")
        println(s"  → Secondary filter time: ${refineTime}ms")
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        (acceptedRecords.toSeq, delimiterName, headerLine, scanVxMs, scanVyMs, scanVzMs, scanTotalMs)
      } finally {
        dataTable.close()
      }
    } finally {
      connection.close()
    }
  }
}