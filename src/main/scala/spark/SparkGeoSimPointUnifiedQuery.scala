package spark

import index.{Z3DEncoder, TimeBucket, UnifiedIndexKey, GeoSimCoordMapper}
import storage.HBaseTableManager

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkGeoSimPointUnifiedQuery {

  case class ChunkTask(
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
  ): (Seq[String], String, String) = {

    val globalStartTime = System.currentTimeMillis()

    val connection = HBaseTableManager.createConnection(zkQuorum)

    try {
      val (bounds, delimiterName, headerLine) = GeoSimCoordMapper.readMetaFull(connection, dataset)

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

      val tasks: Seq[ChunkTask] = zCells.map { zCell =>
        ChunkTask(simId, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
      }

      val spark = SparkSession.builder()
        .appName("GeoSimPointUnifiedQuerySpark")
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
          val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointUnifiedIdxTableName(dataset, unifiedLevel)))
          val localRowKeys = mutable.Set[String]()

          try {
            iter.foreach { task =>
              val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForCellTimeSpan(
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

      val rowKeys: Set[String] = rowKeyRdd.distinct().collect().toSet
      val step1Time = System.currentTimeMillis() - step1StartTime

      if (rowKeys.isEmpty) {
        val totalTime = System.currentTimeMillis() - globalStartTime

        println("\n" + "=" * 80)
        println("[Spark-Unified-Query] Execution plan:")
        println("  Initial filter: SimId + Time + Space (distributed unified index scan)")
        println("  Secondary filter: Spatial precise check (coordinate range validation)")
        println("=" * 80)
        println(s"\n[Execution Parameters]")
        println(s"  → Block task count: ${tasks.size}")
        println(s"  → Task partitions: $numPartitions")
        println(s"  → Executor count: $executorInstances")
        println(s"  → Core count: $cores")
        println(s"\n[Initial Filter] SimId + Time + Space distributed unified index scan")
        println(s"  → Result: ${rowKeys.size} records | Time: ${step1Time}ms")
        println("=" * 80)
        println(s"[Spark-Unified-Query-Summary]")
        println(s"  → Initial filter time: ${step1Time}ms")
        println(s"  → Secondary filter time: 0ms")
        println(s"  → Total query time: ${totalTime}ms")
        println("=" * 80 + "\n")

        (Seq.empty, delimiterName, headerLine)
      } else {
        val step2StartTime = System.currentTimeMillis()

        val dataTable = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointDataTableName(dataset)))
        try {
          val gets = rowKeys.map(k => new Get(Bytes.toBytes(k))).toList.asJava
          val results: Array[Result] = dataTable.get(gets)

          val cf = HBaseTableManager.CF_BYTES
          val acceptedRecords = mutable.ListBuffer[String]()

          results.foreach { r =>
            if (r != null && !r.isEmpty) {
              val sensorId = Bytes.toString(r.getValue(cf, Bytes.toBytes("sensor_id")))
              val time = Bytes.toLong(r.getValue(cf, Bytes.toBytes("time")))
              val lon = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lon")))
              val lat = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lat")))
              val alt = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("alt")))
              val rawLine = Bytes.toString(r.getValue(cf, Bytes.toBytes("raw_line")))

              val matchesAll = (
                sensorId == simId &&
                time >= startMs && time <= endMs &&
                lon >= xMin && lon <= xMax &&
                lat >= yMin && lat <= yMax &&
                alt >= zMin && alt <= zMax
              )

              if (matchesAll) {
                acceptedRecords += rawLine
              }
            }
          }

          val step2Time = System.currentTimeMillis() - step2StartTime
          val totalTime = System.currentTimeMillis() - globalStartTime

          println("\n" + "=" * 80)
          println("[Spark-Unified-Query] Execution plan:")
          println("  Initial filter: SimId + Time + Space (distributed unified index scan)")
          println("  Secondary filter: Spatial precise check (coordinate range validation)")
          println("=" * 80)
          println(s"\n[Execution Parameters]")
          println(s"  → Block task count: ${tasks.size}")
          println(s"  → Task partitions: $numPartitions")
          println(s"  → Executor count: $executorInstances")
          println(s"  → Core count: $cores")
          println(s"\n[Initial Filter] SimId + Time + Space distributed unified index scan")
          println(s"  → Result: ${rowKeys.size} records | Time: ${step1Time}ms")
          println(s"\n[Secondary Filter] Spatial precise check (coordinate range validation)")
          println(s"  → Result: ${acceptedRecords.size} records | Time: ${step2Time}ms")
          println("=" * 80)
          println(s"[Spark-Unified-Query-Summary]")
          println(s"  → Initial filter time: ${step1Time}ms")
          println(s"  → Secondary filter time: ${step2Time}ms (includes data read and coordinate validation)")
          println(s"  → Total query time: ${totalTime}ms")
          println("=" * 80 + "\n")

          (acceptedRecords.toSeq, delimiterName, headerLine)
        } finally {
          dataTable.close()
        }
      }
    } finally {
      connection.close()
    }
  }
}
