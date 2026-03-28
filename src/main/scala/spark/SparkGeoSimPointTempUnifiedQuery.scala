package spark

import index.{Z3DEncoder, TimeBucket, UnifiedIndexKey, GeoSimCoordMapper, GeoSimTempBucket}
import storage.HBaseTableManager

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkGeoSimPointTempUnifiedQuery {

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
  ): (Seq[String], String, String) = {

    val globalStartTime = System.currentTimeMillis()

    val connection = HBaseTableManager.createConnection(zkQuorum)

    try {
      val (bounds, delimiterName, headerLine, tempParams) = GeoSimCoordMapper.readMetaWithTemp(connection, dataset)

      val (mappedLonMin, mappedLatMin, mappedAltMin, mappedLonMax, mappedLatMax, mappedAltMax) =
        GeoSimCoordMapper.mapBBox(bounds, xMin, yMin, zMin, xMax, yMax, zMax)

      val (bMin, bMax) = GeoSimTempBucket.bucketRange(tMin, tMax, tempParams)
      val tempBucketsCount = bMax - bMin + 1

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

      val tasks: Seq[ChunkTask] = for {
        tempBucket <- bMin to bMax
        zCell <- zCells
      } yield {
        ChunkTask(tempBucket, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
      }

      val spark = SparkSession.builder()
        .appName("GeoSimPointTempUnifiedQuerySpark")
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
          val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointTempUnifiedIdxTableName(dataset, unifiedLevel)))
          val localRowKeys = mutable.Set[String]()

          try {
            iter.foreach { task =>
              val (startRowBytes, stopRowBytes) = UnifiedIndexKey.buildScanRangeForTempCellTimeSpan(
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
                    val dataKey = Bytes.toString(dkBytes)
                    localRowKeys += dataKey
                  } else {
                    val rowKeyBytes = result.getRow
                    if (rowKeyBytes.length > 22) {
                      val dataKey = Bytes.toString(rowKeyBytes, 22, rowKeyBytes.length - 22)
                      localRowKeys += dataKey
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
        println("[Spark-Temp-Unified-Query] Execution plan:")
        println("  Initial filter: Temp + Time + Space (distributed temperature unified index scan)")
        println("  Secondary filter: Spatial precise check (coordinate range validation)")
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
        println(s"[Spark-Temp-Unified-Query-Summary]")
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
              val time = Bytes.toLong(r.getValue(cf, Bytes.toBytes("time")))
              val lon = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lon")))
              val lat = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lat")))
              val alt = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("alt")))
              val t = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("T")))
              val rawLine = Bytes.toString(r.getValue(cf, Bytes.toBytes("raw_line")))

              val matchesAll = (
                time >= startMs && time <= endMs &&
                lon >= xMin && lon <= xMax &&
                lat >= yMin && lat <= yMax &&
                alt >= zMin && alt <= zMax &&
                t >= tMin && t <= tMax
              )

              if (matchesAll) {
                acceptedRecords += rawLine
              }
            }
          }

          val step2Time = System.currentTimeMillis() - step2StartTime
          val totalTime = System.currentTimeMillis() - globalStartTime

          println("\n" + "=" * 80)
          println("[Spark-Temp-Unified-Query] Execution plan:")
          println("  Initial filter: Temp + Time + Space (distributed temperature unified index scan)")
          println("  Secondary filter: Spatial precise check (coordinate range validation)")
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
          println(s"\n[Secondary Filter] Spatial precise check (coordinate range validation)")
          println(s"  → Result: ${acceptedRecords.size} records | Time: ${step2Time}ms")
          println("=" * 80)
          println(s"[Spark-Temp-Unified-Query-Summary]")
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
