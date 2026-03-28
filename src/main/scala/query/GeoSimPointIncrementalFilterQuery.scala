package query

import index.{Z3DEncoder, TimeBucket, GeoSimCoordMapper, Bounds}
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable

class GeoSimPointIncrementalFilterQuery(
  zkQuorum: String,
  dataset: Option[String] = None,
  unifiedLevel: Option[Int] = None,
  fetchBatchSize: Int = 500
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

    val step0StartTime = System.currentTimeMillis()
    val (bounds, delimiterName, headerLine) = GeoSimCoordMapper.readMetaFull(connection, dataset)
    metaDelimiterName = delimiterName
    metaHeaderLine = headerLine

    val (mappedLonMin, mappedLatMin, mappedAltMin, mappedLonMax, mappedLatMax, mappedAltMax) =
      GeoSimCoordMapper.mapBBox(bounds, xMin, yMin, zMin, xMax, yMax, zMax)
    val step0Time = System.currentTimeMillis() - step0StartTime

    val step1StartTime = System.currentTimeMillis()
    val S1 = querySimIdIndex(simId)
    val step1Time = System.currentTimeMillis() - step1StartTime
    val step1Count = S1.size

    if (S1.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, 0, 0, 0, 0, step1Count, 0, 0, 0, totalTime)
      return Seq.empty
    }

    val step2StartTime = System.currentTimeMillis()
    val lvl = unifiedLevel.getOrElse(Z3DEncoder.UNIFIED_BLOCK_LEVEL)
    val zCells = Z3DEncoder.cellsAtLevel(
      mappedLonMin, mappedLatMin, mappedAltMin,
      mappedLonMax, mappedLatMax, mappedAltMax,
      level = lvl,
      maxCells = 1000000
    )
    val S2 = querySpatialIndex(zCells)
    val S12 = S1.intersect(S2)
    val step2Time = System.currentTimeMillis() - step2StartTime
    val step2Count = S2.size
    val step12Count = S12.size

    if (S12.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, 0, 0, 0, step1Count, step2Count, step12Count, 0, totalTime)
      return Seq.empty
    }

    val step3StartTime = System.currentTimeMillis()
    val dayBuckets = TimeBucket.dayBuckets(startMs, endMs)
    val S3 = queryTimeIndex(dayBuckets, startMs, endMs)
    val S123 = S12.intersect(S3)
    val step3Time = System.currentTimeMillis() - step3StartTime
    val step3Count = S3.size
    val step123Count = S123.size

    if (S123.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, step3Time, 0, 0, step1Count, step2Count, step12Count, step3Count, totalTime)
      return Seq.empty
    }

    val step4StartTime = System.currentTimeMillis()
    val rawLines = fetchAndRefine(S123, simId, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
    val step4Time = System.currentTimeMillis() - step4StartTime
    val step4Count = rawLines.size

    val totalTime = System.currentTimeMillis() - globalStartTime

    printExecutionPlan(step0Time, step1Time, step2Time, step3Time, step4Time, step4Count, step1Count, step2Count, step12Count, step3Count, totalTime)

    rawLines
  }

  private def querySimIdIndex(simId: String): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointSimIdxTableName(dataset)))
    val ids = mutable.Set[String]()

    try {
      val scan = new Scan()
      scan.setRowPrefixFilter(Bytes.toBytes(s"${simId}_"))

      val scanner = table.getScanner(scan)
      try {
        scanner.asScala.foreach { result =>
          val rowKey = Bytes.toString(result.getRow)
          val dataKey = rowKey.substring(simId.length + 1)
          ids += dataKey
        }
      } finally {
        scanner.close()
      }
    } finally {
      table.close()
    }

    ids.toSet
  }

  private def querySpatialIndex(zCells: Seq[Long]): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointSpatialIdxTableName(dataset)))
    val ids = mutable.Set[String]()

    try {
      zCells.foreach { zCell =>
        val zCellBytes = Z3DEncoder.longToBytes(zCell)

        val scan = new Scan()
        scan.setRowPrefixFilter(zCellBytes)

        val scanner = table.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val rowKeyBytes = result.getRow
            if (rowKeyBytes.length > 9) {
              val dataKey = Bytes.toString(rowKeyBytes, 9, rowKeyBytes.length - 9)
              ids += dataKey
            }
          }
        } finally {
          scanner.close()
        }
      }
    } finally {
      table.close()
    }

    ids.toSet
  }

  private def queryTimeIndex(dayBuckets: Seq[Int], startMs: Long, endMs: Long): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointTimeIdxTableName(dataset)))
    val ids = mutable.Set[String]()

    try {
      dayBuckets.foreach { bucket =>
        val startRow = f"${bucket}_${startMs}%013d_"
        val stopRow = f"${bucket}_${endMs}%013d_~"

        val scan = new Scan()
        scan.withStartRow(Bytes.toBytes(startRow))
        scan.withStopRow(Bytes.toBytes(stopRow))

        val scanner = table.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val rowKey = Bytes.toString(result.getRow)
            val parts = rowKey.split("_")
            if (parts.length >= 3) {
              ids += parts(2)
            }
          }
        } finally {
          scanner.close()
        }
      }
    } finally {
      table.close()
    }

    ids.toSet
  }

  private def fetchAndRefine(rowKeys: Set[String], simId: String,
                             startMs: Long, endMs: Long,
                             xMin: Double, yMin: Double, zMin: Double,
                             xMax: Double, yMax: Double, zMax: Double): Seq[String] = {

    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointDataTableName(dataset)))
    val result = mutable.ArrayBuffer[String]()

    try {
      val cf = HBaseTableManager.CF_BYTES
      val it = rowKeys.iterator

      while (it.hasNext) {
        val batch = new java.util.ArrayList[Get](fetchBatchSize)
        var i = 0
        while (i < fetchBatchSize && it.hasNext) {
          batch.add(new Get(Bytes.toBytes(it.next())))
          i += 1
        }

        val results: Array[Result] = table.get(batch)
        results.foreach { r =>
          if (r != null && !r.isEmpty) {
            val sensorId = Bytes.toString(r.getValue(cf, Bytes.toBytes("sensor_id")))
            val time = Bytes.toLong(r.getValue(cf, Bytes.toBytes("time")))
            val lon = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lon")))
            val lat = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("lat")))
            val alt = Bytes.toDouble(r.getValue(cf, Bytes.toBytes("alt")))
            val rawLine = Bytes.toString(r.getValue(cf, Bytes.toBytes("raw_line")))

            if (sensorId == simId &&
                time >= startMs && time <= endMs &&
                lon >= xMin && lon <= xMax &&
                lat >= yMin && lat <= yMax &&
                alt >= zMin && alt <= zMax) {
              result += rawLine
            }
          }
        }
      }
    } finally {
      table.close()
    }

    result.toSeq
  }

  private def printExecutionPlan(step0Time: Long, step1Time: Long, step2Time: Long, step3Time: Long, step4Time: Long,
                                  step4Count: Int, step1Count: Int, step2Count: Int, step12Count: Int, step3Count: Int,
                                  totalTime: Long): Unit = {
    println("\n" + "=" * 80)
    println("[GeoSim-Point-Incremental-Query] Execution plan:")
    println("  Step 1: Attribute(sim_id)")
    println("  Step 2: Spatial(block)")
    println("  Step 3: Time")
    println("  Step 4: Refined Filter (lookup table for precise filtering)")
    println("=" * 80)

    println(s"\n[Step 0] Read Meta")
    println(s"  → Time: ${step0Time}ms")

    println(s"\n[Step 1] Attribute(sim_id)")
    println(s"  → Result: ${step1Count} records | Time: ${step1Time}ms")

    if (step2Time > 0) {
      println(s"\n[Step 2] Spatial(block)")
      println(s"  → Result: ${step2Count} records | Time: ${step2Time}ms")
      println(s"  → Intersection: ${step12Count} records")
    }

    if (step3Time > 0) {
      println(s"\n[Step 3] Time")
      println(s"  → Result: ${step3Count} records | Time: ${step3Time}ms")
    }

    if (step4Time > 0) {
      println(s"\n[Step 4] Refined Filter (lookup table for precise filtering)")
      println(s"  → Result: ${step4Count} records | Time: ${step4Time}ms")
    }

    println("\n" + "=" * 80)
    println(s"[GeoSim-Point-Incremental-Query-Summary]")
    println(s"  → Step 0 Read Meta time: ${step0Time}ms")
    println(s"  → Step 1 Attribute(sim_id) time: ${step1Time}ms")
    if (step2Time > 0) println(s"  → Step 2 Spatial(block) time: ${step2Time}ms")
    if (step3Time > 0) println(s"  → Step 3 Time time: ${step3Time}ms")
    println(s"  → Step 4 Refined Filter time: ${step4Time}ms")
    println(s"  → Total query time: ${totalTime}ms")
    println("=" * 80 + "\n")
  }

  def close(): Unit = {
    if (connection != null && !connection.isClosed) {
      connection.close()
    }
  }

  def getMeta: (String, String) = (metaDelimiterName, metaHeaderLine)
}
