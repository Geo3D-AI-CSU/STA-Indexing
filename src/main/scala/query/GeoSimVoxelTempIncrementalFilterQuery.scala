package query

import index.{GeoSimVoxelGrid, GeoSimTempBucket, VolumeTimeBucketUtc}
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable

class GeoSimVoxelTempIncrementalFilterQuery(
  zkQuorum: String,
  dataset: Option[String] = None,
  unifiedLevel: Option[Int] = None,
  fetchBatchSize: Int = 500
) {

  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", zkQuorum)
  private val connection = ConnectionFactory.createConnection(conf)

  private var metaHeaderLine: String = ""

  def queryRawLinesByTemp(
    tMin: Double, tMax: Double,
    startMs: Long, endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double
  ): (Seq[String], String, String) = {

    val globalStartTime = System.currentTimeMillis()

    val step0StartTime = System.currentTimeMillis()
    val (gridMeta, headerLine, tempParams) = GeoSimVoxelGrid.readMetaWithHeaderAndTemp(connection, dataset)
    metaHeaderLine = headerLine

    val (bMin, bMax) = GeoSimTempBucket.bucketRange(tMin, tMax, tempParams)
    val tempBucketsCount = bMax - bMin + 1

    val lvl = unifiedLevel.getOrElse(4)
    val zCells = GeoSimVoxelGrid.enumerateZCellsForBBox(
      gridMeta, xMin, yMin, zMin, xMax, yMax, zMax, lvl, maxCells = 1000000
    )
    val zCellsCount = zCells.size

    val step0Time = System.currentTimeMillis() - step0StartTime

    println("\n" + "=" * 80)
    println("[GeoSim-Voxel-Temp-Incremental-Query] Execution plan:")
    println("  Step 0: Read Meta")
    println("  Step 1: Temperature(Attribute) (temperature bucket index)")
    println("  Step 2: Spatial(block)")
    println("  Step 3: Time")
    println("  Step 4: Refined Filter (lookup table for precise filtering)")
    println("=" * 80)

    println(s"\n[Step 0] Read Meta")
    println(s"  → Temperature bucket width: ${tempParams.widthK}K")
    println(s"  → Bucket range: [$bMin, $bMax]")
    println(s"  → Bucket count: $tempBucketsCount")
    println(s"  → zCellsCount: $zCellsCount")
    println(s"  → Time: ${step0Time}ms")

    val step1StartTime = System.currentTimeMillis()
    val S1 = queryTempBucketIndex(bMin, bMax)
    val step1Time = System.currentTimeMillis() - step1StartTime
    val step1Count = S1.size

    println(s"\n[Step 1] Temperature(Attribute) (temperature bucket index)")
    println(s"  → Result: ${step1Count} records | Time: ${step1Time}ms")

    if (S1.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, 0, 0, 0, 0, step1Count, 0, 0, 0, 0, totalTime)
      return (Seq.empty, "", headerLine)
    }

    val step2StartTime = System.currentTimeMillis()
    val S2 = querySpatialIndex(zCells)
    val S12 = S1.intersect(S2)
    val step2Time = System.currentTimeMillis() - step2StartTime
    val step2Count = S2.size
    val step12Count = S12.size

    println(s"\n[Step 2] Spatial(block)")
    println(s"  → Result: ${step2Count} records | Time: ${step2Time}ms")
    println(s"  → Intersection: ${step12Count} records")

    if (S12.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, 0, 0, 0, step1Count, step2Count, step12Count, 0, 0, totalTime)
      return (Seq.empty, "", headerLine)
    }

    val step3StartTime = System.currentTimeMillis()
    val dayBuckets = VolumeTimeBucketUtc.enumerateDays(startMs, endMs)
    val S3 = queryTimeIndex(dayBuckets, startMs, endMs)
    val S123 = S12.intersect(S3)
    val step3Time = System.currentTimeMillis() - step3StartTime
    val step3Count = S3.size
    val step123Count = S123.size

    println(s"\n[Step 3] Time")
    println(s"  → Result: ${step3Count} records | Time: ${step3Time}ms")
    println(s"  → Intersection: ${step123Count} records")

    if (S123.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, step3Time, 0, 0, step1Count, step2Count, step12Count, step3Count, 0, totalTime)
      return (Seq.empty, "", headerLine)
    }

    val step4StartTime = System.currentTimeMillis()
    val rawLines = fetchAndRefine(S123, tMin, tMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
    val step4Time = System.currentTimeMillis() - step4StartTime
    val step4Count = rawLines.size

    val totalTime = System.currentTimeMillis() - globalStartTime

    println(s"\n[Step 4] Refined Filter (lookup table for precise filtering)")
    println(s"  → Result: ${step4Count} records | Time: ${step4Time}ms")

    printExecutionPlan(step0Time, step1Time, step2Time, step3Time, step4Time, step4Count, step1Count, step2Count, step12Count, step3Count, 0, totalTime)

    (rawLines, "", headerLine)
  }

  private def queryTempBucketIndex(bMin: Int, bMax: Int): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelTempBucketIdxTableName(dataset)))
    val ids = mutable.Set[String]()

    try {
      val startRow = f"${bMin}%08d_"
      val stopRow = f"${bMax}%08d_~"

      val scan = new Scan()
      scan.withStartRow(Bytes.toBytes(startRow))
      scan.withStopRow(Bytes.toBytes(stopRow))

      val scanner = table.getScanner(scan)
      try {
        scanner.asScala.foreach { result =>
          val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
          if (dkBytes != null) {
            val brickRowKey = Bytes.toString(dkBytes)
            ids += brickRowKey
          }
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
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelSpatialIdxTableName(dataset, unifiedLevel)))
    val ids = mutable.Set[String]()

    try {
      zCells.foreach { zCell =>
        val zCellBytes = Bytes.toBytes(zCell)

        val scan = new Scan()
        scan.setRowPrefixFilter(zCellBytes)

        val scanner = table.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
            if (dkBytes != null) {
              val brickRowKey = Bytes.toString(dkBytes)
              ids += brickRowKey
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
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelTimeIdxTableName(dataset)))
    val ids = mutable.Set[String]()

    try {
      dayBuckets.zipWithIndex.foreach { case (bucket, idx) =>
        val (startTimeOfDay, endTimeOfDay) = if (dayBuckets.size == 1) {
          (VolumeTimeBucketUtc.timeOfDay(startMs), VolumeTimeBucketUtc.timeOfDay(endMs))
        } else if (idx == 0) {
          (VolumeTimeBucketUtc.timeOfDay(startMs), 86400000)
        } else if (idx == dayBuckets.size - 1) {
          (0, VolumeTimeBucketUtc.timeOfDay(endMs))
        } else {
          (0, 86400000)
        }
        val startRow = f"${bucket}%08d_${startTimeOfDay}%08d_"
        val stopRow = f"${bucket}%08d_${endTimeOfDay}%08d_~"

        val scan = new Scan()
        scan.withStartRow(Bytes.toBytes(startRow))
        scan.withStopRow(Bytes.toBytes(stopRow))

        val scanner = table.getScanner(scan)
        try {
          scanner.asScala.foreach { result =>
            val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
            if (dkBytes != null) {
              val brickRowKey = Bytes.toString(dkBytes)
              ids += brickRowKey
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

  private def fetchAndRefine(brickRowKeys: Set[String], tMin: Double, tMax: Double,
                           startMs: Long, endMs: Long,
                           xMin: Double, yMin: Double, zMin: Double,
                           xMax: Double, yMax: Double, zMax: Double): Seq[String] = {

    val table = connection.getTable(TableName.valueOf(HBaseTableManager.volumeBrickTableName(dataset)))
    val result = mutable.ArrayBuffer[String]()
    var timeFilteredOut = 0
    var spatialFilteredOut = 0
    var tempFilteredOut = 0

    try {
      val cf = HBaseTableManager.CF_BYTES
      val it = brickRowKeys.iterator

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
              result += rawLine
            }
          }
        }
      }
    } finally {
      table.close()
    }

    println(s"[Step 4] Refined Filter (lookup table for precise filtering) Statistics:")
    println(s"  → timeFilteredOut: $timeFilteredOut")
    println(s"  → spatialFilteredOut: $spatialFilteredOut")
    println(s"  → tempFilteredOut: $tempFilteredOut")

    result.toSeq
  }

  private def printExecutionPlan(step0Time: Long, step1Time: Long, step2Time: Long, step3Time: Long, step4Time: Long,
                                  step4Count: Int, step1Count: Int, step2Count: Int, step12Count: Int, step3Count: Int,
                                  step123Count: Int, totalTime: Long): Unit = {
    println("\n" + "=" * 80)
    println(s"[GeoSim-Voxel-Temp-Incremental-Query-Summary]")
    println(s"  → Step 0 Read Meta time: ${step0Time}ms")
    println(s"  → Step 1 Temperature(Attribute) time: ${step1Time}ms")
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

  def getMeta: String = metaHeaderLine
}
