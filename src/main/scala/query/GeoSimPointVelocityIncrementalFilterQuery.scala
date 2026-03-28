package query

import index.{Z3DEncoder, TimeBucket, GeoSimCoordMapper, Bounds, GeoSimVelocityBucket}
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable

class GeoSimPointVelocityIncrementalFilterQuery(
  zkQuorum: String,
  dataset: Option[String] = None,
  unifiedLevel: Option[Int] = None,
  getBatchSize: Int = 500
) {

  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", zkQuorum)
  private val connection = ConnectionFactory.createConnection(conf)

  private var metaDelimiterName: String = ""
  private var metaHeaderLine: String = ""

  def queryRawLinesByVelocity(
    vxMin: Double, vxMax: Double,
    vyMin: Double, vyMax: Double,
    vzMin: Double, vzMax: Double,
    startMs: Long, endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double
  ): (Seq[String], String, String) = {

    val globalStartTime = System.currentTimeMillis()

    val step0StartTime = System.currentTimeMillis()
    val (bounds, delimiterName, headerLine, velParams) = GeoSimCoordMapper.readMetaWithVelocity(connection, dataset)
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

    val (vxBMin, vxBMax) = GeoSimVelocityBucket.bucketRange(vxMin, vxMax, velParams.v0, velParams.dvx, velParams.method)
    val (vyBMin, vyBMax) = GeoSimVelocityBucket.bucketRange(vyMin, vyMax, velParams.v0, velParams.dvy, velParams.method)
    val (vzBMin, vzBMax) = GeoSimVelocityBucket.bucketRange(vzMin, vzMax, velParams.v0, velParams.dvz, velParams.method)

    val vxBucketCount = vxBMax - vxBMin + 1
    val vyBucketCount = vyBMax - vyBMin + 1
    val vzBucketCount = vzBMax - vzBMin + 1
    val zCellsCount = zCells.length

    println(s"[Step0] Read Meta")
    println(s"  dvx: ${velParams.dvx}, dvy: ${velParams.dvy}, dvz: ${velParams.dvz}")
    println(s"  method: ${velParams.method}")
    println(s"  v0: ${velParams.v0}")
    println(s"  vxBucketCount: $vxBucketCount")
    println(s"  vyBucketCount: $vyBucketCount")
    println(s"  vzBucketCount: $vzBucketCount")
    println(s"  zCellsCount: $zCellsCount")

    val step0Time = System.currentTimeMillis() - step0StartTime

    val step1StartTime = System.currentTimeMillis()
    val vxEncMin = GeoSimVelocityBucket.encodeBucket(vxBMin)
    val vxEncMax = GeoSimVelocityBucket.encodeBucket(vxBMax)
    val Sx = queryVxBucketIndex(vxEncMin, vxEncMax)
    val step1Time = System.currentTimeMillis() - step1StartTime
    println(s"[Step1] VX bucket index")
    println(s"  Sx.size: ${Sx.size}")
    println(s"  step1TimeMs: $step1Time")

    if (Sx.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, 0, 0, 0, 0, 0, Sx.size, 0, 0, 0, 0, 0, totalTime)
      return (Seq.empty, metaDelimiterName, metaHeaderLine)
    }

    val step2StartTime = System.currentTimeMillis()
    val vyEncMin = GeoSimVelocityBucket.encodeBucket(vyBMin)
    val vyEncMax = GeoSimVelocityBucket.encodeBucket(vyBMax)
    val Sy = queryVyBucketIndex(vyEncMin, vyEncMax)
    val Sxy = Sx.intersect(Sy)
    val step2Time = System.currentTimeMillis() - step2StartTime
    println(s"[Step2] VY bucket index + intersection")
    println(s"  Sy.size: ${Sy.size}")
    println(s"  Sxy.size: ${Sxy.size}")
    println(s"  step2TimeMs: $step2Time")

    if (Sxy.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, 0, 0, 0, 0, Sx.size, Sy.size, Sxy.size, 0, 0, 0, totalTime)
      return (Seq.empty, metaDelimiterName, metaHeaderLine)
    }

    val step3StartTime = System.currentTimeMillis()
    val vzEncMin = GeoSimVelocityBucket.encodeBucket(vzBMin)
    val vzEncMax = GeoSimVelocityBucket.encodeBucket(vzBMax)
    val Sz = queryVzBucketIndex(vzEncMin, vzEncMax)
    val Sxyz = Sxy.intersect(Sz)
    val step3Time = System.currentTimeMillis() - step3StartTime
    println(s"[Step3] VZ bucket index + intersection")
    println(s"  Sz.size: ${Sz.size}")
    println(s"  Sxyz.size: ${Sxyz.size}")
    println(s"  step3TimeMs: $step3Time")

    if (Sxyz.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, step3Time, 0, 0, 0, Sx.size, Sy.size, Sxy.size, Sz.size, Sxyz.size, 0, totalTime)
      return (Seq.empty, metaDelimiterName, metaHeaderLine)
    }

    val step4StartTime = System.currentTimeMillis()
    val Ss = querySpatialIndex(zCells)
    val Sxyzs = Sxyz.intersect(Ss)
    val step4Time = System.currentTimeMillis() - step4StartTime
    println(s"[Step4] Spatial(block) + intersection")
    println(s"  Ss.size: ${Ss.size}")
    println(s"  Sxyzs.size: ${Sxyzs.size}")
    println(s"  step4TimeMs: $step4Time")

    if (Sxyzs.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, step3Time, step4Time, 0, 0, Sx.size, Sy.size, Sxy.size, Sz.size, Sxyz.size, Sxyzs.size, totalTime)
      return (Seq.empty, metaDelimiterName, metaHeaderLine)
    }

    val step5StartTime = System.currentTimeMillis()
    val dayBuckets = TimeBucket.dayBuckets(startMs, endMs)
    val St = queryTimeIndex(dayBuckets, startMs, endMs)
    val Sfinal = Sxyzs.intersect(St)
    val step5Time = System.currentTimeMillis() - step5StartTime
    println(s"[Step5] Time + intersection")
    println(s"  St.size: ${St.size}")
    println(s"  Sfinal.size: ${Sfinal.size}")
    println(s"  step5TimeMs: $step5Time")

    if (Sfinal.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, step3Time, step4Time, step5Time, 0, Sx.size, Sy.size, Sxy.size, Sz.size, Sxyz.size, Sxyzs.size, totalTime)
      return (Seq.empty, metaDelimiterName, metaHeaderLine)
    }

    val step6StartTime = System.currentTimeMillis()
    val result = refineFilter(Sfinal, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax, vxMin, vyMin, vzMin, vxMax, vyMax, vzMax)
    val step6Time = System.currentTimeMillis() - step6StartTime
    println(s"[Step6] Refined Filter")
    println(s"  Final count: ${result.size}")
    println(s"  step6TimeMs: $step6Time")

    val totalTime = System.currentTimeMillis() - globalStartTime
    printExecutionPlan(step0Time, step1Time, step2Time, step3Time, step4Time, step5Time, step6Time, Sx.size, Sy.size, Sxy.size, Sz.size, Sxyz.size, Sxyzs.size, totalTime)

    (result, metaDelimiterName, metaHeaderLine)
  }

  private def queryVxBucketIndex(vxEncMin: Int, vxEncMax: Int): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVxBucketIdxTableName(dataset)))
    val result = mutable.Set[String]()

    try {
      val startRow = Bytes.toBytes(vxEncMin) ++ Bytes.toBytes("_")
      val stopRow = Bytes.toBytes(vxEncMax) ++ Array[Byte](0xFF.toByte)

      val scan = new Scan()
        .setStartRow(startRow)
        .setStopRow(stopRow)
        .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))

      val scanner = table.getScanner(scan)
      scanner.asScala.foreach { r =>
        val dk = Bytes.toString(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk")))
        if (dk != null) result.add(dk)
      }
      scanner.close()
    } finally {
      table.close()
    }

    result.toSet
  }

  private def queryVyBucketIndex(vyEncMin: Int, vyEncMax: Int): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVyBucketIdxTableName(dataset)))
    val result = mutable.Set[String]()

    try {
      val startRow = Bytes.toBytes(vyEncMin) ++ Bytes.toBytes("_")
      val stopRow = Bytes.toBytes(vyEncMax) ++ Array[Byte](0xFF.toByte)

      val scan = new Scan()
        .setStartRow(startRow)
        .setStopRow(stopRow)
        .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))

      val scanner = table.getScanner(scan)
      scanner.asScala.foreach { r =>
        val dk = Bytes.toString(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk")))
        if (dk != null) result.add(dk)
      }
      scanner.close()
    } finally {
      table.close()
    }

    result.toSet
  }

  private def queryVzBucketIndex(vzEncMin: Int, vzEncMax: Int): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointVzBucketIdxTableName(dataset)))
    val result = mutable.Set[String]()

    try {
      val startRow = Bytes.toBytes(vzEncMin) ++ Bytes.toBytes("_")
      val stopRow = Bytes.toBytes(vzEncMax) ++ Array[Byte](0xFF.toByte)

      val scan = new Scan()
        .setStartRow(startRow)
        .setStopRow(stopRow)
        .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))

      val scanner = table.getScanner(scan)
      scanner.asScala.foreach { r =>
        val dk = Bytes.toString(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk")))
        if (dk != null) result.add(dk)
      }
      scanner.close()
    } finally {
      table.close()
    }

    result.toSet
  }

  private def querySpatialIndex(zCells: Seq[Long]): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointSpatialIdxTableName(dataset)))
    val result = mutable.Set[String]()

    try {
      zCells.foreach { zCell =>
        val prefix = Bytes.toBytes(zCell)
        val scan = new Scan()
          .setRowPrefixFilter(prefix)
          .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))

        val scanner = table.getScanner(scan)
        scanner.asScala.foreach { r =>
          val dk = Bytes.toString(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk")))
          if (dk != null) result.add(dk)
        }
        scanner.close()
      }
    } finally {
      table.close()
    }

    result.toSet
  }

  private def queryTimeIndex(dayBuckets: Seq[Int], startMs: Long, endMs: Long): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointTimeIdxTableName(dataset)))
    val result = mutable.Set[String]()

    try {
      dayBuckets.foreach { bucket =>
        val bucketStr = bucket.toString
        val startRow = Bytes.toBytes(bucketStr)
        val stopRow = Bytes.toBytes(bucketStr + "\uFFFF")

        val scan = new Scan()
          .setStartRow(startRow)
          .setStopRow(stopRow)
          .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))

        val scanner = table.getScanner(scan)
        scanner.asScala.foreach { r =>
          val dk = Bytes.toString(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk")))
          if (dk != null) result.add(dk)
        }
        scanner.close()
      }
    } finally {
      table.close()
    }

    result.toSet
  }

  private def refineFilter(
    candidates: Set[String],
    startMs: Long, endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double,
    vxMin: Double, vyMin: Double, vzMin: Double,
    vxMax: Double, vyMax: Double, vzMax: Double
  ): Seq[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimPointDataTableName(dataset)))
    val result = mutable.ArrayBuffer[String]()

    try {
      candidates.grouped(getBatchSize).foreach { batch =>
        val gets = batch.map { rowKey =>
          new Get(Bytes.toBytes(rowKey))
            .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("time"))
            .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("lon"))
            .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("lat"))
            .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("alt"))
            .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("vx"))
            .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("vy"))
            .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("vz"))
            .addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("raw_line"))
        }.toList

        val results = table.get(gets.asJava).toSeq

        results.foreach { r =>
          val time = Bytes.toLong(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("time")))
          val lon = Bytes.toDouble(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("lon")))
          val lat = Bytes.toDouble(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("lat")))
          val alt = Bytes.toDouble(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("alt")))
          val vx = Bytes.toDouble(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("vx")))
          val vy = Bytes.toDouble(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("vy")))
          val vz = Bytes.toDouble(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("vz")))
          val rawLine = Bytes.toString(r.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("raw_line")))

          if (time >= startMs && time <= endMs &&
              lon >= xMin && lon <= xMax &&
              lat >= yMin && lat <= yMax &&
              alt >= zMin && alt <= zMax &&
              vx >= vxMin && vx <= vxMax &&
              vy >= vyMin && vy <= vyMax &&
              vz >= vzMin && vz <= vzMax) {
            result += rawLine
          }
        }
      }
    } finally {
      table.close()
    }

    result.toSeq
  }

  private def printExecutionPlan(
    step0Time: Long, step1Time: Long, step2Time: Long, step3Time: Long,
    step4Time: Long, step5Time: Long, step6Time: Long,
    sxSize: Int, sySize: Int, sxySize: Int, szSize: Int, sxyzSize: Int, sxyzsSize: Int,
    totalTime: Long
  ): Unit = {
    println(s"[Summary] Execution Plan")
    println(s"  step0TimeMs: $step0Time")
    println(s"  step1TimeMs: $step1Time")
    println(s"  step2TimeMs: $step2Time")
    println(s"  step3TimeMs: $step3Time")
    println(s"  step4TimeMs: $step4Time")
    println(s"  step5TimeMs: $step5Time")
    println(s"  step6TimeMs: $step6Time")
    println(s"  totalTimeMs: $totalTime")
    println(s"  Sx.size: $sxSize")
    println(s"  Sy.size: $sySize")
    println(s"  Sxy.size: $sxySize")
    println(s"  Sz.size: $szSize")
    println(s"  Sxyz.size: $sxyzSize")
    println(s"  Sxyzs.size: $sxyzsSize")
  }

  def close(): Unit = {
    connection.close()
  }

  def getMeta(): (String, String) = {
    (metaDelimiterName, metaHeaderLine)
  }
}
