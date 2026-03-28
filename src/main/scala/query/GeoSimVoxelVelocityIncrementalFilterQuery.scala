package query

import index.{GeoSimVoxelGrid, Z3DEncoder, VolumeTimeBucketUtc}
import storage.HBaseTableManager

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable

class GeoSimVoxelVelocityIncrementalFilterQuery(
  zkQuorum: String,
  dataset: Option[String] = None,
  unifiedLevel: Option[Int] = None,
  getBatchSize: Int = 500
) {

  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", zkQuorum)
  private val connection = ConnectionFactory.createConnection(conf)

  private var metaHeaderLine: String = ""

  def queryRawLinesByVelocity(
    vxMin: Double, vxMax: Double,
    vyMin: Double, vyMax: Double,
    vzMin: Double, vzMax: Double,
    startMs: Long, endMs: Long,
    xMin: Double, yMin: Double, zMin: Double,
    xMax: Double, yMax: Double, zMax: Double
  ): (Seq[String], String) = {

    val globalStartTime = System.currentTimeMillis()

    val step0StartTime = System.currentTimeMillis()
    val (gridMeta, headerLine, _, velParams) = GeoSimVoxelGrid.readMetaWithHeaderTempVel(connection, dataset)
    metaHeaderLine = headerLine

    val lvl = unifiedLevel.getOrElse(4)

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

    val vxBucketCount = vxBucketRawMax - vxBucketRawMin + 1
    val vyBucketCount = vyBucketRawMax - vyBucketRawMin + 1
    val vzBucketCount = vzBucketRawMax - vzBucketRawMin + 1

    val zCells = GeoSimVoxelGrid.enumerateZCellsForBBox(
      gridMeta, xMin, yMin, zMin, xMax, yMax, zMax, lvl, maxCells = 1000000
    )
    val zCellsCount = zCells.length

    val startDayBucket = VolumeTimeBucketUtc.dayBucket(startMs)
    val endDayBucket = VolumeTimeBucketUtc.dayBucket(endMs)
    val dayBucketsCount = endDayBucket - startDayBucket + 1

    println(s"[Step0] Read Meta")
    println(s"  dvx: $dvx")
    println(s"  dvy: $dvy")
    println(s"  dvz: $dvz")
    println(s"  method: $method")
    println(s"  v0: $v0")
    println(s"  vxBucketCount: $vxBucketCount")
    println(s"  vyBucketCount: $vyBucketCount")
    println(s"  vzBucketCount: $vzBucketCount")
    println(s"  zCellsCount: $zCellsCount")
    println(s"  dayBucketsCount: $dayBucketsCount")

    val step0Time = System.currentTimeMillis() - step0StartTime

    val step1StartTime = System.currentTimeMillis()
    val vxEncMin = vxBucketRawMin ^ 0x80000000
    val vxEncMax = vxBucketRawMax ^ 0x80000000
    val sx = queryVxBucketIndex(vxEncMin, vxEncMax)
    val step1Time = System.currentTimeMillis() - step1StartTime
    println(s"[Step1] VX bucket index")
    println(s"  sx.size: ${sx.size}")
    println(s"  step1TimeMs: $step1Time")

    if (sx.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, 0, 0, 0, 0, 0, sx.size, 0, 0, 0, 0, 0, totalTime)
      return (Seq.empty, metaHeaderLine)
    }

    val step2StartTime = System.currentTimeMillis()
    val vyEncMin = vyBucketRawMin ^ 0x80000000
    val vyEncMax = vyBucketRawMax ^ 0x80000000
    val sy = queryVyBucketIndex(vyEncMin, vyEncMax)
    val sxy = sx.intersect(sy)
    val step2Time = System.currentTimeMillis() - step2StartTime
    println(s"[Step2] VY bucket index + intersection")
    println(s"  sy.size: ${sy.size}")
    println(s"  sxy.size: ${sxy.size}")
    println(s"  step2TimeMs: $step2Time")

    if (sxy.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, 0, 0, 0, 0, sx.size, sy.size, sxy.size, 0, 0, 0, totalTime)
      return (Seq.empty, metaHeaderLine)
    }

    val step3StartTime = System.currentTimeMillis()
    val vzEncMin = vzBucketRawMin ^ 0x80000000
    val vzEncMax = vzBucketRawMax ^ 0x80000000
    val sz = queryVzBucketIndex(vzEncMin, vzEncMax)
    val sxyz = sxy.intersect(sz)
    val step3Time = System.currentTimeMillis() - step3StartTime
    println(s"[Step3] VZ bucket index + intersection")
    println(s"  sz.size: ${sz.size}")
    println(s"  sxyz.size: ${sxyz.size}")
    println(s"  step3TimeMs: $step3Time")

    if (sxyz.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, step3Time, 0, 0, 0, sx.size, sy.size, sxy.size, sz.size, sxyz.size, 0, totalTime)
      return (Seq.empty, metaHeaderLine)
    }

    val step4StartTime = System.currentTimeMillis()
    val ss = querySpatialIndex(zCells, lvl)
    val sxyzs = sxyz.intersect(ss)
    val step4Time = System.currentTimeMillis() - step4StartTime
    println(s"[Step4] Spatial(block) + intersection")
    println(s"  ss.size: ${ss.size}")
    println(s"  sxyzs.size: ${sxyzs.size}")
    println(s"  step4TimeMs: $step4Time")

    if (sxyzs.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, step3Time, step4Time, 0, 0, sx.size, sy.size, sxy.size, sz.size, sxyz.size, sxyzs.size, totalTime)
      return (Seq.empty, metaHeaderLine)
    }

    val step5StartTime = System.currentTimeMillis()
    val st = queryTimeIndex(startDayBucket, endDayBucket, startMs, endMs)
    val sfinal = sxyzs.intersect(st)
    val step5Time = System.currentTimeMillis() - step5StartTime
    println(s"[Step5] Time + intersection")
    println(s"  st.size: ${st.size}")
    println(s"  sfinal.size: ${sfinal.size}")
    println(s"  step5TimeMs: $step5Time")

    if (sfinal.isEmpty) {
      val totalTime = System.currentTimeMillis() - globalStartTime
      printExecutionPlan(step0Time, step1Time, step2Time, step3Time, step4Time, step5Time, 0, sx.size, sy.size, sxy.size, sz.size, sxyz.size, sxyzs.size, totalTime)
      return (Seq.empty, metaHeaderLine)
    }

    val step6StartTime = System.currentTimeMillis()
    val result = refineFilter(sfinal, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax, vxMin, vyMin, vzMin, vxMax, vyMax, vzMax)
    val step6Time = System.currentTimeMillis() - step6StartTime
    println(s"[Step6] Refined Filter")
    println(s"  Final count: ${result.size}")
    println(s"  step6TimeMs: $step6Time")

    val totalTime = System.currentTimeMillis() - globalStartTime
    printExecutionPlan(step0Time, step1Time, step2Time, step3Time, step4Time, step5Time, step6Time, sx.size, sy.size, sxy.size, sz.size, sxyz.size, sxyzs.size, totalTime)

    (result, metaHeaderLine)
  }

  private def queryVxBucketIndex(vxEncMin: Int, vxEncMax: Int): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelVxBucketIdxTableName(dataset)))
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
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelVyBucketIdxTableName(dataset)))
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
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelVzBucketIdxTableName(dataset)))
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

  private def querySpatialIndex(zCells: Seq[Long], lvl: Int): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelSpatialIdxTableName(dataset, Some(lvl))))
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

  private def queryTimeIndex(startDayBucket: Int, endDayBucket: Int, startMs: Long, endMs: Long): Set[String] = {
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.geoSimVoxelTimeIdxTableName(dataset)))
    val result = mutable.Set[String]()

    try {
      for (dayBucket <- startDayBucket to endDayBucket) {
        val bucketStr = dayBucket.toString
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
    val table = connection.getTable(TableName.valueOf(HBaseTableManager.volumeBrickTableName(dataset)))
    val cf = HBaseTableManager.CF_BYTES
    val result = mutable.ArrayBuffer[String]()
    var timeFilteredOut = 0
    var spatialFilteredOut = 0
    var vxFilteredOut = 0
    var vyFilteredOut = 0
    var vzFilteredOut = 0

    try {
      candidates.grouped(getBatchSize).foreach { batch =>
        val gets = batch.map { rowKey =>
          new Get(Bytes.toBytes(rowKey))
            .addColumn(cf, Bytes.toBytes("time_millis"))
            .addColumn(cf, Bytes.toBytes("x_min"))
            .addColumn(cf, Bytes.toBytes("x_max"))
            .addColumn(cf, Bytes.toBytes("y_min"))
            .addColumn(cf, Bytes.toBytes("y_max"))
            .addColumn(cf, Bytes.toBytes("z_min"))
            .addColumn(cf, Bytes.toBytes("z_max"))
            .addColumn(cf, Bytes.toBytes("vx"))
            .addColumn(cf, Bytes.toBytes("vy"))
            .addColumn(cf, Bytes.toBytes("vz"))
            .addColumn(cf, Bytes.toBytes("raw_line"))
        }.toList

        val results = table.get(gets.asJava).toSeq

        results.foreach { r =>
          if (r != null && !r.isEmpty) {
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
              result += rawLine
            }
          }
        }
      }
    } finally {
      table.close()
    }

    println(s"[Step6] Refined Filter Statistics:")
    println(s"  totalCandidates: ${candidates.size}")
    println(s"  finalCount: ${result.size}")
    println(s"  timeFilteredOut: $timeFilteredOut")
    println(s"  spatialFilteredOut: $spatialFilteredOut")
    println(s"  vxFilteredOut: $vxFilteredOut")
    println(s"  vyFilteredOut: $vyFilteredOut")
    println(s"  vzFilteredOut: $vzFilteredOut")

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

  def getHeaderLine: String = metaHeaderLine
}
