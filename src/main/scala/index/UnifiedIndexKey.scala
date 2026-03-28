// src/main/scala/index/UnifiedIndexKey.scala
package index

import java.nio.ByteBuffer

/**
 * Unified index: uses a single row key for sensor + time + spatial conditions
 * Row key layout: [attrKind(1B)] + [attrHash(4B BE)] + [Z3D(8B BE)] + [dayBucket(4B BE)] + [timeOfDay(4B BE)] + "_" + [dataRowKey(UTF-8)]
 */
object UnifiedIndexKey {

  // ========== Attribute Types ==========
  sealed trait AttrKind
  case object BySensorId extends AttrKind
  case object ByTempBucket extends AttrKind
  case object ByVxBucket extends AttrKind
  case object ByVyBucket extends AttrKind
  case object ByVzBucket extends AttrKind

  // Attribute type -> byte mapping
  private def attrKindByte(kind: AttrKind): Byte = kind match {
    case BySensorId => 0x01
    case ByTempBucket => 0x02
    case ByVxBucket => 0x03
    case ByVyBucket => 0x04
    case ByVzBucket => 0x05
  }

  // ========== Data Structures ==========
  case class Parts(
    attrKind: AttrKind,
    attrHash: Int,
    z3d: Long,
    dayBucket: Int,
    timeOfDay: Int,
    dataRowKey: String
  )

  // ========== Encoding Methods ==========

  /**
   * Hashes an attribute value (e.g., sensor ID)
   * Uses a stable hash to ensure consistency across JVM instances
   * Scala/Java's hashCode() may differ across JVM processes, so we use a murmur3-style stable hash
   */
  def hashAttr(attr: String): Int = {
    // Stable hash implementation: simple FNV-1a hash algorithm
    var hash = 2166136261L
    var i = 0
    while (i < attr.length) {
      hash ^= attr(i).toLong
      hash = (hash * 16777619L) & 0xFFFFFFFFL
      i += 1
    }
    (hash & 0x7FFFFFFFL).toInt  // Convert to positive integer
  }

  /**
   * Builds a Parts structure for a sensor ID (with specified level)
   * Key improvement: uses block ID instead of point ID
   * This ensures all points within the same block get the same block ID, allowing import and query to match
   */
  def buildPartsForSensorId(
    sensorId: String,
    ts: Long,
    lon: Double,
    lat: Double,
    alt: Double,
    dataRowKey: String,
    unifiedLevel: Int = Z3DEncoder.UNIFIED_BLOCK_LEVEL
  ): Parts = {
    val attrHash = hashAttr(sensorId)
    // Use block ID instead of point ID, calculated at the specified level, ensuring points in the same block can be scanned together
    val z3dBlock = Z3DEncoder.blockIdForUnifiedIndexAtLevel(unifiedLevel, lon, lat, alt)
    val dayBucket = TimeBucket.dayBucket(ts)
    val timeOfDay = TimeBucket.millisOfDay(ts)

    Parts(
      attrKind = BySensorId,
      attrHash = attrHash,
      z3d = z3dBlock,
      dayBucket = dayBucket,
      timeOfDay = timeOfDay,
      dataRowKey = dataRowKey
    )
  }

  /**
   * Encodes Parts into an HBase row key byte array
   * Layout: [attrKind(1B)] + [attrHash(4B BE)] + [Z3D(8B BE)] + [dayBucket(4B BE)] + [timeOfDay(4B BE)] + "_" + [dataRowKey(UTF-8)]
   */
  def toRowKeyBytes(p: Parts): Array[Byte] = {
    val kindByte = attrKindByte(p.attrKind)
    val dataRowKeyBytes = p.dataRowKey.getBytes("UTF-8")

    // Allocate buffer: 1(type) + 4(hash) + 8(z3d) + 4(bucket) + 4(timeOfDay) + 1(_) + dataRowKey
    val totalSize = 1 + 4 + 8 + 4 + 4 + 1 + dataRowKeyBytes.length
    val buffer = ByteBuffer.allocate(totalSize)

    // Write all parts in big-endian byte order
    buffer.put(kindByte)
    buffer.putInt(p.attrHash)
    buffer.putLong(p.z3d)
    buffer.putInt(p.dayBucket)
    buffer.putInt(p.timeOfDay)
    buffer.put('_'.toByte)
    buffer.put(dataRowKeyBytes)

    buffer.array()
  }

  /**
   * Builds a scan range for unified index queries
   * Returns (startRowKeyBytes, stopRowKeyBytes) for use with Scan.withStartRow / withStopRow
   */
  def buildScanRange(
    sensorId: String,
    z3dMin: Long,
    z3dMax: Long,
    dayBucket: Int,
    timeOfDayMin: Int,
    timeOfDayMax: Int
  ): (Array[Byte], Array[Byte]) = {
    val kindByte = attrKindByte(BySensorId)
    val attrHash = hashAttr(sensorId)

    // Start row key: type + hash + z3dMin + dayBucket + timeOfDayMin
    val startBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4)
    startBuffer.put(kindByte)
    startBuffer.putInt(attrHash)
    startBuffer.putLong(z3dMin)
    startBuffer.putInt(dayBucket)
    startBuffer.putInt(timeOfDayMin)
    val startRowBytes = startBuffer.array()

    // Stop row key: type + hash + z3dMax + dayBucket + timeOfDayMax + 0xFF (includes all subsequent bytes)
    val stopBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4 + 1)
    stopBuffer.put(kindByte)
    stopBuffer.putInt(attrHash)
    stopBuffer.putLong(z3dMax)
    stopBuffer.putInt(dayBucket)
    stopBuffer.putInt(timeOfDayMax)
    stopBuffer.put(0xFF.toByte)
    val stopRowBytes = stopBuffer.array()

    (startRowBytes, stopRowBytes)
  }

  /**
   * Builds a Scan for a single Z3D grid block + time range
   *
   * This method is used for multi-level grid block indexing strategies, constructing a precise Scan
   * for each fixed grid block ID (zCell) and time range.
   * Unlike buildScanRange, the Z value here is a fixed single block ID rather than a range,
   * allowing time constraints to take effect during the initial filtering phase.
   *
   * @param sensorId Sensor ID
   * @param zCell Single Z3D grid block ID (encoded value representing the block)
   * @param dayBucket Day bucket (yyyyMMdd format)
   * @param timeOfDayMin Start milliseconds within the day
   * @param timeOfDayMax End milliseconds within the day
   * @return (startRowKeyBytes, stopRowKeyBytes) for HBase Scan
   */
  def buildScanRangeForCell(
    sensorId: String,
    zCell: Long,
    dayBucket: Int,
    timeOfDayMin: Int,
    timeOfDayMax: Int
  ): (Array[Byte], Array[Byte]) = {
    val kindByte = attrKindByte(BySensorId)
    val attrHash = hashAttr(sensorId)

    // Start row key: type + hash + zCell + dayBucket + timeOfDayMin
    val startBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4)
    startBuffer.put(kindByte)
    startBuffer.putInt(attrHash)
    startBuffer.putLong(zCell)
    startBuffer.putInt(dayBucket)
    startBuffer.putInt(timeOfDayMin)
    val startRowBytes = startBuffer.array()

    // Stop row key: type + hash + zCell + dayBucket + timeOfDayMax + 0xFF
    // This ensures that for this fixed zCell, only the time range timeOfDayMin..timeOfDayMax within the dayBucket is scanned
    val stopBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4 + 1)
    stopBuffer.put(kindByte)
    stopBuffer.putInt(attrHash)
    stopBuffer.putLong(zCell)
    stopBuffer.putInt(dayBucket)
    stopBuffer.putInt(timeOfDayMax)
    stopBuffer.put(0xFF.toByte)
    val stopRowBytes = stopBuffer.array()

    (startRowBytes, stopRowBytes)
  }

  /**
   * Builds a Scan for a single Z3D grid block + multi-day continuous time range
   *
   * This method optimizes long time-span queries by merging scans across multiple dayBuckets
   * into a single continuous time interval Scan.
   * The row key structure remains unchanged: type + hash + zCell + dayBucket + timeOfDay + "_" + dataRowKey
   * but now a single Scan covers all rows from [startDayBucket, startTimeOfDay] to [endDayBucket, endTimeOfDay].
   *
   * @param sensorId Sensor ID
   * @param zCell Single Z3D grid block ID (encoded value representing the block)
   * @param startDayBucket Start day bucket (yyyyMMdd format)
   * @param startTimeOfDay Milliseconds within the start day
   * @param endDayBucket End day bucket (yyyyMMdd format)
   * @param endTimeOfDay Milliseconds within the end day
   * @return (startRowKeyBytes, stopRowKeyBytes) for HBase Scan
   */
  def buildScanRangeForCellTimeSpan(
    sensorId: String,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  ): (Array[Byte], Array[Byte]) = {
    val kindByte = attrKindByte(BySensorId)
    val attrHash = hashAttr(sensorId)

    // Start row key: type + hash + zCell + startDayBucket + startTimeOfDay
    val startBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4)
    startBuffer.put(kindByte)
    startBuffer.putInt(attrHash)
    startBuffer.putLong(zCell)
    startBuffer.putInt(startDayBucket)
    startBuffer.putInt(startTimeOfDay)
    val startRowBytes = startBuffer.array()

    // Stop row key: type + hash + zCell + endDayBucket + endTimeOfDay + 0xFF
    // The 0xFF byte ensures the scan includes any data keys (dataRowKey) after that time point
    val stopBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4 + 1)
    stopBuffer.put(kindByte)
    stopBuffer.putInt(attrHash)
    stopBuffer.putLong(zCell)
    stopBuffer.putInt(endDayBucket)
    stopBuffer.putInt(endTimeOfDay)
    stopBuffer.put(0xFF.toByte)
    val stopRowBytes = stopBuffer.array()

    (startRowBytes, stopRowBytes)
  }

  def buildPartsForTempBucket(
    tempBucket: Int,
    ts: Long,
    lon: Double,
    lat: Double,
    alt: Double,
    dataRowKey: String,
    unifiedLevel: Int = Z3DEncoder.UNIFIED_BLOCK_LEVEL
  ): Parts = {
    val attrHash = tempBucket
    val z3dBlock = Z3DEncoder.blockIdForUnifiedIndexAtLevel(unifiedLevel, lon, lat, alt)
    val dayBucket = TimeBucket.dayBucket(ts)
    val timeOfDay = TimeBucket.millisOfDay(ts)

    Parts(
      attrKind = ByTempBucket,
      attrHash = attrHash,
      z3d = z3dBlock,
      dayBucket = dayBucket,
      timeOfDay = timeOfDay,
      dataRowKey = dataRowKey
    )
  }

  def buildScanRangeForTempCellTimeSpan(
    tempBucket: Int,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  ): (Array[Byte], Array[Byte]) = {
    val kindByte = attrKindByte(ByTempBucket)
    val attrHash = tempBucket

    val startBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4)
    startBuffer.put(kindByte)
    startBuffer.putInt(attrHash)
    startBuffer.putLong(zCell)
    startBuffer.putInt(startDayBucket)
    startBuffer.putInt(startTimeOfDay)
    val startRowBytes = startBuffer.array()

    val stopBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4 + 1)
    stopBuffer.put(kindByte)
    stopBuffer.putInt(attrHash)
    stopBuffer.putLong(zCell)
    stopBuffer.putInt(endDayBucket)
    stopBuffer.putInt(endTimeOfDay)
    stopBuffer.put(0xFF.toByte)
    val stopRowBytes = stopBuffer.array()

    (startRowBytes, stopRowBytes)
  }

  def buildPartsForVxBucket(
    vxBucketEncoded: Int,
    ts: Long,
    lon: Double,
    lat: Double,
    alt: Double,
    dataRowKey: String,
    unifiedLevel: Int = Z3DEncoder.UNIFIED_BLOCK_LEVEL
  ): Parts = {
    val attrHash = vxBucketEncoded
    val z3dBlock = Z3DEncoder.blockIdForUnifiedIndexAtLevel(unifiedLevel, lon, lat, alt)
    val dayBucket = TimeBucket.dayBucket(ts)
    val timeOfDay = TimeBucket.millisOfDay(ts)

    Parts(
      attrKind = ByVxBucket,
      attrHash = attrHash,
      z3d = z3dBlock,
      dayBucket = dayBucket,
      timeOfDay = timeOfDay,
      dataRowKey = dataRowKey
    )
  }

  def buildPartsForVyBucket(
    vyBucketEncoded: Int,
    ts: Long,
    lon: Double,
    lat: Double,
    alt: Double,
    dataRowKey: String,
    unifiedLevel: Int = Z3DEncoder.UNIFIED_BLOCK_LEVEL
  ): Parts = {
    val attrHash = vyBucketEncoded
    val z3dBlock = Z3DEncoder.blockIdForUnifiedIndexAtLevel(unifiedLevel, lon, lat, alt)
    val dayBucket = TimeBucket.dayBucket(ts)
    val timeOfDay = TimeBucket.millisOfDay(ts)

    Parts(
      attrKind = ByVyBucket,
      attrHash = attrHash,
      z3d = z3dBlock,
      dayBucket = dayBucket,
      timeOfDay = timeOfDay,
      dataRowKey = dataRowKey
    )
  }

  def buildPartsForVzBucket(
    vzBucketEncoded: Int,
    ts: Long,
    lon: Double,
    lat: Double,
    alt: Double,
    dataRowKey: String,
    unifiedLevel: Int = Z3DEncoder.UNIFIED_BLOCK_LEVEL
  ): Parts = {
    val attrHash = vzBucketEncoded
    val z3dBlock = Z3DEncoder.blockIdForUnifiedIndexAtLevel(unifiedLevel, lon, lat, alt)
    val dayBucket = TimeBucket.dayBucket(ts)
    val timeOfDay = TimeBucket.millisOfDay(ts)

    Parts(
      attrKind = ByVzBucket,
      attrHash = attrHash,
      z3d = z3dBlock,
      dayBucket = dayBucket,
      timeOfDay = timeOfDay,
      dataRowKey = dataRowKey
    )
  }

  def buildScanRangeForVxCellTimeSpan(
    vxBucketEncoded: Int,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  ): (Array[Byte], Array[Byte]) = {
    val kindByte = attrKindByte(ByVxBucket)
    val attrHash = vxBucketEncoded

    val startBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4)
    startBuffer.put(kindByte)
    startBuffer.putInt(attrHash)
    startBuffer.putLong(zCell)
    startBuffer.putInt(startDayBucket)
    startBuffer.putInt(startTimeOfDay)
    val startRowBytes = startBuffer.array()

    val stopBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4 + 1)
    stopBuffer.put(kindByte)
    stopBuffer.putInt(attrHash)
    stopBuffer.putLong(zCell)
    stopBuffer.putInt(endDayBucket)
    stopBuffer.putInt(endTimeOfDay)
    stopBuffer.put(0xFF.toByte)
    val stopRowBytes = stopBuffer.array()

    (startRowBytes, stopRowBytes)
  }

  def buildScanRangeForVyCellTimeSpan(
    vyBucketEncoded: Int,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  ): (Array[Byte], Array[Byte]) = {
    val kindByte = attrKindByte(ByVyBucket)
    val attrHash = vyBucketEncoded

    val startBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4)
    startBuffer.put(kindByte)
    startBuffer.putInt(attrHash)
    startBuffer.putLong(zCell)
    startBuffer.putInt(startDayBucket)
    startBuffer.putInt(startTimeOfDay)
    val startRowBytes = startBuffer.array()

    val stopBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4 + 1)
    stopBuffer.put(kindByte)
    stopBuffer.putInt(attrHash)
    stopBuffer.putLong(zCell)
    stopBuffer.putInt(endDayBucket)
    stopBuffer.putInt(endTimeOfDay)
    stopBuffer.put(0xFF.toByte)
    val stopRowBytes = stopBuffer.array()

    (startRowBytes, stopRowBytes)
  }

  def buildScanRangeForVzCellTimeSpan(
    vzBucketEncoded: Int,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  ): (Array[Byte], Array[Byte]) = {
    val kindByte = attrKindByte(ByVzBucket)
    val attrHash = vzBucketEncoded

    val startBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4)
    startBuffer.put(kindByte)
    startBuffer.putInt(attrHash)
    startBuffer.putLong(zCell)
    startBuffer.putInt(startDayBucket)
    startBuffer.putInt(startTimeOfDay)
    val startRowBytes = startBuffer.array()

    val stopBuffer = ByteBuffer.allocate(1 + 4 + 8 + 4 + 4 + 1)
    stopBuffer.put(kindByte)
    stopBuffer.putInt(attrHash)
    stopBuffer.putLong(zCell)
    stopBuffer.putInt(endDayBucket)
    stopBuffer.putInt(endTimeOfDay)
    stopBuffer.put(0xFF.toByte)
    val stopRowBytes = stopBuffer.array()

    (startRowBytes, stopRowBytes)
  }
}

