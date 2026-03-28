package index

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object GeoSimVoxelVelocityUnifiedIndexKey {

  val ATTR_KIND_VX: Byte = 0x03.toByte
  val ATTR_KIND_VY: Byte = 0x04.toByte
  val ATTR_KIND_VZ: Byte = 0x05.toByte

  private val PREFIX_LENGTH = 21

  def toRowKeyBytes(kind: Byte, bucketEnc: Int, zCell: Long, dayBucket: Int, timeOfDay: Int, brickRowKey: String): Array[Byte] = {
    val brickRowKeyBytes = brickRowKey.getBytes(StandardCharsets.UTF_8)

    val prefix = ByteBuffer.allocate(PREFIX_LENGTH)
    prefix.put(kind)
    prefix.putInt(bucketEnc)
    prefix.putLong(zCell)
    prefix.putInt(dayBucket)
    prefix.putInt(timeOfDay)

    val rowKey = ByteBuffer.allocate(PREFIX_LENGTH + 1 + brickRowKeyBytes.length)
    rowKey.put(prefix.array())
    rowKey.put('_'.toByte)
    rowKey.put(brickRowKeyBytes)

    rowKey.array()
  }

  def buildScanRangeForCellTimeSpan(kind: Byte, bucketEnc: Int, zCell: Long, startDayBucket: Int, startTimeOfDay: Int, endDayBucket: Int, endTimeOfDay: Int): (Array[Byte], Array[Byte]) = {
    val startPrefix = ByteBuffer.allocate(PREFIX_LENGTH)
    startPrefix.put(kind)
    startPrefix.putInt(bucketEnc)
    startPrefix.putLong(zCell)
    startPrefix.putInt(startDayBucket)
    startPrefix.putInt(startTimeOfDay)

    val stopPrefix = ByteBuffer.allocate(PREFIX_LENGTH + 1)
    stopPrefix.put(kind)
    stopPrefix.putInt(bucketEnc)
    stopPrefix.putLong(zCell)
    stopPrefix.putInt(endDayBucket)
    stopPrefix.putInt(endTimeOfDay)
    stopPrefix.put(0xFF.toByte)

    (startPrefix.array(), stopPrefix.array())
  }

  def vxRowKeyBytes(bucketEnc: Int, zCell: Long, dayBucket: Int, timeOfDay: Int, brickRowKey: String): Array[Byte] = {
    toRowKeyBytes(ATTR_KIND_VX, bucketEnc, zCell, dayBucket, timeOfDay, brickRowKey)
  }

  def vyRowKeyBytes(bucketEnc: Int, zCell: Long, dayBucket: Int, timeOfDay: Int, brickRowKey: String): Array[Byte] = {
    toRowKeyBytes(ATTR_KIND_VY, bucketEnc, zCell, dayBucket, timeOfDay, brickRowKey)
  }

  def vzRowKeyBytes(bucketEnc: Int, zCell: Long, dayBucket: Int, timeOfDay: Int, brickRowKey: String): Array[Byte] = {
    toRowKeyBytes(ATTR_KIND_VZ, bucketEnc, zCell, dayBucket, timeOfDay, brickRowKey)
  }

  def vxScanRangeTimeSpan(bucketEnc: Int, zCell: Long, startDayBucket: Int, startTimeOfDay: Int, endDayBucket: Int, endTimeOfDay: Int): (Array[Byte], Array[Byte]) = {
    buildScanRangeForCellTimeSpan(ATTR_KIND_VX, bucketEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
  }

  def vyScanRangeTimeSpan(bucketEnc: Int, zCell: Long, startDayBucket: Int, startTimeOfDay: Int, endDayBucket: Int, endTimeOfDay: Int): (Array[Byte], Array[Byte]) = {
    buildScanRangeForCellTimeSpan(ATTR_KIND_VY, bucketEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
  }

  def vzScanRangeTimeSpan(bucketEnc: Int, zCell: Long, startDayBucket: Int, startTimeOfDay: Int, endDayBucket: Int, endTimeOfDay: Int): (Array[Byte], Array[Byte]) = {
    buildScanRangeForCellTimeSpan(ATTR_KIND_VZ, bucketEnc, zCell, startDayBucket, startTimeOfDay, endDayBucket, endTimeOfDay)
  }
}
