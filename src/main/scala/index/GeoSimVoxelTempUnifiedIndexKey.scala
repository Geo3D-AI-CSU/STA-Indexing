// src/main/scala/index/GeoSimVoxelTempUnifiedIndexKey.scala
package index

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * RowKey encoding utility for GeoSim Voxel temperature unified index
 *
 * RowKey structure (fixed prefix length of 21 bytes):
 * [kind(1B)=0x02] + [tempBucket(4B, BE)] + [zCell(8B, BE)] + [dayBucket(4B, BE yyyyMMdd)] + [timeOfDay(4B, BE)] + '_' + [brickRowKey UTF-8]
 *
 * Where:
 * - kind: Fixed as 0x02 (1 byte)
 * - tempBucket: Temperature bucket ID (4 bytes, big-endian)
 * - zCell: Morton-encoded spatial cell (8 bytes, big-endian)
 * - dayBucket: UTC date (yyyyMMdd format, 4 bytes, big-endian, e.g., 20251105)
 * - timeOfDay: Milliseconds within the day (4 bytes, big-endian)
 * - brickRowKey: Brick table rowkey suffix, UTF-8 string
 */
object GeoSimVoxelTempUnifiedIndexKey {

  val ATTR_KIND_TEMP_BUCKET: Byte = 0x02.toByte

  private val PREFIX_LENGTH = 21

  /**
   * Builds a RowKey for GeoSim Voxel temperature unified index
   *
   * @param tempBucket Temperature bucket ID
   * @param zCell Morton-encoded spatial cell
   * @param dayBucket UTC date (yyyyMMdd format, e.g., 20251105)
   * @param timeOfDay Milliseconds within the day
   * @param brickRowKey Brick table rowkey suffix
   * @return RowKey byte array
   */
  def toRowKeyBytes(tempBucket: Int, zCell: Long, dayBucket: Int, timeOfDay: Int, brickRowKey: String): Array[Byte] = {
    val brickRowKeyBytes = brickRowKey.getBytes(StandardCharsets.UTF_8)

    val prefix = ByteBuffer.allocate(PREFIX_LENGTH)
    prefix.put(ATTR_KIND_TEMP_BUCKET)
    prefix.putInt(tempBucket)
    prefix.putLong(zCell)
    prefix.putInt(dayBucket)
    prefix.putInt(timeOfDay)

    val rowKey = ByteBuffer.allocate(PREFIX_LENGTH + 1 + brickRowKeyBytes.length)
    rowKey.put(prefix.array())
    rowKey.put('_'.toByte)
    rowKey.put(brickRowKeyBytes)

    rowKey.array()
  }

  /**
   * Builds a time-span scan range (one scan per tempBucket×zCell)
   *
   * @param tempBucket Temperature bucket ID
   * @param zCell Morton-encoded spatial cell
   * @param startDayBucket Start day bucket (yyyyMMdd format)
   * @param startTimeOfDay Milliseconds within the start day
   * @param endDayBucket End day bucket (yyyyMMdd format)
   * @param endTimeOfDay Milliseconds within the end day
   * @return (startRowKey, endRowKey) tuple
   */
  def buildScanRangeForTempCellTimeSpan(
    tempBucket: Int,
    zCell: Long,
    startDayBucket: Int,
    startTimeOfDay: Int,
    endDayBucket: Int,
    endTimeOfDay: Int
  ): (Array[Byte], Array[Byte]) = {
    val startPrefix = ByteBuffer.allocate(PREFIX_LENGTH)
    startPrefix.put(ATTR_KIND_TEMP_BUCKET)
    startPrefix.putInt(tempBucket)
    startPrefix.putLong(zCell)
    startPrefix.putInt(startDayBucket)
    startPrefix.putInt(startTimeOfDay)

    val stopPrefix = ByteBuffer.allocate(PREFIX_LENGTH + 1)
    stopPrefix.put(ATTR_KIND_TEMP_BUCKET)
    stopPrefix.putInt(tempBucket)
    stopPrefix.putLong(zCell)
    stopPrefix.putInt(endDayBucket)
    stopPrefix.putInt(endTimeOfDay)
    stopPrefix.put(0xFF.toByte)

    (startPrefix.array(), stopPrefix.array())
  }

  /**
   * Extracts brickRowKey from RowKey
   *
   * @param rowKey RowKey byte array
   * @return brickRowKey string
   */
  def extractBrickRowKey(rowKey: Array[Byte]): String = {
    val separatorIndex = rowKey.indexOf('_'.toByte)
    if (separatorIndex == -1) {
      throw new IllegalArgumentException("Invalid rowKey: separator '_' not found")
    }

    val brickRowKeyBytes = rowKey.slice(separatorIndex + 1, rowKey.length)
    new String(brickRowKeyBytes, StandardCharsets.UTF_8)
  }
}
