// src/main/scala/index/VolumeUnifiedIndexKey.scala
package index

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * RowKey encoding utility for Volume unified index
 *
 * RowKey structure (fixed prefix length):
 * [attrKind(1B)] + [attrHash(4B, BE)] + [zCell(8B, BE)] + [dayBucket(4B, BE)] + [timeOfDay(4B, BE)] + '_' + [blockKey(UTF-8)]
 *
 * Where:
 * - attrKind: Fixed as ByModelType (1 byte)
 * - attrHash: Stable hash of model_type (4 bytes, big-endian)
 * - zCell: Morton-encoded spatial cell (8 bytes, big-endian)
 * - dayBucket: UTC date (yyyyMMdd format, 4 bytes, big-endian, e.g., 20251105)
 * - timeOfDay: Milliseconds within the day (4 bytes, big-endian)
 * - blockKey: Brick table rowkey suffix, format: {model_id}|{timeMillis}|{tile_i}|{tile_j}|{tile_k}
 *
 * Note: dayBucket uses yyyyMMdd format Int, isolated from the epochDay used in the legacy point data system
 */
object VolumeUnifiedIndexKey {
  
  // Attribute type constant
  val ATTR_KIND_BY_MODEL_TYPE: Byte = 0x01.toByte
  
  /**
   * FNV-1a 32-bit hash algorithm (stable hash)
   *
   * @param str Input string
   * @return 32-bit hash value
   */
  def fnv1a32(str: String): Int = {
    val FNV_PRIME = 16777619
    val FNV_OFFSET_BASIS = 2166136261L
    
    var hash = FNV_OFFSET_BASIS
    for (c <- str) {
      hash ^= c.toLong
      hash *= FNV_PRIME
    }
    (hash & 0xFFFFFFFFL).toInt
  }
  
  /**
   * Encodes an Int to 4-byte big-endian
   */
  def intToBytesBE(value: Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(4)
    buf.putInt(value)
    buf.array()
  }
  
  /**
   * Encodes a Long to 8-byte big-endian
   */
  def longToBytesBE(value: Long): Array[Byte] = {
    val buf = ByteBuffer.allocate(8)
    buf.putLong(value)
    buf.array()
  }
  
  /**
   * Builds a RowKey for the volume unified index
   *
   * @param modelType Model type (string)
   * @param zCell Morton-encoded spatial cell
   * @param dayBucket UTC date (yyyyMMdd format, e.g., 20251105)
   * @param timeOfDay Milliseconds within the day
   * @param blockKey Brick table rowkey suffix, format: {model_id}|{timeMillis}|{tile_i}|{tile_j}|{tile_k}
   * @return RowKey byte array
   */
  def toRowKeyBytes(modelType: String, zCell: Long, dayBucket: Int, timeOfDay: Int, blockKey: String): Array[Byte] = {
    // Calculate attrHash
    val attrHash = fnv1a32(modelType)
    
    // Build prefix part (fixed length: 1+4+8+4+4 = 21 bytes)
    val prefix = ByteBuffer.allocate(21)
    prefix.put(ATTR_KIND_BY_MODEL_TYPE)
    prefix.putInt(attrHash)
    prefix.putLong(zCell)
    prefix.putInt(dayBucket)
    prefix.putInt(timeOfDay)
    
    // Add separator and blockKey
    val blockKeyBytes = blockKey.getBytes(StandardCharsets.UTF_8)
    val rowKey = ByteBuffer.allocate(21 + 1 + blockKeyBytes.length)
    rowKey.put(prefix.array())
    rowKey.put('_'.toByte)
    rowKey.put(blockKeyBytes)
    
    rowKey.array()
  }
  
  /**
   * Builds a RowKey for the volume unified index (from brick info)
   *
   * @param modelType Model type
   * @param zCell Morton-encoded spatial cell
   * @param dayBucket UTC date (yyyyMMdd format, e.g., 20251105)
   * @param timeOfDay Milliseconds within the day
   * @param modelId Model ID
   * @param timeMillis Timestamp (milliseconds)
   * @param tileI Brick i index
   * @param tileJ Brick j index
   * @param tileK Brick k index
   * @return RowKey byte array
   */
  def toRowKeyBytes(modelType: String, zCell: Long, dayBucket: Int, timeOfDay: Int,
                    modelId: String, timeMillis: Long, tileI: Int, tileJ: Int, tileK: Int): Array[Byte] = {
    val blockKey = s"${modelId}|${timeMillis}|${tileI}|${tileJ}|${tileK}"
    toRowKeyBytes(modelType, zCell, dayBucket, timeOfDay, blockKey)
  }
  
  /**
   * Builds a scan range (for queries)
   *
   * Fixed zCell + timeOfDay range within a day
   *
   * @param modelType Model type
   * @param zCell Morton-encoded spatial cell
   * @param dayBucket UTC date (yyyyMMdd format, e.g., 20251105)
   * @param startTimeOfDay Start milliseconds within the day (usually 0)
   * @param endTimeOfDay End milliseconds within the day (usually 86399999)
   * @return (startRowKey, endRowKey) tuple
   */
  def buildScanRangeForCell(modelType: String, zCell: Long, dayBucket: Int,
                             startTimeOfDay: Int, endTimeOfDay: Int): (Array[Byte], Array[Byte]) = {
    // Calculate attrHash
    val attrHash = fnv1a32(modelType)
    
    // Build startRowKey (using startTimeOfDay)
    val startPrefix = ByteBuffer.allocate(21)
    startPrefix.put(ATTR_KIND_BY_MODEL_TYPE)
    startPrefix.putInt(attrHash)
    startPrefix.putLong(zCell)
    startPrefix.putInt(dayBucket)
    startPrefix.putInt(startTimeOfDay)
    
    // Build endRowKey (using endTimeOfDay, blockKey part set to maximum value)
    val endPrefix = ByteBuffer.allocate(21)
    endPrefix.put(ATTR_KIND_BY_MODEL_TYPE)
    endPrefix.putInt(attrHash)
    endPrefix.putLong(zCell)
    endPrefix.putInt(dayBucket)
    endPrefix.putInt(endTimeOfDay)
    
    // startRowKey: prefix + '_' + minimum blockKey
    val startRowKey = ByteBuffer.allocate(22)
    startRowKey.put(startPrefix.array())
    startRowKey.put('_'.toByte)
    
    // endRowKey: prefix + '_' + maximum blockKey (using UTF-8 max character)
    val maxBlockKey = "\uFFFF".getBytes(StandardCharsets.UTF_8)
    val endRowKey = ByteBuffer.allocate(22 + maxBlockKey.length)
    endRowKey.put(endPrefix.array())
    endRowKey.put('_'.toByte)
    endRowKey.put(maxBlockKey)
    
    (startRowKey.array(), endRowKey.array())
  }
  
  /**
   * Builds a scan range (query by day, covering entire day's timeOfDay)
   *
   * @param modelType Model type
   * @param zCell Morton-encoded spatial cell
   * @param dayBucket UTC date (yyyyMMdd format, e.g., 20251105)
   * @return (startRowKey, endRowKey) tuple
   */
  def buildScanRangeForDay(modelType: String, zCell: Long, dayBucket: Int): (Array[Byte], Array[Byte]) = {
    // Full day range: 0 ~ 86399999 milliseconds
    buildScanRangeForCell(modelType, zCell, dayBucket, 0, 86399999)
  }
  
  /**
   * Parses blockKey (extracted from RowKey)
   *
   * @param rowKey RowKey byte array
   * @return (modelId, timeMillis, tileI, tileJ, tileK) tuple
   */
  def parseBlockKey(rowKey: Array[Byte]): (String, Long, Int, Int, Int) = {
    // Find the position of separator '_'
    val separatorIndex = rowKey.indexOf('_'.toByte)
    if (separatorIndex == -1) {
      throw new IllegalArgumentException("Invalid rowKey: separator '_' not found")
    }
    
    // Extract blockKey part (after the separator)
    val blockKeyBytes = rowKey.slice(separatorIndex + 1, rowKey.length)
    val blockKey = new String(blockKeyBytes, StandardCharsets.UTF_8)
    
    // Parse blockKey: {model_id}|{timeMillis}|{tile_i}|{tile_j}|{tile_k}
    val parts = blockKey.split("\\|")
    if (parts.length != 5) {
      throw new IllegalArgumentException(s"Invalid blockKey format: $blockKey")
    }
    
    val modelId = parts(0)
    val timeMillis = parts(1).toLong
    val tileI = parts(2).toInt
    val tileJ = parts(3).toInt
    val tileK = parts(4).toInt
    
    (modelId, timeMillis, tileI, tileJ, tileK)
  }
  
  /**
   * Extracts blockKey string from RowKey
   *
   * @param rowKey RowKey byte array
   * @return blockKey string
   */
  def extractBlockKey(rowKey: Array[Byte]): String = {
    val separatorIndex = rowKey.indexOf('_'.toByte)
    if (separatorIndex == -1) {
      throw new IllegalArgumentException("Invalid rowKey: separator '_' not found")
    }
    
    val blockKeyBytes = rowKey.slice(separatorIndex + 1, rowKey.length)
    new String(blockKeyBytes, StandardCharsets.UTF_8)
  }
}
