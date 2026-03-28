// src/main/scala/index/GeoSimVolumeUnifiedScanKey.scala
package index

import java.nio.ByteBuffer

object GeoSimVolumeUnifiedScanKey {

  def buildScanRangeForCellTimeSpan(simId: String, zCell: Long, startDayBucket: Int, startTimeOfDay: Int, endDayBucket: Int, endTimeOfDay: Int): (Array[Byte], Array[Byte]) = {
    val attrHash = VolumeUnifiedIndexKey.fnv1a32(simId)
    val attrKind = VolumeUnifiedIndexKey.ATTR_KIND_BY_MODEL_TYPE
    
    val startBuffer = ByteBuffer.allocate(21)
    startBuffer.put(attrKind)
    startBuffer.putInt(attrHash)
    startBuffer.putLong(zCell)
    startBuffer.putInt(startDayBucket)
    startBuffer.putInt(startTimeOfDay)
    val startRowBytes = startBuffer.array()
    
    val stopBuffer = ByteBuffer.allocate(22)
    stopBuffer.put(attrKind)
    stopBuffer.putInt(attrHash)
    stopBuffer.putLong(zCell)
    stopBuffer.putInt(endDayBucket)
    stopBuffer.putInt(endTimeOfDay)
    stopBuffer.put(0xFF.toByte)
    val stopRowBytes = stopBuffer.array()
    
    (startRowBytes, stopRowBytes)
  }
}
