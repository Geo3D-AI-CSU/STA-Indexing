// src/main/scala/model/SensorRecord.scala
package model

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class SensorRecord(
  rowKey: String,
  time: Long,
  sensorId: String,
  longitude: Double,
  latitude: Double,
  altitude: Double,
  sensorType: String
)

object SensorRecord {
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
  
  def fromCsvLine(line: String): Option[SensorRecord] = {
    try {
      val parts = line.split(",").map(_.trim)
      if (parts.length >= 6) {
        val time = LocalDateTime.parse(parts(0), formatter)
        val timestamp = time.atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli
        
        Some(SensorRecord(
          rowKey = java.util.UUID.randomUUID().toString.replace("-", ""),
          time = timestamp,
          sensorId = parts(1),
          longitude = parts(2).toDouble,
          latitude = parts(3).toDouble,
          altitude = parts(4).toDouble,
          sensorType = parts(5)
        ))
      } else None
    } catch {
      case _: Exception => None
    }
  }
}