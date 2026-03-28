// src/main/scala/model/GeoSimPointLine.scala
package model

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

case class GeoSimPointLine(
  simId: String,
  timeIso: String,
  timeMillis: Long,
  x: Double,
  y: Double,
  z: Double,
  t: Double,
  vx: Double,
  vy: Double,
  vz: Double,
  rawLine: String
)

object GeoSimPointLine {
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

  def normalizeSimId(s: String): String = {
    val trimmed = s.trim
    val pattern = "^\\d+\\.0+$".r
    if (pattern.findFirstIn(trimmed).isDefined) {
      trimmed.split("\\.")(0)
    } else {
      trimmed
    }
  }

  def fromLine(line: String, delimiter: Char): Option[GeoSimPointLine] = {
    try {
      val parts = line.split(delimiter).map(_.trim)
      
      if (parts.length < 10) {
        return None
      }
      
      if (parts(0) == "sim_id") {
        return None
      }
      
      val simIdRaw = parts(0)
      val simId = normalizeSimId(simIdRaw)
      val timeIso = parts(1)
      val x = parts(2).toDouble
      val y = parts(3).toDouble
      val z = parts(4).toDouble
      val t = parts(5).toDouble
      val vx = parts(7).toDouble
      val vy = parts(8).toDouble
      val vz = parts(9).toDouble
      
      val timeMillis = LocalDateTime.parse(timeIso, formatter).atZone(ZoneOffset.UTC).toInstant.toEpochMilli
      
      Some(GeoSimPointLine(
        simId = simId,
        timeIso = timeIso,
        timeMillis = timeMillis,
        x = x,
        y = y,
        z = z,
        t = t,
        vx = vx,
        vy = vy,
        vz = vz,
        rawLine = line
      ))
    } catch {
      case _: Exception => None
    }
  }
}
