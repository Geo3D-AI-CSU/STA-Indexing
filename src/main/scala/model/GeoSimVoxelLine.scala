// src/main/scala/model/GeoSimVoxelLine.scala
package model

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

case class GeoSimVoxelLine(
  simId: String,
  timeIso: String,
  timeMillis: Long,
  xMin: Double,
  xMax: Double,
  yMin: Double,
  yMax: Double,
  zMin: Double,
  zMax: Double,
  t: Double,
  vx: Double,
  vy: Double,
  vz: Double,
  rawLine: String
) {
  def dx: Double = xMax - xMin
  def dy: Double = yMax - yMin
  def dz: Double = zMax - zMin
}

object GeoSimVoxelLine {
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

  def detectDelimiter(firstLine: String): Char = {
    if (firstLine.contains('\t')) '\t' else ','
  }

  def normalizeSimId(simId: String): String = {
    val trimmed = simId.trim
    val parts = trimmed.split("/")
    
    if (parts.length == 2) {
      val left = parts(0).trim
      val right = parts(1).trim
      
      val leftNormalized = try {
        val d = left.toDouble
        if (d == d.toLong.toDouble) d.toLong.toString else left
      } catch {
        case _: NumberFormatException => left
      }
      
      val rightNormalized = try {
        val d = right.toDouble
        if (d == d.toLong.toDouble) d.toLong.toString else right
      } catch {
        case _: NumberFormatException => right
      }
      
      s"$leftNormalized/$rightNormalized"
    } else {
      trimmed
    }
  }

  def fromLine(line: String, delimiter: Char): Option[GeoSimVoxelLine] = {
    try {
      val parts = line.split(delimiter).map(_.trim)
      
      if (parts.length < 13) {
        return None
      }
      
      if (parts(0) == "sim_id") {
        return None
      }
      
      val simId = normalizeSimId(parts(0))
      val timeIso = parts(1)
      val xMin = parts(2).toDouble
      val xMax = parts(3).toDouble
      val yMin = parts(4).toDouble
      val yMax = parts(5).toDouble
      val zMin = parts(6).toDouble
      val zMax = parts(7).toDouble
      val t = parts(8).toDouble
      val vx = parts(10).toDouble
      val vy = parts(11).toDouble
      val vz = parts(12).toDouble
      
      val timeMillis = LocalDateTime.parse(timeIso, formatter).atZone(ZoneOffset.UTC).toInstant.toEpochMilli
      
      Some(GeoSimVoxelLine(
        simId = simId,
        timeIso = timeIso,
        timeMillis = timeMillis,
        xMin = xMin,
        xMax = xMax,
        yMin = yMin,
        yMax = yMax,
        zMin = zMin,
        zMax = zMax,
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
