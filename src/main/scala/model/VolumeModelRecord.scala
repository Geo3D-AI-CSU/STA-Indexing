// src/main/scala/model/VolumeModelRecord.scala
package model

import java.time.Instant

/**
 * Voxel model record
 * Used for parsing a row from input CSV/TSV, for ingest use
 */
case class VolumeModelRecord(
  modelId: String,
  timeIso: String,
  timeMillis: Long,
  modelType: String,
  lonMin: Double,
  latMin: Double,
  zMin: Double,
  lonMax: Double,
  latMax: Double,
  zMax: Double,
  nx: Int,
  ny: Int,
  nz: Int,
  payloadB64: String
)

object VolumeModelRecord {

  // Missing value marker
  val NoData = -9999f

  /**
   * Parses a CSV/TSV record line
   * @param line Input line
   * @param delimiter Delimiter character
   * @return Right(record) on success, Left(errorMessage) on failure
   */
  def parseLine(line: String, delimiter: Char): Either[String, VolumeModelRecord] = {
    if (line == null || line.trim.isEmpty) {
      return Left("Empty line")
    }

    // Detect header row
    val parts = line.split(delimiter).map(_.trim)
    if (parts.nonEmpty && parts.head == "model_id") {
      return Left("Header row (skip)")
    }

    // Must have at least 13 fields
    if (parts.length < 13) {
      return Left(s"Missing fields: need 13 fields, got ${parts.length}")
    }

    try {
      val modelId = parts(0)
      val timeIso = parts(1)
      val timeMillis = try {
        Instant.parse(timeIso).toEpochMilli
      } catch {
        case e: Exception => return Left(s"Time parse failed: $timeIso - ${e.getMessage}")
      }
      val modelType = parts(2)

      val lonMin = try { parts(3).toDouble } catch {
        case _: NumberFormatException => return Left(s"lonMin number parse failed: ${parts(3)}")
      }
      val latMin = try { parts(4).toDouble } catch {
        case _: NumberFormatException => return Left(s"latMin number parse failed: ${parts(4)}")
      }
      val zMin = try { parts(5).toDouble } catch {
        case _: NumberFormatException => return Left(s"zMin number parse failed: ${parts(5)}")
      }
      val lonMax = try { parts(6).toDouble } catch {
        case _: NumberFormatException => return Left(s"lonMax number parse failed: ${parts(6)}")
      }
      val latMax = try { parts(7).toDouble } catch {
        case _: NumberFormatException => return Left(s"latMax number parse failed: ${parts(7)}")
      }
      val zMax = try { parts(8).toDouble } catch {
        case _: NumberFormatException => return Left(s"zMax number parse failed: ${parts(8)}")
      }

      val nx = try { parts(9).toInt } catch {
        case _: NumberFormatException => return Left(s"nx number parse failed: ${parts(9)}")
      }
      val ny = try { parts(10).toInt } catch {
        case _: NumberFormatException => return Left(s"ny number parse failed: ${parts(10)}")
      }
      val nz = try { parts(11).toInt } catch {
        case _: NumberFormatException => return Left(s"nz number parse failed: ${parts(11)}")
      }

      val payloadB64 = parts(12)

      // Validate dimension values are reasonable
      if (nx <= 0 || ny <= 0 || nz <= 0) {
        return Left(s"Dimension values must be positive: nx=$nx, ny=$ny, nz=$nz")
      }

      Right(VolumeModelRecord(
        modelId,
        timeIso,
        timeMillis,
        modelType,
        lonMin, latMin, zMin,
        lonMax, latMax, zMax,
        nx, ny, nz,
        payloadB64
      ))

    } catch {
      case e: Exception => Left(s"Parse exception: ${e.getMessage}")
    }
  }

  /**
   * Auto-detects delimiter
   * @param line First line (usually header)
   * @return ',' or '\t'
   */
  def detectDelimiter(line: String): Char = {
    if (line.contains('\t')) '\t' else ','
  }
}