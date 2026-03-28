// src/main/scala/index/VolumeTimeBucketUtc.scala
package index

import java.time.{Instant, LocalDate, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

/**
 * UTC time bucket utility for Volume system
 *
 * The volume system uses yyyyMMdd (Int) for dayBucket, isolated from the legacy point data system.
 * For example: 2025-11-05 corresponds to 20251105
 *
 * Used for time dimension partitioning in volume unified index, based on UTC timezone
 * Avoids affecting the original sensor system's TimeBucket implementation
 */
object VolumeTimeBucketUtc {
  
  /**
   * ISO8601 format parser (with timezone)
   */
  private val ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT

  /**
   * Parses an ISO8601 time string (with Z suffix) to Instant
   *
   * @param timeIsoZ ISO8601 format time string, e.g., "2025-11-05T17:00:53Z"
   * @return UTC Instant object
   */
  def parseIsoZ(timeIsoZ: String): Instant = {
    Instant.parse(timeIsoZ)
  }

  /**
   * Calculates dayBucket (yyyyMMdd format)
   *
   * For example: 2025-11-05 UTC -> 20251105
   *
   * @param instant UTC time point
   * @return yyyyMMdd format Int (e.g., 20251105)
   */
  def dayBucket(instant: Instant): Int = {
    val utcDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
    val localDate = utcDateTime.toLocalDate
    localDate.getYear * 10000 + localDate.getMonthValue * 100 + localDate.getDayOfMonth
  }

  /**
   * Calculates dayBucket (from millisecond timestamp, yyyyMMdd format)
   *
   * @param timeMillis Millisecond timestamp (UTC)
   * @return yyyyMMdd format Int (e.g., 20251105)
   */
  def dayBucket(timeMillis: Long): Int = {
    val instant = Instant.ofEpochMilli(timeMillis)
    dayBucket(instant)
  }

  /**
   * Calculates timeOfDay (milliseconds within the day, 0-86399999)
   *
   * @param instant UTC time point
   * @return timeOfDay (milliseconds)
   */
  def timeOfDay(instant: Instant): Int = {
    val utcDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
    val hour = utcDateTime.getHour
    val minute = utcDateTime.getMinute
    val second = utcDateTime.getSecond
    val nano = utcDateTime.getNano
    hour * 3600000 + minute * 60000 + second * 1000 + (nano / 1000000)
  }

  /**
   * Calculates timeOfDay (from millisecond timestamp)
   *
   * @param timeMillis Millisecond timestamp (UTC)
   * @return timeOfDay (milliseconds)
   */
  def timeOfDay(timeMillis: Long): Int = {
    val instant = Instant.ofEpochMilli(timeMillis)
    timeOfDay(instant)
  }

  /**
   * Enumerates all dayBuckets (yyyyMMdd format) within the time range
   *
   * @param startMillis Start time (milliseconds, UTC)
   * @param endMillis End time (milliseconds, UTC)
   * @return List of yyyyMMdd format Ints (including all days from start to end)
   */
  def enumerateDays(startMillis: Long, endMillis: Long): List[Int] = {
    val startInstant = Instant.ofEpochMilli(startMillis)
    val endInstant = Instant.ofEpochMilli(endMillis)
    
    val startDate = ZonedDateTime.ofInstant(startInstant, ZoneOffset.UTC).toLocalDate
    val endDate = ZonedDateTime.ofInstant(endInstant, ZoneOffset.UTC).toLocalDate
    
    (0 to java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate).toInt)
      .map { daysToAdd =>
        val date = startDate.plusDays(daysToAdd)
        date.getYear * 10000 + date.getMonthValue * 100 + date.getDayOfMonth
      }
      .toList
  }

  /**
   * Converts an Instant to UTC LocalDate
   *
   * @param instant Timestamp (Instant)
   * @return LocalDate in UTC timezone
   */
  def toLocalDateUtc(instant: Instant): LocalDate = {
    ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalDate
  }

  /**
   * Converts a millisecond timestamp to ISO8601 format string
   *
   * @param timeMillis Millisecond timestamp (UTC)
   * @return ISO8601 format string (with Z suffix)
   */
  def toIsoZ(timeMillis: Long): String = {
    Instant.ofEpochMilli(timeMillis).toString
  }
}
