// src/main/scala/index/TimeBucket.scala
package index

import java.time.{Instant, ZoneId, LocalTime}

object TimeBucket {
  
  val DAY_MS = 24 * 60 * 60 * 1000L
  
  def dayBucket(timestamp: Long): Int = {
    val date = Instant.ofEpochMilli(timestamp)
      .atZone(ZoneId.systemDefault())
      .toLocalDate
    date.getYear * 10000 + date.getMonthValue * 100 + date.getDayOfMonth
  }
  
  def dayBuckets(startTime: Long, endTime: Long): Seq[Int] = {
    val startDate = Instant.ofEpochMilli(startTime)
      .atZone(ZoneId.systemDefault())
      .toLocalDate
    val endDate = Instant.ofEpochMilli(endTime)
      .atZone(ZoneId.systemDefault())
      .toLocalDate

    Iterator.iterate(startDate)(_.plusDays(1))
      .takeWhile(!_.isAfter(endDate))
      .map(d => d.getYear * 10000 + d.getMonthValue * 100 + d.getDayOfMonth)
      .toSeq
  }

  /**
   * Calculates the milliseconds within the day for a given timestamp
   * (Counted from 00:00:00 of the day, range 0 to 86399999)
   */
  def millisOfDay(timestamp: Long): Int = {
    val time = Instant.ofEpochMilli(timestamp)
      .atZone(ZoneId.systemDefault())
      .toLocalTime
    val secondOfDay = time.toSecondOfDay
    val nanos = time.getNano
    (secondOfDay * 1000L + nanos / 1000000L).toInt
  }

  /**
   * Enumerates all dayBuckets (yyyyMMdd as Int) within the given time range
   * Includes both the start and end days, returned in ascending order
   */
  def enumerateDays(fromTs: Long, toTs: Long): Seq[Int] = {
    if (fromTs > toTs) {
      // If start time is greater than end time, swap and retry
      enumerateDays(toTs, fromTs)
    } else {
      val startDate = Instant.ofEpochMilli(fromTs)
        .atZone(ZoneId.systemDefault())
        .toLocalDate
      val endDate = Instant.ofEpochMilli(toTs)
        .atZone(ZoneId.systemDefault())
        .toLocalDate

      Iterator.iterate(startDate)(_.plusDays(1))
        .takeWhile(!_.isAfter(endDate))
        .map(d => d.getYear * 10000 + d.getMonthValue * 100 + d.getDayOfMonth)
        .toSeq
    }
  }
}