// src/main/scala/query/SelectivityEstimator.scala
package query

import index.{Z3DEncoder, TimeBucket}

object SelectivityEstimator {
  
  var totalRecords: Long = 1000000L
  var distinctSensorIds: Int = 50
  var distinctTypes: Int = 4
  var timeRangeMs: Long = 30L * 24 * 60 * 60 * 1000
  
  sealed trait QueryCondition {
    def selectivity: Double
  }
  
  case class SensorIdEquals(sensorId: String) extends QueryCondition {
    override def selectivity: Double = 1.0 / distinctSensorIds
  }
  
  case class TypeEquals(sensorType: String) extends QueryCondition {
    override def selectivity: Double = 1.0 / distinctTypes
  }
  
  case class TimeRange(startTime: Long, endTime: Long) extends QueryCondition {
    override def selectivity: Double = {
      val queryRange = endTime - startTime
      math.min(1.0, queryRange.toDouble / timeRangeMs)
    }
  }
  
  case class SpatialBBox(
    lonMin: Double, latMin: Double, altMin: Double,
    lonMax: Double, latMax: Double, altMax: Double
  ) extends QueryCondition {
    override def selectivity: Double = {
      val queryVolume = (lonMax - lonMin) * (latMax - latMin) * (altMax - altMin)
      val totalVolume = (Z3DEncoder.LON_MAX - Z3DEncoder.LON_MIN) *
                        (Z3DEncoder.LAT_MAX - Z3DEncoder.LAT_MIN) *
                        (Z3DEncoder.ALT_MAX - Z3DEncoder.ALT_MIN)
      math.min(1.0, queryVolume / totalVolume)
    }
  }
  
  def sortBySelectivity(conditions: Seq[QueryCondition]): Seq[QueryCondition] = {
    // [Improved] Fixed query order: Attribute(SensorId/Type) → Time(TimeRange) → Space(SpatialBBox)
    // This ensures consistent incremental filter query order for performance testing and comparison

    val sorted = scala.collection.mutable.ListBuffer[QueryCondition]()

    // Step 1: Add all attribute conditions
    conditions.foreach {
      case sensor: SensorIdEquals => sorted += sensor
      case typeEq: TypeEquals => sorted += typeEq
      case _ =>
    }

    // Step 2: Add time conditions
    conditions.foreach {
      case time: TimeRange => sorted += time
      case _ =>
    }

    // Step 3: Add spatial conditions
    conditions.foreach {
      case spatial: SpatialBBox => sorted += spatial
      case _ =>
    }

    sorted.toSeq
  }
}