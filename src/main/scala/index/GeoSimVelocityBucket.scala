package index

object GeoSimVelocityBucket {
  
  val DEFAULT_V0: Double = 0.0
  val DEFAULT_METHOD: String = "floor"
  val DEFAULT_DVX: Double = 1e-4
  val DEFAULT_DVY: Double = 1e-4
  val DEFAULT_DVZ: Double = 1e-3

  case class VelBucketParams(v0: Double, dvx: Double, dvy: Double, dvz: Double, method: String)

  def defaultParams: VelBucketParams = {
    VelBucketParams(DEFAULT_V0, DEFAULT_DVX, DEFAULT_DVY, DEFAULT_DVZ, DEFAULT_METHOD)
  }

  def bucketOf(v: Double, v0: Double, dv: Double, method: String): Int = {
    method match {
      case "floor" =>
        math.floor((v - v0) / dv).toInt
      case _ =>
        throw new IllegalArgumentException(s"Unsupported bucket method: $method")
    }
  }

  def bucketRange(vMin: Double, vMax: Double, v0: Double, dv: Double, method: String): (Int, Int) = {
    val actualMin = math.min(vMin, vMax)
    val actualMax = math.max(vMin, vMax)
    val bucketMin = bucketOf(actualMin, v0, dv, method)
    val bucketMax = bucketOf(actualMax, v0, dv, method)
    (bucketMin, bucketMax)
  }

  def encodeBucket(b: Int): Int = b ^ 0x80000000

  def decodeBucket(enc: Int): Int = enc ^ 0x80000000
}
