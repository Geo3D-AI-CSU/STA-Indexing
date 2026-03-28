package index

object GeoSimTempBucket {

  val DEFAULT_TEMP_BUCKET_WIDTH_K: Double = 2.0
  val DEFAULT_TEMP_BUCKET_T0_K: Double = 0.0
  val DEFAULT_TEMP_BUCKET_METHOD: String = "floor"

  case class TempBucketParams(widthK: Double, t0K: Double, method: String)

  def bucketOf(t: Double, p: TempBucketParams): Int = {
    p.method match {
      case "floor" =>
        val offset = t - p.t0K
        if (offset >= 0) {
          (offset / p.widthK).toInt
        } else {
          ((offset - p.widthK + 1) / p.widthK).toInt
        }
      case other =>
        throw new IllegalArgumentException(s"Unsupported bucket method: $other. Only 'floor' is supported.")
    }
  }

  def bucketRange(tMin: Double, tMax: Double, p: TempBucketParams): (Int, Int) = {
    val (actualMin, actualMax) = if (tMin <= tMax) (tMin, tMax) else (tMax, tMin)
    val bucketMin = bucketOf(actualMin, p)
    val bucketMax = bucketOf(actualMax, p)
    (bucketMin, bucketMax)
  }

  def defaultParams: TempBucketParams = {
    TempBucketParams(DEFAULT_TEMP_BUCKET_WIDTH_K, DEFAULT_TEMP_BUCKET_T0_K, DEFAULT_TEMP_BUCKET_METHOD)
  }
}
