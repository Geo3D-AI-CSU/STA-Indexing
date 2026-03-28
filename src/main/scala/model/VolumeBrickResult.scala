package model

/**
 * Volume Brick query result
 */
case class VolumeBrickResult(
  modelId: String,
  timeIso: String,
  timeMillis: Long,
  modelType: String,
  tileI: Int,
  tileJ: Int,
  tileK: Int,
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
