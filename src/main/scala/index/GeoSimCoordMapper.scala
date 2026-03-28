// src/main/scala/index/GeoSimCoordMapper.scala
package index

import org.apache.hadoop.hbase.client.{Connection, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import storage.HBaseTableManager

case class Bounds(
  xMin: Double,
  xMax: Double,
  yMin: Double,
  yMax: Double,
  zMin: Double,
  zMax: Double
)

object GeoSimCoordMapper {
  val META_ROWKEY = "__GEOSIM_POINT_META__"
  
  val QUAL_X_MIN = "x_min"
  val QUAL_X_MAX = "x_max"
  val QUAL_Y_MIN = "y_min"
  val QUAL_Y_MAX = "y_max"
  val QUAL_Z_MIN = "z_min"
  val QUAL_Z_MAX = "z_max"
  val QUAL_DELIMITER = "delimiter"
  val QUAL_HEADER_LINE = "header_line"
  val QUAL_TEMP_BUCKET_WIDTH_K = "temp_bucket_width_k"
  val QUAL_TEMP_BUCKET_T0_K = "temp_bucket_t0_k"
  val QUAL_TEMP_BUCKET_METHOD = "temp_bucket_method"
  val QUAL_VEL_V0 = "vel_v0"
  val QUAL_VEL_METHOD = "vel_method"
  val QUAL_VEL_DVX = "vel_dvx"
  val QUAL_VEL_DVY = "vel_dvy"
  val QUAL_VEL_DVZ = "vel_dvz"

  def detectDelimiter(firstLine: String): Char = {
    if (firstLine.contains('\t')) '\t' else ','
  }

  def mapX(bounds: Bounds, x: Double): Double = {
    val clampedX = math.max(bounds.xMin, math.min(bounds.xMax, x))
    Z3DEncoder.LON_MIN + (clampedX - bounds.xMin) / (bounds.xMax - bounds.xMin) * (Z3DEncoder.LON_MAX - Z3DEncoder.LON_MIN)
  }

  def mapY(bounds: Bounds, y: Double): Double = {
    val clampedY = math.max(bounds.yMin, math.min(bounds.yMax, y))
    Z3DEncoder.LAT_MIN + (clampedY - bounds.yMin) / (bounds.yMax - bounds.yMin) * (Z3DEncoder.LAT_MAX - Z3DEncoder.LAT_MIN)
  }

  def mapZ(bounds: Bounds, z: Double): Double = {
    val clampedZ = math.max(bounds.zMin, math.min(bounds.zMax, z))
    Z3DEncoder.ALT_MIN + (clampedZ - bounds.zMin) / (bounds.zMax - bounds.zMin) * (Z3DEncoder.ALT_MAX - Z3DEncoder.ALT_MIN)
  }

  def mapBBox(bounds: Bounds, xMin: Double, yMin: Double, zMin: Double, xMax: Double, yMax: Double, zMax: Double): (Double, Double, Double, Double, Double, Double) = {
    val lonMin = mapX(bounds, xMin)
    val latMin = mapY(bounds, yMin)
    val altMin = mapZ(bounds, zMin)
    val lonMax = mapX(bounds, xMax)
    val latMax = mapY(bounds, yMax)
    val altMax = mapZ(bounds, zMax)
    (lonMin, latMin, altMin, lonMax, latMax, altMax)
  }

  def writeMeta(connection: Connection, dataset: Option[String], bounds: Bounds, delimiterName: String, headerLine: String): Unit = {
    val tableName = HBaseTableManager.geoSimPointDataTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val put = new Put(Bytes.toBytes(META_ROWKEY))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_X_MIN), Bytes.toBytes(bounds.xMin))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_X_MAX), Bytes.toBytes(bounds.xMax))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_Y_MIN), Bytes.toBytes(bounds.yMin))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_Y_MAX), Bytes.toBytes(bounds.yMax))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_Z_MIN), Bytes.toBytes(bounds.zMin))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_Z_MAX), Bytes.toBytes(bounds.zMax))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER), Bytes.toBytes(delimiterName))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_HEADER_LINE), Bytes.toBytes(headerLine))
      
      table.put(put)
    } finally {
      table.close()
    }
  }

  def readMetaFull(connection: Connection, dataset: Option[String]): (Bounds, String, String) = {
    val tableName = HBaseTableManager.geoSimPointDataTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val get = new Get(Bytes.toBytes(META_ROWKEY))
      val result = table.get(get)
      
      if (result.isEmpty) {
        throw new IllegalStateException(s"GeoSim metadata row not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      
      val xMin = getValueAsDouble(result, QUAL_X_MIN, dataset)
      val xMax = getValueAsDouble(result, QUAL_X_MAX, dataset)
      val yMin = getValueAsDouble(result, QUAL_Y_MIN, dataset)
      val yMax = getValueAsDouble(result, QUAL_Y_MAX, dataset)
      val zMin = getValueAsDouble(result, QUAL_Z_MIN, dataset)
      val zMax = getValueAsDouble(result, QUAL_Z_MAX, dataset)
      
      val delimiterBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER))
      if (delimiterBytes == null) {
        throw new IllegalStateException(s"GeoSim metadata field '$QUAL_DELIMITER' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val delimiterName = Bytes.toString(delimiterBytes)
      
      val headerLineBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_HEADER_LINE))
      if (headerLineBytes == null) {
        throw new IllegalStateException(s"GeoSim metadata field '$QUAL_HEADER_LINE' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val headerLine = Bytes.toString(headerLineBytes)
      
      (Bounds(xMin, xMax, yMin, yMax, zMin, zMax), delimiterName, headerLine)
    } finally {
      table.close()
    }
  }

  def readMeta(connection: Connection, dataset: Option[String]): (Bounds, String) = {
    val tableName = HBaseTableManager.geoSimPointDataTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val get = new Get(Bytes.toBytes(META_ROWKEY))
      val result = table.get(get)
      
      if (result.isEmpty) {
        throw new IllegalStateException(s"GeoSim metadata row not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      
      val xMin = getValueAsDouble(result, QUAL_X_MIN, dataset)
      val xMax = getValueAsDouble(result, QUAL_X_MAX, dataset)
      val yMin = getValueAsDouble(result, QUAL_Y_MIN, dataset)
      val yMax = getValueAsDouble(result, QUAL_Y_MAX, dataset)
      val zMin = getValueAsDouble(result, QUAL_Z_MIN, dataset)
      val zMax = getValueAsDouble(result, QUAL_Z_MAX, dataset)
      
      val delimiterBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER))
      if (delimiterBytes == null) {
        throw new IllegalStateException(s"GeoSim metadata field '$QUAL_DELIMITER' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val delimiterName = Bytes.toString(delimiterBytes)
      
      (Bounds(xMin, xMax, yMin, yMax, zMin, zMax), delimiterName)
    } finally {
      table.close()
    }
  }

  private def getValueAsDouble(result: org.apache.hadoop.hbase.client.Result, qualifier: String, dataset: Option[String]): Double = {
    val bytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(qualifier))
    if (bytes == null) {
      throw new IllegalStateException(s"GeoSim metadata field '$qualifier' not found for dataset: ${dataset.getOrElse("(default)")}")
    }
    Bytes.toDouble(bytes)
  }

  def readMetaWithTemp(
    connection: org.apache.hadoop.hbase.client.Connection,
    dataset: Option[String]
  ): (Bounds, String, String, index.GeoSimTempBucket.TempBucketParams) = {
    val tableName = HBaseTableManager.geoSimPointDataTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val get = new Get(Bytes.toBytes(META_ROWKEY))
      val result = table.get(get)
      
      if (result.isEmpty) {
        throw new IllegalStateException(s"GeoSim metadata row not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      
      val xMin = getValueAsDouble(result, QUAL_X_MIN, dataset)
      val xMax = getValueAsDouble(result, QUAL_X_MAX, dataset)
      val yMin = getValueAsDouble(result, QUAL_Y_MIN, dataset)
      val yMax = getValueAsDouble(result, QUAL_Y_MAX, dataset)
      val zMin = getValueAsDouble(result, QUAL_Z_MIN, dataset)
      val zMax = getValueAsDouble(result, QUAL_Z_MAX, dataset)
      
      val delimiterBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER))
      if (delimiterBytes == null) {
        throw new IllegalStateException(s"GeoSim metadata field '$QUAL_DELIMITER' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val delimiterName = Bytes.toString(delimiterBytes)
      
      val headerLineBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_HEADER_LINE))
      if (headerLineBytes == null) {
        throw new IllegalStateException(s"GeoSim metadata field '$QUAL_HEADER_LINE' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val headerLine = Bytes.toString(headerLineBytes)
      
      val tempBucketParams = readTempBucketParams(result, dataset)
      
      (Bounds(xMin, xMax, yMin, yMax, zMin, zMax), delimiterName, headerLine, tempBucketParams)
    } finally {
      table.close()
    }
  }

  private def readTempBucketParams(result: org.apache.hadoop.hbase.client.Result, dataset: Option[String]): index.GeoSimTempBucket.TempBucketParams = {
    val widthBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_TEMP_BUCKET_WIDTH_K))
    val t0Bytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_TEMP_BUCKET_T0_K))
    val methodBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_TEMP_BUCKET_METHOD))
    
    if (widthBytes != null && t0Bytes != null && methodBytes != null) {
      val widthK = Bytes.toDouble(widthBytes)
      val t0K = Bytes.toDouble(t0Bytes)
      val method = Bytes.toString(methodBytes)
      index.GeoSimTempBucket.TempBucketParams(widthK, t0K, method)
    } else {
      index.GeoSimTempBucket.defaultParams
    }
  }

  def readMetaWithVelocity(
    connection: org.apache.hadoop.hbase.client.Connection,
    dataset: Option[String]
  ): (Bounds, String, String, index.GeoSimVelocityBucket.VelBucketParams) = {
    val tableName = HBaseTableManager.geoSimPointDataTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val get = new Get(Bytes.toBytes(META_ROWKEY))
      val result = table.get(get)
      
      if (result.isEmpty) {
        throw new IllegalStateException(s"GeoSim metadata row not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      
      val xMin = getValueAsDouble(result, QUAL_X_MIN, dataset)
      val xMax = getValueAsDouble(result, QUAL_X_MAX, dataset)
      val yMin = getValueAsDouble(result, QUAL_Y_MIN, dataset)
      val yMax = getValueAsDouble(result, QUAL_Y_MAX, dataset)
      val zMin = getValueAsDouble(result, QUAL_Z_MIN, dataset)
      val zMax = getValueAsDouble(result, QUAL_Z_MAX, dataset)
      
      val delimiterBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER))
      if (delimiterBytes == null) {
        throw new IllegalStateException(s"GeoSim metadata field '$QUAL_DELIMITER' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val delimiterName = Bytes.toString(delimiterBytes)
      
      val headerLineBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_HEADER_LINE))
      if (headerLineBytes == null) {
        throw new IllegalStateException(s"GeoSim metadata field '$QUAL_HEADER_LINE' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val headerLine = Bytes.toString(headerLineBytes)
      
      val velParams = readVelocityBucketParams(result, dataset)
      
      (Bounds(xMin, xMax, yMin, yMax, zMin, zMax), delimiterName, headerLine, velParams)
    } finally {
      table.close()
    }
  }

  private def readVelocityBucketParams(result: org.apache.hadoop.hbase.client.Result, dataset: Option[String]): index.GeoSimVelocityBucket.VelBucketParams = {
    val v0Bytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_V0))
    val methodBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_METHOD))
    val dvxBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_DVX))
    val dvyBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_DVY))
    val dvzBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_DVZ))
    
    if (v0Bytes != null && methodBytes != null && dvxBytes != null && dvyBytes != null && dvzBytes != null) {
      val v0 = Bytes.toDouble(v0Bytes)
      val method = Bytes.toString(methodBytes)
      val dvx = Bytes.toDouble(dvxBytes)
      val dvy = Bytes.toDouble(dvyBytes)
      val dvz = Bytes.toDouble(dvzBytes)
      index.GeoSimVelocityBucket.VelBucketParams(v0, dvx, dvy, dvz, method)
    } else {
      index.GeoSimVelocityBucket.defaultParams
    }
  }

  def writeVelocityParams(
    connection: org.apache.hadoop.hbase.client.Connection,
    dataset: Option[String],
    velParams: index.GeoSimVelocityBucket.VelBucketParams
  ): Unit = {
    val tableName = HBaseTableManager.geoSimPointDataTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val put = new Put(Bytes.toBytes(META_ROWKEY))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_V0), Bytes.toBytes(velParams.v0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_METHOD), Bytes.toBytes(velParams.method))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_DVX), Bytes.toBytes(velParams.dvx))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_DVY), Bytes.toBytes(velParams.dvy))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_VEL_DVZ), Bytes.toBytes(velParams.dvz))
      
      table.put(put)
    } finally {
      table.close()
    }
  }
}
