// src/main/scala/index/GeoSimVoxelGrid.scala
package index

import org.apache.hadoop.hbase.client.{Connection, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import storage.HBaseTableManager

case class GridMeta(
  x0: Double,
  y0: Double,
  z0: Double,
  dx0: Double,
  dy0: Double,
  dz0: Double,
  biasI: Long,
  biasJ: Long,
  biasK: Long,
  blockX: Int,
  blockY: Int,
  blockZ: Int,
  delimiterName: String
)

object GeoSimVoxelGrid {
  val META_ROWKEY = "__GEOSIM_VOXEL_GRID__"
  
  val QUAL_X0 = "x0"
  val QUAL_Y0 = "y0"
  val QUAL_Z0 = "z0"
  val QUAL_DX0 = "dx0"
  val QUAL_DY0 = "dy0"
  val QUAL_DZ0 = "dz0"
  val QUAL_BIAS_I = "bias_i"
  val QUAL_BIAS_J = "bias_j"
  val QUAL_BIAS_K = "bias_k"
  val QUAL_BLOCK_X = "block_x"
  val QUAL_BLOCK_Y = "block_y"
  val QUAL_BLOCK_Z = "block_z"
  val QUAL_DELIMITER = "delimiter"
  val Q_HEADER_LINE = "header_line"
  val Q_TEMP_BUCKET_WIDTH_K = "temp_bucket_width_k"
  val Q_TEMP_BUCKET_T0_K = "temp_bucket_t0_k"
  val Q_TEMP_BUCKET_METHOD = "temp_bucket_method"
  val Q_VEL_V0 = "vel_v0"
  val Q_VEL_METHOD = "vel_method"
  val Q_VEL_DVX = "vel_dvx"
  val Q_VEL_DVY = "vel_dvy"
  val Q_VEL_DVZ = "vel_dvz"
  
  val DEFAULT_X0 = 0.0
  val DEFAULT_Y0 = 0.0
  val DEFAULT_Z0 = 0.0
  
  val BLOCK_X = 16
  val BLOCK_Y = 16
  val BLOCK_Z = 8
  
  def blockSizeForLevel(unifiedLevel: Int): (Int, Int, Int) = {
    unifiedLevel match {
      case 4 => (16, 16, 8)
      case 5 => (8, 8, 4)
      case 6 => (4, 4, 2)
      case 7 => (2, 2, 1)
      case 8 => (1, 1, 1)
      case _ => throw new IllegalArgumentException("GeoSimVoxel only supports unifiedLevel 4/5/6/7/8")
    }
  }
  
  def quantizeIndex(v: Double, v0: Double, step: Double): Long = {
    Math.round((v - v0) / step)
  }
  
  def applyBias(i: Long, bias: Long): Long = {
    i + bias
  }
  
  def blockIndex(iBiased: Long, blockSize: Int): Long = {
    iBiased / blockSize
  }
  
  def computeIJK(meta: GridMeta, xMin: Double, yMin: Double, zMin: Double): (Long, Long, Long) = {
    val i = quantizeIndex(xMin, meta.x0, meta.dx0)
    val j = quantizeIndex(yMin, meta.y0, meta.dy0)
    val k = quantizeIndex(zMin, meta.z0, meta.dz0)
    
    val iB = applyBias(i, meta.biasI)
    val jB = applyBias(j, meta.biasJ)
    val kB = applyBias(k, meta.biasK)
    
    (iB, jB, kB)
  }
  
  def computeZCellFromIJK(meta: GridMeta, iB: Long, jB: Long, kB: Long, unifiedLevel: Int): Long = {
    val (bxSize, bySize, bzSize) = blockSizeForLevel(unifiedLevel)
    val bi = blockIndex(iB, bxSize)
    val bj = blockIndex(jB, bySize)
    val bk = blockIndex(kB, bzSize)
    
    VolumeZCell.encodeMorton(bi, bj, bk)
  }
  
  def enumerateZCellsForBBox(meta: GridMeta, xMin: Double, yMin: Double, zMin: Double,
                             xMax: Double, yMax: Double, zMax: Double,
                             unifiedLevel: Int, maxCells: Int = 1000000): Seq[Long] = {
    val (bxSize, bySize, bzSize) = blockSizeForLevel(unifiedLevel)
    
    val iStart = quantizeIndex(xMin, meta.x0, meta.dx0)
    val iEnd = quantizeIndex(xMax, meta.x0, meta.dx0)
    val jStart = quantizeIndex(yMin, meta.y0, meta.dy0)
    val jEnd = quantizeIndex(yMax, meta.y0, meta.dy0)
    val kStart = quantizeIndex(zMin, meta.z0, meta.dz0)
    val kEnd = quantizeIndex(zMax, meta.z0, meta.dz0)
    
    val iBStart = applyBias(iStart, meta.biasI)
    val iBEnd = applyBias(iEnd, meta.biasI)
    val jBStart = applyBias(jStart, meta.biasJ)
    val jBEnd = applyBias(jEnd, meta.biasJ)
    val kBStart = applyBias(kStart, meta.biasK)
    val kBEnd = applyBias(kEnd, meta.biasK)
    
    val biStart = blockIndex(iBStart, bxSize)
    val biEnd = blockIndex(iBEnd, bxSize)
    val bjStart = blockIndex(jBStart, bySize)
    val bjEnd = blockIndex(jBEnd, bySize)
    val bkStart = blockIndex(kBStart, bzSize)
    val bkEnd = blockIndex(kBEnd, bzSize)
    
    val result = scala.collection.mutable.ListBuffer[Long]()
    
    var bi = biStart
    while (bi <= biEnd && result.size < maxCells) {
      var bj = bjStart
      while (bj <= bjEnd && result.size < maxCells) {
        var bk = bkStart
        while (bk <= bkEnd && result.size < maxCells) {
          val zCell = VolumeZCell.encodeMorton(bi, bj, bk)
          result += zCell
          bk += 1
        }
        bj += 1
      }
      bi += 1
    }
    
    if (result.size >= maxCells) {
      println(s"[WARN] enumerateZCells reached maxCells=$maxCells at level=$unifiedLevel, truncated")
    }
    
    result.toSeq
  }
  
  def writeMeta(connection: Connection, dataset: Option[String], meta: GridMeta, headerLine: String): Unit = {
    val tableName = HBaseTableManager.volumeMetaTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val put = new Put(Bytes.toBytes(META_ROWKEY))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_X0), Bytes.toBytes(meta.x0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_Y0), Bytes.toBytes(meta.y0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_Z0), Bytes.toBytes(meta.z0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DX0), Bytes.toBytes(meta.dx0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DY0), Bytes.toBytes(meta.dy0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DZ0), Bytes.toBytes(meta.dz0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_BIAS_I), Bytes.toBytes(meta.biasI))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_BIAS_J), Bytes.toBytes(meta.biasJ))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_BIAS_K), Bytes.toBytes(meta.biasK))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_BLOCK_X), Bytes.toBytes(meta.blockX))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_BLOCK_Y), Bytes.toBytes(meta.blockY))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_BLOCK_Z), Bytes.toBytes(meta.blockZ))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER), Bytes.toBytes(meta.delimiterName))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_HEADER_LINE), Bytes.toBytes(headerLine))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_TEMP_BUCKET_WIDTH_K), Bytes.toBytes(2.0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_TEMP_BUCKET_T0_K), Bytes.toBytes(0.0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_TEMP_BUCKET_METHOD), Bytes.toBytes("floor"))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_V0), Bytes.toBytes(0.0))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_METHOD), Bytes.toBytes("floor"))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_DVX), Bytes.toBytes(5e-6))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_DVY), Bytes.toBytes(5e-6))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_DVZ), Bytes.toBytes(5e-5))
      
      table.put(put)
    } finally {
      table.close()
    }
  }
  
  def readMeta(connection: Connection, dataset: Option[String]): GridMeta = {
    val tableName = HBaseTableManager.volumeMetaTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val get = new Get(Bytes.toBytes(META_ROWKEY))
      val result = table.get(get)
      
      if (result.isEmpty) {
        throw new IllegalStateException(s"GeoSim voxel grid metadata row not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      
      val x0 = getValueAsDouble(result, QUAL_X0, dataset)
      val y0 = getValueAsDouble(result, QUAL_Y0, dataset)
      val z0 = getValueAsDouble(result, QUAL_Z0, dataset)
      val dx0 = getValueAsDouble(result, QUAL_DX0, dataset)
      val dy0 = getValueAsDouble(result, QUAL_DY0, dataset)
      val dz0 = getValueAsDouble(result, QUAL_DZ0, dataset)
      val biasI = getValueAsLong(result, QUAL_BIAS_I, dataset)
      val biasJ = getValueAsLong(result, QUAL_BIAS_J, dataset)
      val biasK = getValueAsLong(result, QUAL_BIAS_K, dataset)
      val blockX = getValueAsInt(result, QUAL_BLOCK_X, dataset)
      val blockY = getValueAsInt(result, QUAL_BLOCK_Y, dataset)
      val blockZ = getValueAsInt(result, QUAL_BLOCK_Z, dataset)
      
      val delimiterBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER))
      if (delimiterBytes == null) {
        throw new IllegalStateException(s"GeoSim voxel grid metadata field '$QUAL_DELIMITER' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val delimiterName = Bytes.toString(delimiterBytes)
      
      GridMeta(x0, y0, z0, dx0, dy0, dz0, biasI, biasJ, biasK, blockX, blockY, blockZ, delimiterName)
    } finally {
      table.close()
    }
  }
  
  def readMetaWithHeader(connection: Connection, dataset: Option[String]): (GridMeta, String) = {
    val tableName = HBaseTableManager.volumeMetaTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val get = new Get(Bytes.toBytes(META_ROWKEY))
      val result = table.get(get)
      
      if (result.isEmpty) {
        throw new IllegalStateException(s"GeoSim voxel grid metadata row not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      
      val x0 = getValueAsDouble(result, QUAL_X0, dataset)
      val y0 = getValueAsDouble(result, QUAL_Y0, dataset)
      val z0 = getValueAsDouble(result, QUAL_Z0, dataset)
      val dx0 = getValueAsDouble(result, QUAL_DX0, dataset)
      val dy0 = getValueAsDouble(result, QUAL_DY0, dataset)
      val dz0 = getValueAsDouble(result, QUAL_DZ0, dataset)
      val biasI = getValueAsLong(result, QUAL_BIAS_I, dataset)
      val biasJ = getValueAsLong(result, QUAL_BIAS_J, dataset)
      val biasK = getValueAsLong(result, QUAL_BIAS_K, dataset)
      val blockX = getValueAsInt(result, QUAL_BLOCK_X, dataset)
      val blockY = getValueAsInt(result, QUAL_BLOCK_Y, dataset)
      val blockZ = getValueAsInt(result, QUAL_BLOCK_Z, dataset)
      
      val delimiterBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER))
      if (delimiterBytes == null) {
        throw new IllegalStateException(s"GeoSim voxel grid metadata field '$QUAL_DELIMITER' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val delimiterName = Bytes.toString(delimiterBytes)
      
      val headerBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_HEADER_LINE))
      val headerLine = if (headerBytes != null) {
        Bytes.toString(headerBytes)
      } else {
        "raw_line"
      }
      
      val meta = GridMeta(x0, y0, z0, dx0, dy0, dz0, biasI, biasJ, biasK, blockX, blockY, blockZ, delimiterName)
      (meta, headerLine)
    } finally {
      table.close()
    }
  }

  def readMetaWithHeaderAndTemp(
    connection: org.apache.hadoop.hbase.client.Connection,
    dataset: Option[String]
  ): (GridMeta, String, index.GeoSimTempBucket.TempBucketParams) = {
    val tableName = HBaseTableManager.volumeMetaTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))

    try {
      val get = new Get(Bytes.toBytes(META_ROWKEY))
      val result = table.get(get)

      if (result.isEmpty) {
        throw new IllegalStateException(s"GeoSim voxel grid metadata row not found for dataset: ${dataset.getOrElse("(default)")}")
      }

      val x0 = getValueAsDouble(result, QUAL_X0, dataset)
      val y0 = getValueAsDouble(result, QUAL_Y0, dataset)
      val z0 = getValueAsDouble(result, QUAL_Z0, dataset)
      val dx0 = getValueAsDouble(result, QUAL_DX0, dataset)
      val dy0 = getValueAsDouble(result, QUAL_DY0, dataset)
      val dz0 = getValueAsDouble(result, QUAL_DZ0, dataset)
      val biasI = getValueAsLong(result, QUAL_BIAS_I, dataset)
      val biasJ = getValueAsLong(result, QUAL_BIAS_J, dataset)
      val biasK = getValueAsLong(result, QUAL_BIAS_K, dataset)
      val blockX = getValueAsInt(result, QUAL_BLOCK_X, dataset)
      val blockY = getValueAsInt(result, QUAL_BLOCK_Y, dataset)
      val blockZ = getValueAsInt(result, QUAL_BLOCK_Z, dataset)

      val delimiterBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER))
      if (delimiterBytes == null) {
        throw new IllegalStateException(s"GeoSim voxel grid metadata field '$QUAL_DELIMITER' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val delimiterName = Bytes.toString(delimiterBytes)

      val headerBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_HEADER_LINE))
      val headerLine = if (headerBytes != null) {
        Bytes.toString(headerBytes)
      } else {
        "raw_line"
      }

      val tempBucketParams = readTempBucketParams(result, dataset)

      val meta = GridMeta(x0, y0, z0, dx0, dy0, dz0, biasI, biasJ, biasK, blockX, blockY, blockZ, delimiterName)
      (meta, headerLine, tempBucketParams)
    } finally {
      table.close()
    }
  }

  private def readTempBucketParams(result: org.apache.hadoop.hbase.client.Result, dataset: Option[String]): index.GeoSimTempBucket.TempBucketParams = {
    val widthBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_TEMP_BUCKET_WIDTH_K))
    val t0Bytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_TEMP_BUCKET_T0_K))
    val methodBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_TEMP_BUCKET_METHOD))

    if (widthBytes != null && t0Bytes != null && methodBytes != null) {
      val widthK = Bytes.toDouble(widthBytes)
      val t0K = Bytes.toDouble(t0Bytes)
      val method = Bytes.toString(methodBytes)
      index.GeoSimTempBucket.TempBucketParams(widthK, t0K, method)
    } else {
      index.GeoSimTempBucket.defaultParams
    }
  }

  private def readVelBucketParams(result: org.apache.hadoop.hbase.client.Result, dataset: Option[String]): (Double, String, Double, Double, Double) = {
    val v0Bytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_V0))
    val methodBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_METHOD))
    val dvxBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_DVX))
    val dvyBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_DVY))
    val dvzBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_VEL_DVZ))

    if (v0Bytes != null && methodBytes != null && dvxBytes != null && dvyBytes != null && dvzBytes != null) {
      val v0 = Bytes.toDouble(v0Bytes)
      val method = Bytes.toString(methodBytes)
      val dvx = Bytes.toDouble(dvxBytes)
      val dvy = Bytes.toDouble(dvyBytes)
      val dvz = Bytes.toDouble(dvzBytes)
      (v0, method, dvx, dvy, dvz)
    } else {
      (0.0, "floor", 5e-6, 5e-6, 5e-5)
    }
  }

  def readVelBucketParamsFromMeta(meta: GridMeta): (Double, String, Double, Double, Double) = {
    (0.0, "floor", 5e-6, 5e-6, 5e-5)
  }

  def readMetaWithHeaderTempVel(
    connection: org.apache.hadoop.hbase.client.Connection,
    dataset: Option[String]
  ): (GridMeta, String, index.GeoSimTempBucket.TempBucketParams, (Double, String, Double, Double, Double)) = {
    val tableName = HBaseTableManager.volumeMetaTableName(dataset)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))

    try {
      val get = new Get(Bytes.toBytes(META_ROWKEY))
      val result = table.get(get)

      if (result.isEmpty) {
        throw new IllegalStateException(s"GeoSim voxel grid metadata row not found for dataset: ${dataset.getOrElse("(default)")}")
      }

      val x0 = getValueAsDouble(result, QUAL_X0, dataset)
      val y0 = getValueAsDouble(result, QUAL_Y0, dataset)
      val z0 = getValueAsDouble(result, QUAL_Z0, dataset)
      val dx0 = getValueAsDouble(result, QUAL_DX0, dataset)
      val dy0 = getValueAsDouble(result, QUAL_DY0, dataset)
      val dz0 = getValueAsDouble(result, QUAL_DZ0, dataset)
      val biasI = getValueAsLong(result, QUAL_BIAS_I, dataset)
      val biasJ = getValueAsLong(result, QUAL_BIAS_J, dataset)
      val biasK = getValueAsLong(result, QUAL_BIAS_K, dataset)
      val blockX = getValueAsInt(result, QUAL_BLOCK_X, dataset)
      val blockY = getValueAsInt(result, QUAL_BLOCK_Y, dataset)
      val blockZ = getValueAsInt(result, QUAL_BLOCK_Z, dataset)

      val delimiterBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(QUAL_DELIMITER))
      if (delimiterBytes == null) {
        throw new IllegalStateException(s"GeoSim voxel grid metadata field '$QUAL_DELIMITER' not found for dataset: ${dataset.getOrElse("(default)")}")
      }
      val delimiterName = Bytes.toString(delimiterBytes)

      val headerBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(Q_HEADER_LINE))
      val headerLine = if (headerBytes != null) {
        Bytes.toString(headerBytes)
      } else {
        "raw_line"
      }

      val tempBucketParams = readTempBucketParams(result, dataset)
      val velBucketParams = readVelBucketParams(result, dataset)

      val meta = GridMeta(x0, y0, z0, dx0, dy0, dz0, biasI, biasJ, biasK, blockX, blockY, blockZ, delimiterName)
      (meta, headerLine, tempBucketParams, velBucketParams)
    } finally {
      table.close()
    }
  }
  
  private def getValueAsDouble(result: org.apache.hadoop.hbase.client.Result, qualifier: String, dataset: Option[String]): Double = {
    val bytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(qualifier))
    if (bytes == null) {
      throw new IllegalStateException(s"GeoSim voxel grid metadata field '$qualifier' not found for dataset: ${dataset.getOrElse("(default)")}")
    }
    Bytes.toDouble(bytes)
  }
  
  private def getValueAsLong(result: org.apache.hadoop.hbase.client.Result, qualifier: String, dataset: Option[String]): Long = {
    val bytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(qualifier))
    if (bytes == null) {
      throw new IllegalStateException(s"GeoSim voxel grid metadata field '$qualifier' not found for dataset: ${dataset.getOrElse("(default)")}")
    }
    Bytes.toLong(bytes)
  }
  
  private def getValueAsInt(result: org.apache.hadoop.hbase.client.Result, qualifier: String, dataset: Option[String]): Int = {
    val bytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes(qualifier))
    if (bytes == null) {
      throw new IllegalStateException(s"GeoSim voxel grid metadata field '$qualifier' not found for dataset: ${dataset.getOrElse("(default)")}")
    }
    Bytes.toInt(bytes)
  }
}
