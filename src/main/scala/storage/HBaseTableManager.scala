// src/main/scala/storage/HBaseTableManager.scala
package storage

import java.nio.ByteBuffer
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, ColumnFamilyDescriptorBuilder, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes

object HBaseTableManager {

  val TABLE_DATA = "sensor_data"
  val TABLE_IDX_SENSOR = "idx_sensor_id"
  val TABLE_IDX_TIME = "idx_time"
  val TABLE_IDX_SPATIAL = "idx_spatial_z3d"
  val TABLE_IDX_UNIFIED = "idx_unified"

  // Volume table constants
  val TABLE_VOLUME_META = "volume_model_meta"
  val TABLE_VOLUME_BRICK = "volume_brick_data"
  val TABLE_VOLUME_IDX_UNIFIED = "volume_idx_unified"

  // GeoSim point table constants
  val TABLE_GEOSIM_POINT_DATA = "geosim_point_data"
  val TABLE_GEOSIM_POINT_IDX_UNIFIED = "geosim_point_idx_unified"
  val TABLE_GEOSIM_POINT_IDX_SIM = "geosim_point_idx_sim_id"
  val TABLE_GEOSIM_POINT_IDX_TIME = "geosim_point_idx_time"
  val TABLE_GEOSIM_POINT_IDX_SPATIAL = "geosim_point_idx_spatial_blk"
  val TABLE_GEOSIM_POINT_IDX_TEMP_BUCKET = "geosim_point_idx_temp_bucket"

  // GeoSim Voxel incremental index table constants
  val TABLE_GEOSIM_VOXEL_IDX_SIM = "geosim_voxel_idx_sim_id"
  val TABLE_GEOSIM_VOXEL_IDX_TIME = "geosim_voxel_idx_time"
  val TABLE_GEOSIM_VOXEL_IDX_SPATIAL = "geosim_voxel_idx_spatial"
  val TABLE_GEOSIM_VOXEL_IDX_UNIFIED_TEMP = "geosim_voxel_idx_unified_temp"
  val TABLE_GEOSIM_VOXEL_IDX_TEMP_BUCKET = "geosim_voxel_idx_temp_bucket"

  val CF = "cf"
  val CF_BYTES: Array[Byte] = Bytes.toBytes(CF)

  def createConnection(zkQuorum: String): Connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    ConnectionFactory.createConnection(conf)
  }

  // ========== Table name generation utility methods (for other modules to call) ==========

  def dataTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_DATA}_$d").getOrElse(TABLE_DATA)

  def sensorIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_IDX_SENSOR}_$d").getOrElse(TABLE_IDX_SENSOR)

  def timeIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_IDX_TIME}_$d").getOrElse(TABLE_IDX_TIME)

  def spatialIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_IDX_SPATIAL}_$d").getOrElse(TABLE_IDX_SPATIAL)

  def unifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"idx_unified_L${lvl}_$d"
      case (Some(d), None)      => s"${TABLE_IDX_UNIFIED}_$d"
      case (None, Some(lvl))    => s"idx_unified_L$lvl"
      case (None, None)         => TABLE_IDX_UNIFIED
    }
  }

  // Volume table name generation methods
  def volumeMetaTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_VOLUME_META}_$d").getOrElse(TABLE_VOLUME_META)

  def volumeBrickTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_VOLUME_BRICK}_$d").getOrElse(TABLE_VOLUME_BRICK)

  def volumeUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"volume_idx_unified_L${lvl}_$d"
      case (Some(d), None)      => s"${TABLE_VOLUME_IDX_UNIFIED}_$d"
      case (None, Some(lvl))    => s"volume_idx_unified_L$lvl"
      case (None, None)         => TABLE_VOLUME_IDX_UNIFIED
    }
  }

  // GeoSim point table name generation methods
  def geoSimPointDataTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_GEOSIM_POINT_DATA}_$d").getOrElse(TABLE_GEOSIM_POINT_DATA)

  def geoSimPointUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_point_idx_unified_L${lvl}_$d"
      case (Some(d), None)      => s"${TABLE_GEOSIM_POINT_IDX_UNIFIED}_$d"
      case (None, Some(lvl))    => s"geosim_point_idx_unified_L$lvl"
      case (None, None)         => TABLE_GEOSIM_POINT_IDX_UNIFIED
    }
  }

  def geoSimPointTempUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_point_idx_unified_temp_L${lvl}_$d"
      case (Some(d), None)      => s"geosim_point_idx_unified_temp_$d"
      case (None, Some(lvl))    => s"geosim_point_idx_unified_temp_L$lvl"
      case (None, None)         => "geosim_point_idx_unified_temp"
    }
  }

  def geoSimPointSimIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_GEOSIM_POINT_IDX_SIM}_$d").getOrElse(TABLE_GEOSIM_POINT_IDX_SIM)

  def geoSimPointTimeIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_GEOSIM_POINT_IDX_TIME}_$d").getOrElse(TABLE_GEOSIM_POINT_IDX_TIME)

  def geoSimPointSpatialIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_GEOSIM_POINT_IDX_SPATIAL}_$d").getOrElse(TABLE_GEOSIM_POINT_IDX_SPATIAL)

  def geoSimPointTempBucketIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_GEOSIM_POINT_IDX_TEMP_BUCKET}_$d").getOrElse(TABLE_GEOSIM_POINT_IDX_TEMP_BUCKET)

  def geoSimPointVxBucketIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"geosim_point_idx_vx_bucket_$d").getOrElse("geosim_point_idx_vx_bucket")

  def geoSimPointVyBucketIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"geosim_point_idx_vy_bucket_$d").getOrElse("geosim_point_idx_vy_bucket")

  def geoSimPointVzBucketIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"geosim_point_idx_vz_bucket_$d").getOrElse("geosim_point_idx_vz_bucket")

  def geoSimPointVxUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_point_idx_unified_vx_L${lvl}_$d"
      case (Some(d), None)      => s"geosim_point_idx_unified_vx_$d"
      case (None, Some(lvl))    => s"geosim_point_idx_unified_vx_L$lvl"
      case (None, None)         => "geosim_point_idx_unified_vx"
    }
  }

  def geoSimPointVyUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_point_idx_unified_vy_L${lvl}_$d"
      case (Some(d), None)      => s"geosim_point_idx_unified_vy_$d"
      case (None, Some(lvl))    => s"geosim_point_idx_unified_vy_L$lvl"
      case (None, None)         => "geosim_point_idx_unified_vy"
    }
  }

  def geoSimPointVzUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_point_idx_unified_vz_L${lvl}_$d"
      case (Some(d), None)      => s"geosim_point_idx_unified_vz_$d"
      case (None, Some(lvl))    => s"geosim_point_idx_unified_vz_L$lvl"
      case (None, None)         => "geosim_point_idx_unified_vz"
    }
  }

  // GeoSim Voxel incremental index table name generation methods
  def geoSimVoxelSimIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_GEOSIM_VOXEL_IDX_SIM}_$d").getOrElse(TABLE_GEOSIM_VOXEL_IDX_SIM)

  def geoSimVoxelTimeIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_GEOSIM_VOXEL_IDX_TIME}_$d").getOrElse(TABLE_GEOSIM_VOXEL_IDX_TIME)

  def geoSimVoxelSpatialIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_voxel_idx_spatial_L${lvl}_$d"
      case (Some(d), None)      => s"${TABLE_GEOSIM_VOXEL_IDX_SPATIAL}_$d"
      case (None, Some(lvl))    => s"geosim_voxel_idx_spatial_L$lvl"
      case (None, None)         => TABLE_GEOSIM_VOXEL_IDX_SPATIAL
    }
  }

  def geoSimVoxelTempUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_voxel_idx_unified_temp_L${lvl}_$d"
      case (Some(d), None)      => s"${TABLE_GEOSIM_VOXEL_IDX_UNIFIED_TEMP}_$d"
      case (None, Some(lvl))    => s"geosim_voxel_idx_unified_temp_L$lvl"
      case (None, None)         => TABLE_GEOSIM_VOXEL_IDX_UNIFIED_TEMP
    }
  }

  def geoSimVoxelTempBucketIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"${TABLE_GEOSIM_VOXEL_IDX_TEMP_BUCKET}_$d").getOrElse(TABLE_GEOSIM_VOXEL_IDX_TEMP_BUCKET)

  def geoSimVoxelVxBucketIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"geosim_voxel_idx_vx_bucket_$d").getOrElse("geosim_voxel_idx_vx_bucket")

  def geoSimVoxelVyBucketIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"geosim_voxel_idx_vy_bucket_$d").getOrElse("geosim_voxel_idx_vy_bucket")

  def geoSimVoxelVzBucketIdxTableName(dataset: Option[String]): String =
    dataset.map(d => s"geosim_voxel_idx_vz_bucket_$d").getOrElse("geosim_voxel_idx_vz_bucket")

  def geoSimVoxelVxUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_voxel_idx_unified_vx_L${lvl}_$d"
      case (Some(d), None)      => s"geosim_voxel_idx_unified_vx_$d"
      case (None, Some(lvl))    => s"geosim_voxel_idx_unified_vx_L$lvl"
      case (None, None)         => "geosim_voxel_idx_unified_vx"
    }
  }

  def geoSimVoxelVyUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_voxel_idx_unified_vy_L${lvl}_$d"
      case (Some(d), None)      => s"geosim_voxel_idx_unified_vy_$d"
      case (None, Some(lvl))    => s"geosim_voxel_idx_unified_vy_L$lvl"
      case (None, None)         => "geosim_voxel_idx_unified_vy"
    }
  }

  def geoSimVoxelVzUnifiedIdxTableName(dataset: Option[String], unifiedLevel: Option[Int]): String = {
    (dataset, unifiedLevel) match {
      case (Some(d), Some(lvl)) => s"geosim_voxel_idx_unified_vz_L${lvl}_$d"
      case (Some(d), None)      => s"geosim_voxel_idx_unified_vz_$d"
      case (None, Some(lvl))    => s"geosim_voxel_idx_unified_vz_L$lvl"
      case (None, None)         => "geosim_voxel_idx_unified_vz"
    }
  }

  def createTables(connection: Connection): Unit = {
    val admin = connection.getAdmin

    try {
      createTableIfNotExists(admin, TABLE_DATA, hexSplits(16))
      createTableIfNotExists(admin, TABLE_IDX_SENSOR, sensorIdSplits())
      createTableIfNotExists(admin, TABLE_IDX_TIME, timeBucketSplits())
      createTableIfNotExists(admin, TABLE_IDX_SPATIAL, z3dSplits(16))
      createTableIfNotExists(admin, TABLE_IDX_UNIFIED, unifiedSplits(16))

      println("Tables created/verified:")
      println(s"  - $TABLE_DATA")
      println(s"  - $TABLE_IDX_SENSOR")
      println(s"  - $TABLE_IDX_TIME")
      println(s"  - $TABLE_IDX_SPATIAL")
      println(s"  - $TABLE_IDX_UNIFIED")
    } finally {
      admin.close()
    }
  }

  def createTablesForDataset(
    connection: Connection,
    dataset: Option[String],
    unifiedLevel: Option[Int]
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val dataTable = dataTableName(dataset)
      val sensorIdxTable = sensorIdxTableName(dataset)
      val timeIdxTable = timeIdxTableName(dataset)
      val spatialIdxTable = spatialIdxTableName(dataset)
      val unifiedIdxTable = unifiedIdxTableName(dataset, unifiedLevel)

      createTableIfNotExists(admin, dataTable, hexSplits(16))
      createTableIfNotExists(admin, sensorIdxTable, sensorIdSplits())
      createTableIfNotExists(admin, timeIdxTable, timeBucketSplits())
      createTableIfNotExists(admin, spatialIdxTable, z3dSplits(16))
      createTableIfNotExists(admin, unifiedIdxTable, unifiedSplits(16))

      println("Tables created/verified for dataset:")
      println(s"  dataset: ${dataset.getOrElse("(default)")}")
      println(s"  unifiedLevel: ${unifiedLevel.getOrElse("(default)")}")
      println(s"  - $dataTable")
      println(s"  - $sensorIdxTable")
      println(s"  - $timeIdxTable")
      println(s"  - $spatialIdxTable")
      println(s"  - $unifiedIdxTable")
    } finally {
      admin.close()
    }
  }

  /**
   * Creates Volume tables (meta table + brick table + unified index table)
   *
   * @param connection HBase connection
   * @param datasetOpt Optional dataset ID
   * @param unifiedLevelOpt Optional unified index level
   */
  def createVolumeTablesForDataset(
    connection: Connection,
    datasetOpt: Option[String],
    unifiedLevelOpt: Option[Int] = None
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val metaTable = volumeMetaTableName(datasetOpt)
      val brickTable = volumeBrickTableName(datasetOpt)
      val unifiedIdxTable = volumeUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)

      // Create meta table
      createTableIfNotExists(admin, metaTable, hexSplits(16))

      // Create brick table
      createTableIfNotExists(admin, brickTable, hexSplits(16))

      // Create volume unified index table
      val numRegions = 16
      val splitKeys = volumeUnifiedSplits(numRegions)
      println(s"[INFO] Preparing to create volume unified index table:")
      println(s"       Table name: $unifiedIdxTable")
      println(s"       numRegions: $numRegions")
      println(s"       Split keys count: ${splitKeys.length}")
      if (splitKeys.isEmpty) {
        println(s"       Will create unified table without pre-splitting (auto split)")
      }
      createTableIfNotExists(admin, unifiedIdxTable, splitKeys)

      println("Volume tables created/verified:")
      println(s"  dataset: ${datasetOpt.getOrElse("(default)")}")
      println(s"  unifiedLevel: ${unifiedLevelOpt.getOrElse("(default)")}")
      println(s"  - $metaTable")
      println(s"  - $brickTable")
      println(s"  - $unifiedIdxTable")
    } finally {
      admin.close()
    }
  }

  /**
   * Creates GeoSim point tables (data table + incremental index tables + unified index table)
   *
   * @param connection HBase connection
   * @param datasetOpt Optional dataset ID
   * @param unifiedLevelOpt Optional unified index level
   */
  def createGeoSimPointTablesForDataset(
    connection: Connection,
    datasetOpt: Option[String],
    unifiedLevelOpt: Option[Int] = None
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val dataTable = geoSimPointDataTableName(datasetOpt)
      val simIdxTable = geoSimPointSimIdxTableName(datasetOpt)
      val timeIdxTable = geoSimPointTimeIdxTableName(datasetOpt)
      val spatialIdxTable = geoSimPointSpatialIdxTableName(datasetOpt)
      val unifiedIdxTable = geoSimPointUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)

      // Create GeoSim point data table
      createTableIfNotExists(admin, dataTable, hexSplits(16))

      // Create GeoSim point sim_id index table
      createTableIfNotExists(admin, simIdxTable, hexSplits(16))

      // Create GeoSim point time index table
      createTableIfNotExists(admin, timeIdxTable, timeBucketSplits())

      // Create GeoSim point spatial index table
      createTableIfNotExists(admin, spatialIdxTable, z3dSplits(16))

      // Create GeoSim point unified index table
      val numRegions = 16
      val splitKeys = unifiedSplits(numRegions)
      println(s"[INFO] Preparing to create GeoSim point unified index table:")
      println(s"       Table name: $unifiedIdxTable")
      println(s"       numRegions: $numRegions")
      println(s"       Split keys count: ${splitKeys.length}")
      if (splitKeys.isEmpty) {
        println(s"       Will create unified table without pre-splitting (auto split)")
      }
      createTableIfNotExists(admin, unifiedIdxTable, splitKeys)

      println("GeoSim point tables created/verified:")
      println(s"  dataset: ${datasetOpt.getOrElse("(default)")}")
      println(s"  unifiedLevel: ${unifiedLevelOpt.getOrElse("(default)")}")
      println(s"  - $dataTable")
      println(s"  - $simIdxTable")
      println(s"  - $timeIdxTable")
      println(s"  - $spatialIdxTable")
      println(s"  - $unifiedIdxTable")
    } finally {
      admin.close()
    }
  }

  /**
   * Creates GeoSim point temperature bucket incremental index table
   *
   * @param connection HBase connection
   * @param datasetOpt Optional dataset ID
   */
  def createGeoSimPointTempBucketIdxTableForDataset(
    connection: Connection,
    datasetOpt: Option[String]
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val tempBucketIdxTable = geoSimPointTempBucketIdxTableName(datasetOpt)

      // Create GeoSim point temperature bucket index table
      createTableIfNotExists(admin, tempBucketIdxTable, hexSplits(16))

      println("GeoSim point temp bucket index table created/verified:")
      println(s"  dataset: ${datasetOpt.getOrElse("(default)")}")
      println(s"  - $tempBucketIdxTable")
    } finally {
      admin.close()
    }
  }

  def createGeoSimPointVelocityBucketTablesForDataset(
    connection: Connection,
    datasetOpt: Option[String]
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val vxBucketIdxTable = geoSimPointVxBucketIdxTableName(datasetOpt)
      val vyBucketIdxTable = geoSimPointVyBucketIdxTableName(datasetOpt)
      val vzBucketIdxTable = geoSimPointVzBucketIdxTableName(datasetOpt)

      // Create GeoSim point velocity bucket index tables
      createTableIfNotExists(admin, vxBucketIdxTable, hexSplits(16))
      createTableIfNotExists(admin, vyBucketIdxTable, hexSplits(16))
      createTableIfNotExists(admin, vzBucketIdxTable, hexSplits(16))

      println("GeoSim point velocity bucket index tables created/verified:")
      println(s"  dataset: ${datasetOpt.getOrElse("(default)")}")
      println(s"  - $vxBucketIdxTable")
      println(s"  - $vyBucketIdxTable")
      println(s"  - $vzBucketIdxTable")
    } finally {
      admin.close()
    }
  }

  /**
   * Creates GeoSim Voxel incremental index tables (sim_id + time + spatial)
   *
   * @param connection HBase connection
   * @param datasetOpt Optional dataset ID
   * @param unifiedLevelOpt Optional unified index level (for spatial table)
   */
  def createGeoSimVoxelIncrementalTablesForDataset(
    connection: Connection,
    datasetOpt: Option[String],
    unifiedLevelOpt: Option[Int] = None
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val simIdxTable = geoSimVoxelSimIdxTableName(datasetOpt)
      val timeIdxTable = geoSimVoxelTimeIdxTableName(datasetOpt)
      val spatialIdxTable = geoSimVoxelSpatialIdxTableName(datasetOpt, unifiedLevelOpt)

      // Create GeoSim Voxel sim_id index table
      createTableIfNotExists(admin, simIdxTable, hexSplits(16))

      // Create GeoSim Voxel time index table
      createTableIfNotExists(admin, timeIdxTable, timeBucketSplits())

      // Create GeoSim Voxel spatial index table (using z3dSplits, rowkey has 8-byte long prefix)
      createTableIfNotExists(admin, spatialIdxTable, z3dSplits(16))

      println("GeoSim voxel incremental tables created/verified:")
      println(s"  dataset: ${datasetOpt.getOrElse("(default)")}")
      println(s"  unifiedLevel: ${unifiedLevelOpt.getOrElse("(default)")}")
      println(s"  - $simIdxTable")
      println(s"  - $timeIdxTable")
      println(s"  - $spatialIdxTable")
    } finally {
      admin.close()
    }
  }

  /**
   * Creates GeoSim Voxel temperature unified index table
   *
   * @param connection HBase connection
   * @param datasetOpt Optional dataset ID
   * @param unifiedLevelOpt Optional unified index level
   */
  def createGeoSimVoxelTempUnifiedTableForDataset(
    connection: Connection,
    datasetOpt: Option[String],
    unifiedLevelOpt: Option[Int] = None
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val tempUnifiedIdxTable = geoSimVoxelTempUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)

      // Create GeoSim Voxel temperature unified index table
      val splitKeys = unifiedSplits(16)
      createTableIfNotExists(admin, tempUnifiedIdxTable, splitKeys)

      println(s"[GeoSim-Voxel] Created/verified table: $tempUnifiedIdxTable")
    } finally {
      admin.close()
    }
  }

  /**
   * Creates GeoSim Voxel temperature bucket incremental index table
   *
   * @param connection HBase connection
   * @param datasetOpt Optional dataset ID
   */
  def createGeoSimVoxelTempBucketIdxTableForDataset(
    connection: Connection,
    datasetOpt: Option[String]
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val tempBucketIdxTable = geoSimVoxelTempBucketIdxTableName(datasetOpt)

      // Create GeoSim Voxel temperature bucket index table
      createTableIfNotExists(admin, tempBucketIdxTable, hexSplits(16))

      println(s"[GeoSim-Voxel] Created/verified table: $tempBucketIdxTable")
    } finally {
      admin.close()
    }
  }

  def createGeoSimVoxelVelocityBucketTablesForDataset(
    connection: Connection,
    datasetOpt: Option[String]
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val vxBucketIdxTable = geoSimVoxelVxBucketIdxTableName(datasetOpt)
      val vyBucketIdxTable = geoSimVoxelVyBucketIdxTableName(datasetOpt)
      val vzBucketIdxTable = geoSimVoxelVzBucketIdxTableName(datasetOpt)

      // Create GeoSim Voxel velocity bucket index tables
      createTableIfNotExists(admin, vxBucketIdxTable, hexSplits(16))
      createTableIfNotExists(admin, vyBucketIdxTable, hexSplits(16))
      createTableIfNotExists(admin, vzBucketIdxTable, hexSplits(16))

      println("GeoSim voxel velocity bucket index tables created/verified:")
      println(s"  dataset: ${datasetOpt.getOrElse("(default)")}")
      println(s"  - $vxBucketIdxTable")
      println(s"  - $vyBucketIdxTable")
      println(s"  - $vzBucketIdxTable")
    } finally {
      admin.close()
    }
  }

  def createGeoSimVoxelVelocityUnifiedTablesForDataset(
    connection: Connection,
    datasetOpt: Option[String],
    unifiedLevelOpt: Option[Int] = None
  ): Unit = {
    val admin = connection.getAdmin

    try {
      val vxIdxTable = geoSimVoxelVxUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)
      val vyIdxTable = geoSimVoxelVyUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)
      val vzIdxTable = geoSimVoxelVzUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)

      // Create GeoSim Voxel velocity unified index tables
      createTableIfNotExists(admin, vxIdxTable, unifiedSplits(16))
      createTableIfNotExists(admin, vyIdxTable, unifiedSplits(16))
      createTableIfNotExists(admin, vzIdxTable, unifiedSplits(16))

      println(s"[GeoSim-Voxel] Created/verified table: $vxIdxTable")
      println(s"[GeoSim-Voxel] Created/verified table: $vyIdxTable")
      println(s"[GeoSim-Voxel] Created/verified table: $vzIdxTable")
    } finally {
      admin.close()
    }
  }

  /**
   * Drops all tables for a dataset (including main table, incremental indexes, and unified indexes)
   *
   * @param connection HBase connection
   * @param dataset Dataset ID (only alphanumeric and underscore allowed)
   * @param force Whether to actually execute deletion; false means dry-run preview only
   * @throws IllegalArgumentException If dataset format is invalid
   */
  def dropDataset(connection: Connection, dataset: String, force: Boolean): Unit = {
    // 1. Validate dataset format
    if (dataset == null || dataset.isEmpty) {
      throw new IllegalArgumentException("Dataset cannot be null or empty")
    }

    if (!dataset.matches("^[A-Za-z0-9_]+$")) {
      throw new IllegalArgumentException(
        s"Dataset ID '$dataset' contains invalid characters. Only alphanumeric and underscore allowed."
      )
    }

    val admin = connection.getAdmin

    try {
      // 2. List all existing tables and filter those belonging to this dataset
      val allTableNames = admin.listTableNames()
      val tablesToDrop = scala.collection.mutable.Set[String]()

      // Exact match incremental index tables
      val incrementalTables = Seq(
        s"${TABLE_DATA}_$dataset",
        s"${TABLE_IDX_SENSOR}_$dataset",
        s"${TABLE_IDX_TIME}_$dataset",
        s"${TABLE_IDX_SPATIAL}_$dataset"
      )

      // Check if incremental index tables exist
      incrementalTables.foreach { tableName =>
        if (allTableNames.exists(_.getNameAsString == tableName)) {
          tablesToDrop += tableName
        }
      }

      // Exact match GeoSim point tables
      val geosimPointExactTables = Seq(
        s"geosim_point_data_$dataset",
        s"geosim_point_idx_sim_id_$dataset",
        s"geosim_point_idx_time_$dataset",
        s"geosim_point_idx_spatial_blk_$dataset",
        s"geosim_point_idx_unified_$dataset"
      )

      geosimPointExactTables.foreach { tableName =>
        if (allTableNames.exists(_.getNameAsString == tableName)) {
          tablesToDrop += tableName
        }
      }

      // Exact match GeoSim voxel tables
      val geosimVoxelExactTables = Seq(
        s"volume_model_meta_$dataset",
        s"volume_brick_data_$dataset",
        s"geosim_voxel_idx_sim_id_$dataset",
        s"geosim_voxel_idx_time_$dataset",
        s"volume_idx_unified_$dataset"
      )

      geosimVoxelExactTables.foreach { tableName =>
        if (allTableNames.exists(_.getNameAsString == tableName)) {
          tablesToDrop += tableName
        }
      }

      // Prefix and suffix match unified index tables (idx_unified_L*_<dataset>)
      val unifiedPrefix = "idx_unified_L"
      val unifiedSuffix = s"_$dataset"
      allTableNames.foreach { tn =>
        val tableName = tn.getNameAsString
        if (tableName.startsWith(unifiedPrefix) && tableName.endsWith(unifiedSuffix)) {
          tablesToDrop += tableName
        }
      }

      // Prefix and suffix match GeoSim point tables (L* level)
      val geosimPointUnifiedPrefix = "geosim_point_idx_unified_L"
      val geosimPointSpatialPrefix = "geosim_point_idx_spatial_blk_L"
      allTableNames.foreach { tn =>
        val tableName = tn.getNameAsString
        if ((tableName.startsWith(geosimPointUnifiedPrefix) || tableName.startsWith(geosimPointSpatialPrefix)) && tableName.endsWith(unifiedSuffix)) {
          tablesToDrop += tableName
        }
      }

      // Prefix and suffix match GeoSim voxel tables (L* level)
      val geosimVoxelSpatialPrefix = "geosim_voxel_idx_spatial_L"
      val volumeUnifiedPrefix = "volume_idx_unified_L"
      allTableNames.foreach { tn =>
        val tableName = tn.getNameAsString
        if ((tableName.startsWith(geosimVoxelSpatialPrefix) || tableName.startsWith(volumeUnifiedPrefix)) && tableName.endsWith(unifiedSuffix)) {
          tablesToDrop += tableName
        }
      }

      // Exact match GeoSim point temperature index tables
      val geosimPointTempExactTables = Seq(
        s"geosim_point_idx_temp_bucket_$dataset",
        s"geosim_point_idx_unified_temp_$dataset"
      )

      geosimPointTempExactTables.foreach { tableName =>
        if (allTableNames.exists(_.getNameAsString == tableName)) {
          tablesToDrop += tableName
        }
      }

      // Exact match GeoSim voxel temperature index tables
      val geosimVoxelTempExactTables = Seq(
        s"geosim_voxel_idx_temp_bucket_$dataset",
        s"geosim_voxel_idx_unified_temp_$dataset"
      )

      geosimVoxelTempExactTables.foreach { tableName =>
        if (allTableNames.exists(_.getNameAsString == tableName)) {
          tablesToDrop += tableName
        }
      }

      // Exact match GeoSim point velocity index tables
      val geosimPointVelExactTables = Seq(
        s"geosim_point_idx_unified_vx_$dataset",
        s"geosim_point_idx_unified_vy_$dataset",
        s"geosim_point_idx_unified_vz_$dataset",
        s"geosim_point_idx_vx_bucket_$dataset",
        s"geosim_point_idx_vy_bucket_$dataset",
        s"geosim_point_idx_vz_bucket_$dataset"
      )

      geosimPointVelExactTables.foreach { tableName =>
        if (allTableNames.exists(_.getNameAsString == tableName)) {
          tablesToDrop += tableName
        }
      }

      // Exact match GeoSim voxel velocity index tables
      val geosimVoxelVelExactTables = Seq(
        s"geosim_voxel_idx_unified_vx_$dataset",
        s"geosim_voxel_idx_unified_vy_$dataset",
        s"geosim_voxel_idx_unified_vz_$dataset",
        s"geosim_voxel_idx_vx_bucket_$dataset",
        s"geosim_voxel_idx_vy_bucket_$dataset",
        s"geosim_voxel_idx_vz_bucket_$dataset"
      )

      geosimVoxelVelExactTables.foreach { tableName =>
        if (allTableNames.exists(_.getNameAsString == tableName)) {
          tablesToDrop += tableName
        }
      }

      // Prefix and suffix match GeoSim point temperature unified index tables (L* level)
      val geosimPointTempUnifiedPrefix = "geosim_point_idx_unified_temp_L"
      allTableNames.foreach { tn =>
        val tableName = tn.getNameAsString
        if (tableName.startsWith(geosimPointTempUnifiedPrefix) && tableName.endsWith(unifiedSuffix)) {
          tablesToDrop += tableName
        }
      }

      // Prefix and suffix match GeoSim voxel temperature unified index tables (L* level)
      val geosimVoxelTempUnifiedPrefix = "geosim_voxel_idx_unified_temp_L"
      allTableNames.foreach { tn =>
        val tableName = tn.getNameAsString
        if (tableName.startsWith(geosimVoxelTempUnifiedPrefix) && tableName.endsWith(unifiedSuffix)) {
          tablesToDrop += tableName
        }
      }

      // Prefix and suffix match GeoSim point velocity unified index tables (L* level)
      val geosimPointVelUnifiedVxPrefix = "geosim_point_idx_unified_vx_L"
      val geosimPointVelUnifiedVyPrefix = "geosim_point_idx_unified_vy_L"
      val geosimPointVelUnifiedVzPrefix = "geosim_point_idx_unified_vz_L"
      allTableNames.foreach { tn =>
        val tableName = tn.getNameAsString
        if ((tableName.startsWith(geosimPointVelUnifiedVxPrefix) || 
             tableName.startsWith(geosimPointVelUnifiedVyPrefix) || 
             tableName.startsWith(geosimPointVelUnifiedVzPrefix)) && 
            tableName.endsWith(unifiedSuffix)) {
          tablesToDrop += tableName
        }
      }

      // Prefix and suffix match GeoSim voxel velocity unified index tables (L* level)
      val geosimVoxelVelUnifiedVxPrefix = "geosim_voxel_idx_unified_vx_L"
      val geosimVoxelVelUnifiedVyPrefix = "geosim_voxel_idx_unified_vy_L"
      val geosimVoxelVelUnifiedVzPrefix = "geosim_voxel_idx_unified_vz_L"
      allTableNames.foreach { tn =>
        val tableName = tn.getNameAsString
        if ((tableName.startsWith(geosimVoxelVelUnifiedVxPrefix) || 
             tableName.startsWith(geosimVoxelVelUnifiedVyPrefix) || 
             tableName.startsWith(geosimVoxelVelUnifiedVzPrefix)) && 
            tableName.endsWith(unifiedSuffix)) {
          tablesToDrop += tableName
        }
      }

      // 3. If no tables found
      if (tablesToDrop.isEmpty) {
        println(s"[Drop-Dataset] No tables found for dataset '$dataset'")
        return
      }

      // 4. Dry-run mode or actual deletion
      if (!force) {
        // Dry-run preview
        println(s"[Drop-Dataset] Dry-run mode for dataset '$dataset'. Tables to be dropped:")
        tablesToDrop.toSeq.sorted.foreach { tableName =>
          println(s"  - $tableName")
        }
        println("[Drop-Dataset] (Re-run with --force to actually delete these tables.)")
      } else {
        // Actually execute deletion
        var droppedCount = 0
        tablesToDrop.toSeq.sorted.foreach { tableName =>
          val tn = TableName.valueOf(tableName)
          try {
            // If table is enabled, disable it first
            if (admin.isTableEnabled(tn)) {
              println(s"[Drop-Dataset] Disabling table: $tableName")
              admin.disableTable(tn)
            }
            // Drop table
            println(s"[Drop-Dataset] Dropping table: $tableName")
            admin.deleteTable(tn)
            println(s"[Drop-Dataset] Table dropped: $tableName")
            droppedCount += 1
          } catch {
            case e: Exception =>
              println(s"[Drop-Dataset] Error dropping table '$tableName': ${e.getMessage}")
          }
        }

        // Summary
        println(s"[Drop-Dataset] Finished dropping dataset '$dataset'. Total tables dropped: $droppedCount")
      }

    } finally {
      admin.close()
    }
  }

  private def createTableIfNotExists(admin: Admin, tableName: String,
                                      splitKeys: Array[Array[Byte]]): Unit = {
    val tn = TableName.valueOf(tableName)
    
    if (!admin.tableExists(tn)) {
      val cfDesc = ColumnFamilyDescriptorBuilder.newBuilder(CF_BYTES)
        .setMaxVersions(1)
        .build()
      
      val tableDesc = TableDescriptorBuilder.newBuilder(tn)
        .setColumnFamily(cfDesc)
        .build()
      
      if (splitKeys.nonEmpty) {
        admin.createTable(tableDesc, splitKeys)
      } else {
        admin.createTable(tableDesc)
      }
      
      println(s"  Created table: $tableName")
    } else {
      println(s"  Table exists: $tableName")
    }
  }
  
  private def hexSplits(numRegions: Int): Array[Array[Byte]] = {
    val chars = "0123456789abcdef"
    chars.take(numRegions - 1).map(c => Bytes.toBytes(c.toString)).toArray
  }
  
  private def sensorIdSplits(): Array[Array[Byte]] = {
    (0 to 4).map(i => Bytes.toBytes(f"SENS_0$i")).toArray
  }
  
  private def timeBucketSplits(): Array[Array[Byte]] = {
    Seq("20251007", "20251014", "20251021", "20251028")
      .map(Bytes.toBytes)
      .toArray
  }
  
  private def z3dSplits(numRegions: Int): Array[Array[Byte]] = {
    val maxZ3D = 1L << 62
    val step = maxZ3D / numRegions
    (1 until numRegions).map(i => Bytes.toBytes(i * step)).toArray
  }

  def unifiedSplits(numRegions: Int): Array[Array[Byte]] = {
    // Unified index row key prefix: [attrKind(1B)] + [attrHash(4B BE)] + [Z3D(8B BE)] + ...
    // To avoid duplication, we split based on attribute hash values
    // Simple strategy: for each split point, create a unique hash value
    (1 until numRegions).map { i =>
      val hashValue = (i * (Int.MaxValue.toLong / numRegions)).toInt
      val z3dValue = (i.toLong * (1L << 62)) / numRegions
      val combined = ByteBuffer.allocate(13)  // 1 + 4 + 8 = 13 bytes
      combined.put(0x01.toByte)  // Attribute type (1 byte)
      combined.putInt(hashValue)  // Attribute hash (4 bytes)
      combined.putLong(z3dValue)  // Z3D (8 bytes)
      combined.array()
    }.toArray
  }

  /**
   * Split keys for Volume unified index table
   *
   * RowKey structure: [attrKind(1B)] + [attrHash(4B, BE)] + [zCell(8B, BE)] + [dayBucket(4B, BE)] + [timeOfDay(4B, BE)] + '_' + [blockKey(UTF-8)]
   *
   * Important notes:
   * - Unified index rowkey sort prefix is [attrKind][attrHash][zCell]...
   * - Since attrHash precedes zCell, pre-splitting must prioritize splitting by attrHash, otherwise it will be ineffective or cause hotspots
   * - If splitting by zCell but attrHash is fixed at 0, real data (hash>0) will tend to fall into the last region
   *
   * @param numRegions Number of regions
   * @return Array of split keys (returns empty array if generation fails)
   */
  private def volumeUnifiedSplits(numRegions: Int): Array[Array[Byte]] = {
    // Volume unified index row key prefix: [attrKind(1B)] + [attrHash(4B BE)] + [zCell(8B BE)] + ...
    // Split evenly by attrHash (4 bytes big-endian), zCell fixed at 0
    val rawSplits = (1 until numRegions).map { i =>
      // Calculate evenly distributed hash split points (covering [0, Int.MaxValue])
      var hashValue = ((i.toLong * Int.MaxValue.toLong) / numRegions).toInt
      
      // Guard: when hashValue == 0, set it to 1 (ensure split point doesn't duplicate minimum key)
      if (hashValue == 0) {
        hashValue = 1
      }
      
      // Construct 13-byte split key (consistent with rowkey sort prefix)
      val combined = ByteBuffer.allocate(13)  // 1 + 4 + 8 = 13 bytes
      combined.put(0x01.toByte)  // attrKind: ByModelType (1 byte)
      combined.putInt(hashValue)  // attrHash: split point (4 bytes, big-endian)
      combined.putLong(0L)  // zCell: fixed at 0 (8 bytes, big-endian)
      combined.array()
    }.toArray
    
    // Validate split keys are unique and sorted
    validateAndSortSplits(rawSplits, numRegions)
  }
  
  /**
   * Validates and sorts split keys
   *
   * @param splits Raw split keys
   * @param numRegions Expected number of regions
   * @return Validated split keys (returns empty array if validation fails)
   */
  private def validateAndSortSplits(splits: Array[Array[Byte]], numRegions: Int): Array[Array[Byte]] = {
    if (splits.isEmpty) {
      return splits
    }
    
    // Sort lexicographically
    val sortedSplits = splits.sortWith((a, b) => compareBytes(a, b) < 0)
    
    // Check for duplicates
    val uniqueSplits = removeDuplicates(sortedSplits)
    
    // If duplicates exist or count is wrong, print warning and return empty array
    if (uniqueSplits.length != splits.length) {
      val duplicateCount = splits.length - uniqueSplits.length
      println(s"[WARN] Volume unified split keys found duplicates:")
      println(s"       Expected count: ${numRegions - 1}, Actual count: ${splits.length}, Duplicate count: $duplicateCount")
      println(s"       First 3 split keys: ${uniqueSplits.take(3).map(bytesToHex).mkString(", ")}")
      println(s"       Last 3 split keys: ${uniqueSplits.takeRight(3).map(bytesToHex).mkString(", ")}")
      println(s"       Will create unified table without pre-splitting (auto split)")
      return Array.empty[Array[Byte]]
    }
    
    // Check if strictly increasing
    for (i <- 1 until uniqueSplits.length) {
      if (compareBytes(uniqueSplits(i - 1), uniqueSplits(i)) >= 0) {
        println(s"[WARN] Volume unified split keys are not strictly increasing:")
        println(s"       split[$i-1] = ${bytesToHex(uniqueSplits(i - 1))}")
        println(s"       split[$i] = ${bytesToHex(uniqueSplits(i))}")
        println(s"       Will create unified table without pre-splitting (auto split)")
        return Array.empty[Array[Byte]]
      }
    }
    
    uniqueSplits
  }
  
  /**
   * Removes duplicate split keys
   *
   * @param splits Input split keys
   * @return Deduplicated split keys
   */
  private def removeDuplicates(splits: Array[Array[Byte]]): Array[Array[Byte]] = {
    if (splits.isEmpty) {
      return splits
    }
    
    val result = scala.collection.mutable.ArrayBuffer[Array[Byte]]()
    val seen = scala.collection.mutable.Set[String]()
    
    splits.foreach { split =>
      val hex = bytesToHex(split)
      if (!seen.contains(hex)) {
        seen += hex
        result += split
      }
    }
    
    result.toArray
  }
  
  /**
   * Compares two byte arrays lexicographically
   *
   * @param a First byte array
   * @param b Second byte array
   * @return Negative if a < b, 0 if a == b, positive if a > b
   */
  private def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
    val minLen = math.min(a.length, b.length)
    var i = 0
    while (i < minLen) {
      val cmp = (a(i) & 0xFF) - (b(i) & 0xFF)
      if (cmp != 0) {
        return cmp
      }
      i += 1
    }
    a.length - b.length
  }
  
  /**
   * Converts byte array to hexadecimal string (for debugging)
   *
   * @param bytes Byte array
   * @return Hexadecimal string
   */
  private def bytesToHex(bytes: Array[Byte]): String = {
    bytes.map(b => f"$b%02x").mkString("\\x")
  }
}