// src/main/scala/ingest/SensorDataIngestJob.scala
package ingest

import model.SensorRecord
import index.{Z3DEncoder, TimeBucket, UnifiedIndexKey}
import storage.HBaseTableManager

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Put, ConnectionFactory, BufferedMutator, BufferedMutatorParams}
import org.apache.hadoop.hbase.util.Bytes

object SensorDataIngestJob {
  
  // Debug counter
  @volatile var debugCount = 0
  
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: SensorDataIngestJob <input_path> <zk_quorum> <batch_size> [--dataset <id>] [--indexes <list>] [--unified-level <int>]")
      System.exit(1)
    }

    val inputPath = args(0)
    val zkQuorum = args(1)
    val batchSize = args(2).toInt

    // Parse optional arguments: --key value format
    var datasetOpt: Option[String] = None
    var indexesOpt: Set[String] = Set("incremental", "unified")  // Default: write both
    var unifiedLevelOpt: Option[Int] = None

    var i = 3
    while (i < args.length) {
      val key = args(i)
      val value = if (i + 1 < args.length) args(i + 1) else ""

      key match {
        case "--dataset" =>
          datasetOpt = Some(value)
          i += 2
        case "--indexes" =>
          indexesOpt = value.split(",").map(_.trim).toSet
          i += 2
        case "--unified-level" =>
          try {
            unifiedLevelOpt = Some(value.toInt)
            i += 2
          } catch {
            case _: NumberFormatException =>
              println(s"[Warn] Invalid --unified-level value: $value, ignored")
              i += 2
          }
        case _ =>
          i += 1
      }
    }

    // Validate indexes value
    val validIndexes = Set("incremental", "unified")
    if (!indexesOpt.subsetOf(validIndexes)) {
      println(s"[Warn] Invalid index types, using defaults. Valid: ${validIndexes.mkString(", ")}")
      indexesOpt = Set("incremental", "unified")
    }

    // Print configuration information
    println("=" * 60)
    println("[Config] SensorDataIngestJob")
    println(s"  dataset: ${datasetOpt.getOrElse("(default)")}")
    println(s"  indexes: ${indexesOpt.mkString(", ")}")
    println(s"  unifiedLevel: ${unifiedLevelOpt.getOrElse("(default)")}")
    println("=" * 60)

    // Print Z3DEncoder configuration
    println("=" * 60)
    println("Z3DEncoder Configuration:")
    println(s"  LON_MIN = ${Z3DEncoder.LON_MIN}")
    println(s"  LON_MAX = ${Z3DEncoder.LON_MAX}")
    println(s"  LAT_MIN = ${Z3DEncoder.LAT_MIN}")
    println(s"  LAT_MAX = ${Z3DEncoder.LAT_MAX}")
    println(s"  ALT_MIN = ${Z3DEncoder.ALT_MIN}")
    println(s"  ALT_MAX = ${Z3DEncoder.ALT_MAX}")
    println("=" * 60)

    // Test encoding
    val testLon = 114.353816
    val testLat = 23.977158
    val testAlt = 102.9519
    val testZ3D = Z3DEncoder.encode(testLon, testLat, testAlt)
    println(s"Test encoding: ($testLon, $testLat, $testAlt)")
    println(f"  Z3D = $testZ3D (0x${testZ3D}%016x)")
    println("=" * 60)

    val spark = SparkSession.builder()
      .appName("SensorDataIngest")
      .getOrCreate()

    try {
      val connection = HBaseTableManager.createConnection(zkQuorum)
      try {
        HBaseTableManager.createTablesForDataset(connection, datasetOpt, unifiedLevelOpt)
      } finally {
        connection.close()
      }

      val records = spark.sparkContext
        .textFile(inputPath)
        .flatMap(SensorRecord.fromCsvLine)

      records.foreachPartition { partition =>
        writePartitionToHBase(partition, zkQuorum, batchSize, datasetOpt, indexesOpt, unifiedLevelOpt)
      }

      println("Data ingestion completed!")

    } finally {
      spark.stop()
    }
  }
  
  private def writePartitionToHBase(partition: Iterator[SensorRecord],
                                     zkQuorum: String,
                                     batchSize: Int,
                                     dataset: Option[String],
                                     indexes: Set[String],
                                     unifiedLevel: Option[Int]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    val connection = ConnectionFactory.createConnection(conf)

    // Determine which mutators to create based on indexes
    val mutatorData = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.dataTableName(dataset)))
    } else None

    val mutatorSensor = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.sensorIdxTableName(dataset)))
    } else None

    val mutatorTime = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.timeIdxTableName(dataset)))
    } else None

    val mutatorSpatial = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.spatialIdxTableName(dataset)))
    } else None

    val mutatorUnified = if (indexes.contains("unified")) {
      Some(createMutator(connection, HBaseTableManager.unifiedIdxTableName(dataset, unifiedLevel)))
    } else None

    val CF = HBaseTableManager.CF_BYTES

    try {
      var count = 0

      partition.foreach { record =>
        // Main data
        if (mutatorData.isDefined) {
          val dataPut = new Put(Bytes.toBytes(record.rowKey))
          dataPut.addColumn(CF, Bytes.toBytes("time"), Bytes.toBytes(record.time))
          dataPut.addColumn(CF, Bytes.toBytes("sensor_id"), Bytes.toBytes(record.sensorId))
          dataPut.addColumn(CF, Bytes.toBytes("lon"), Bytes.toBytes(record.longitude))
          dataPut.addColumn(CF, Bytes.toBytes("lat"), Bytes.toBytes(record.latitude))
          dataPut.addColumn(CF, Bytes.toBytes("alt"), Bytes.toBytes(record.altitude))
          dataPut.addColumn(CF, Bytes.toBytes("type"), Bytes.toBytes(record.sensorType))
          mutatorData.get.mutate(dataPut)
        }

        // Sensor index
        if (mutatorSensor.isDefined) {
          val sensorKey = s"${record.sensorId}_${record.rowKey}"
          val sensorPut = new Put(Bytes.toBytes(sensorKey))
          sensorPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(record.rowKey))
          mutatorSensor.get.mutate(sensorPut)
        }

        // Time index
        if (mutatorTime.isDefined) {
          val bucket = TimeBucket.dayBucket(record.time)
          val timeKey = f"${bucket}_${record.time}%013d_${record.rowKey}"
          val timePut = new Put(Bytes.toBytes(timeKey))
          timePut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(record.rowKey))
          mutatorTime.get.mutate(timePut)
        }

        // Spatial index - Z3D encoding
        if (mutatorSpatial.isDefined) {
          val z3d = Z3DEncoder.encode(record.longitude, record.latitude, record.altitude)
          val z3dBytes = Z3DEncoder.longToBytes(z3d)
          val spatialKeyBytes = z3dBytes ++ Bytes.toBytes(s"_${record.rowKey}")

          // Print first 5 debug messages
          if (count < 5) {
            println(s"[DEBUG] Record $count:")
            println(s"  Coords: (${record.longitude}, ${record.latitude}, ${record.altitude})")
            println(f"  Z3D: $z3d (0x${z3d}%016x)")
            println(s"  SpatialKey (hex): ${z3dBytes.map(b => f"${b}%02x").mkString}")
          }

          val spatialPut = new Put(spatialKeyBytes)
          spatialPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(record.rowKey))
          mutatorSpatial.get.mutate(spatialPut)
        }

        // === Unified index write ===
        if (mutatorUnified.isDefined) {
          val lvl = unifiedLevel.getOrElse(Z3DEncoder.UNIFIED_BLOCK_LEVEL)
          val unifiedParts = UnifiedIndexKey.buildPartsForSensorId(
            record.sensorId,
            record.time,
            record.longitude,
            record.latitude,
            record.altitude,
            record.rowKey,
            lvl  // Pass level parameter
          )

          // Debug: calculate block index and print first 10 records
          if (count < 10) {
            val (x, y, z) = Z3DEncoder.normalize(record.longitude, record.latitude, record.altitude)
            val cellSize = 1 << (Z3DEncoder.BITS_PER_DIM - lvl)
            val bx = x / cellSize
            val by = y / cellSize
            val bz = z / cellSize
            println(f"[INGEST] Record $count: ($x, $y, $z) -> block idx=($bx, $by, $bz), z3dBlock=${unifiedParts.z3d}%016x, level=$lvl")
          }

          val unifiedRowKeyBytes = UnifiedIndexKey.toRowKeyBytes(unifiedParts)
          val unifiedPut = new Put(unifiedRowKeyBytes)
          unifiedPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(record.rowKey))
          mutatorUnified.get.mutate(unifiedPut)
        }

        count += 1
        if (count % batchSize == 0) {
          mutatorData.foreach(_.flush())
          mutatorSensor.foreach(_.flush())
          mutatorTime.foreach(_.flush())
          mutatorSpatial.foreach(_.flush())
          mutatorUnified.foreach(_.flush())
        }
      }

      mutatorData.foreach(_.flush())
      mutatorSensor.foreach(_.flush())
      mutatorTime.foreach(_.flush())
      mutatorSpatial.foreach(_.flush())
      mutatorUnified.foreach(_.flush())

      println(s"[Partition] Wrote $count records")

    } finally {
      mutatorData.foreach(_.close())
      mutatorSensor.foreach(_.close())
      mutatorTime.foreach(_.close())
      mutatorSpatial.foreach(_.close())
      mutatorUnified.foreach(_.close())
      connection.close()
    }
  }
  
  private def createMutator(connection: org.apache.hadoop.hbase.client.Connection,
                            tableName: String): BufferedMutator = {
    val params = new BufferedMutatorParams(TableName.valueOf(tableName))
      .writeBufferSize(4 * 1024 * 1024)
    connection.getBufferedMutator(params)
  }
}