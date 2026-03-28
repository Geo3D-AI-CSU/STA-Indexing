// src/main/scala/ingest/GeoSimPointIngestJob.scala
package ingest

import model.GeoSimPointLine
import index.{Z3DEncoder, TimeBucket, UnifiedIndexKey, GeoSimCoordMapper, Bounds, GeoSimTempBucket, GeoSimVelocityBucket}
import storage.HBaseTableManager

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Put, ConnectionFactory, BufferedMutator, BufferedMutatorParams, Admin}
import org.apache.hadoop.hbase.util.Bytes

object GeoSimPointIngestJob {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: GeoSimPointIngestJob <input_path> <zk_quorum> <batch_size> [--dataset <id>] [--indexes <list>] [--unified-level <int>]")
      System.exit(1)
    }

    val inputPath = args(0)
    val zkQuorum = args(1)
    val batchSize = args(2).toInt

    var datasetOpt: Option[String] = None
    var indexesOpt: Set[String] = Set("incremental", "unified")
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

    val validIndexes = Set("incremental", "unified", "temp_unified", "vel_unified")
    if (!indexesOpt.subsetOf(validIndexes)) {
      println(s"[Warn] Invalid index types, using defaults. Valid: ${validIndexes.mkString(", ")}")
      indexesOpt = Set("incremental", "unified")
    }

    println("=" * 60)
    println("[Config] GeoSimPointIngestJob")
    println(s"  dataset: ${datasetOpt.getOrElse("(default)")}")
    println(s"  indexes: ${indexesOpt.mkString(", ")}")
    println(s"  unifiedLevel: ${unifiedLevelOpt.getOrElse("(default)")}")
    println("=" * 60)

    val spark = SparkSession.builder()
      .appName("GeoSimPointIngest")
      .getOrCreate()

    try {
      val connection = HBaseTableManager.createConnection(zkQuorum)
      try {
        HBaseTableManager.createGeoSimPointTablesForDataset(connection, datasetOpt, unifiedLevelOpt)
        
        if (indexesOpt.contains("temp_unified")) {
          createTempUnifiedTable(connection, datasetOpt, unifiedLevelOpt)
        }
        
        if (indexesOpt.contains("vel_unified")) {
          createVelUnifiedTables(connection, datasetOpt, unifiedLevelOpt)
        }
        
        if (indexesOpt.contains("incremental")) {
          HBaseTableManager.createGeoSimPointTempBucketIdxTableForDataset(connection, datasetOpt)
          HBaseTableManager.createGeoSimPointVelocityBucketTablesForDataset(connection, datasetOpt)
        }
      } finally {
        connection.close()
      }

      val firstLine = spark.sparkContext.textFile(inputPath).first()
      val delimiter = GeoSimCoordMapper.detectDelimiter(firstLine)
      val delimiterName = if (delimiter == '\t') "tab" else "comma"
      val headerLine = firstLine.trim
      println(s"[Detect] Delimiter: $delimiterName")
      println(s"[Detect] Header line: $headerLine")

      val records = spark.sparkContext
        .textFile(inputPath)
        .flatMap(GeoSimPointLine.fromLine(_, delimiter))
        .persist()

      val totalCount = records.count()
      println(s"[Parse] Total records parsed: $totalCount")

      val bounds = records.aggregate(
        (Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue)
      )(
        (acc, record) => (
          math.min(acc._1, record.x),
          math.max(acc._2, record.x),
          math.min(acc._3, record.y),
          math.max(acc._4, record.y),
          math.min(acc._5, record.z),
          math.max(acc._6, record.z)
        ),
        (acc1, acc2) => (
          math.min(acc1._1, acc2._1),
          math.max(acc1._2, acc2._2),
          math.min(acc1._3, acc2._3),
          math.max(acc1._4, acc2._4),
          math.min(acc1._5, acc2._5),
          math.max(acc1._6, acc2._6)
        )
      )

      val boundsObj = Bounds(bounds._1, bounds._2, bounds._3, bounds._4, bounds._5, bounds._6)
      println(s"[Bounds] X: [${bounds._1}, ${bounds._2}], Y: [${bounds._3}, ${bounds._4}], Z: [${bounds._5}, ${bounds._6}]")

      val metaConnection = HBaseTableManager.createConnection(zkQuorum)
      try {
        GeoSimCoordMapper.writeMeta(metaConnection, datasetOpt, boundsObj, delimiterName, headerLine)
        writeTempMeta(metaConnection, datasetOpt)
        GeoSimCoordMapper.writeVelocityParams(metaConnection, datasetOpt, GeoSimVelocityBucket.defaultParams)
        val velParams = GeoSimVelocityBucket.defaultParams
        println(s"[Meta] Velocity params: dvx=${velParams.dvx}, dvy=${velParams.dvy}, dvz=${velParams.dvz}, method=${velParams.method}")
        println("[Meta] Metadata written to HBase")
      } finally {
        metaConnection.close()
      }

      records.foreachPartition { partition =>
        writePartitionToHBase(partition, zkQuorum, batchSize, datasetOpt, indexesOpt, unifiedLevelOpt, boundsObj)
      }

      println("Data ingestion completed!")

    } finally {
      spark.stop()
    }
  }

  private def writePartitionToHBase(partition: Iterator[GeoSimPointLine],
                                     zkQuorum: String,
                                     batchSize: Int,
                                     dataset: Option[String],
                                     indexes: Set[String],
                                     unifiedLevel: Option[Int],
                                     bounds: Bounds): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    val connection = ConnectionFactory.createConnection(conf)

    val mutatorData = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointDataTableName(dataset)))
    } else None

    val mutatorSim = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointSimIdxTableName(dataset)))
    } else None

    val mutatorTime = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointTimeIdxTableName(dataset)))
    } else None

    val mutatorSpatial = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointSpatialIdxTableName(dataset)))
    } else None

    val mutatorUnified = if (indexes.contains("unified")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointUnifiedIdxTableName(dataset, unifiedLevel)))
    } else None

    val mutatorTempUnified = if (indexes.contains("temp_unified")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointTempUnifiedIdxTableName(dataset, unifiedLevel)))
    } else None

    val mutatorTempBucket = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointTempBucketIdxTableName(dataset)))
    } else None

    val mutatorVxBucket = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointVxBucketIdxTableName(dataset)))
    } else None

    val mutatorVyBucket = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointVyBucketIdxTableName(dataset)))
    } else None

    val mutatorVzBucket = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointVzBucketIdxTableName(dataset)))
    } else None

    val mutatorVxUnified = if (indexes.contains("vel_unified")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointVxUnifiedIdxTableName(dataset, unifiedLevel)))
    } else None

    val mutatorVyUnified = if (indexes.contains("vel_unified")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointVyUnifiedIdxTableName(dataset, unifiedLevel)))
    } else None

    val mutatorVzUnified = if (indexes.contains("vel_unified")) {
      Some(createMutator(connection, HBaseTableManager.geoSimPointVzUnifiedIdxTableName(dataset, unifiedLevel)))
    } else None

    val CF = HBaseTableManager.CF_BYTES
    val lvl = unifiedLevel.getOrElse(Z3DEncoder.UNIFIED_BLOCK_LEVEL)
    val tempBucketParams = GeoSimTempBucket.defaultParams
    val velBucketParams = GeoSimVelocityBucket.defaultParams

    try {
      var count = 0
      var dataCount = 0
      var simCount = 0
      var timeCount = 0
      var spatialCount = 0
      var unifiedCount = 0
      var tempBucketCount = 0
      var vxUnifiedCount = 0
      var vyUnifiedCount = 0
      var vzUnifiedCount = 0
      var vxBucketIdxCount = 0
      var vyBucketIdxCount = 0
      var vzBucketIdxCount = 0

      partition.foreach { record =>
        val dataRowKey = java.util.UUID.randomUUID().toString.replace("-", "")

        if (mutatorData.isDefined) {
          val dataPut = new Put(Bytes.toBytes(dataRowKey))
          dataPut.addColumn(CF, Bytes.toBytes("time"), Bytes.toBytes(record.timeMillis))
          dataPut.addColumn(CF, Bytes.toBytes("sensor_id"), Bytes.toBytes(record.simId))
          dataPut.addColumn(CF, Bytes.toBytes("lon"), Bytes.toBytes(record.x))
          dataPut.addColumn(CF, Bytes.toBytes("lat"), Bytes.toBytes(record.y))
          dataPut.addColumn(CF, Bytes.toBytes("alt"), Bytes.toBytes(record.z))
          dataPut.addColumn(CF, Bytes.toBytes("T"), Bytes.toBytes(record.t))
          dataPut.addColumn(CF, Bytes.toBytes("vx"), Bytes.toBytes(record.vx))
          dataPut.addColumn(CF, Bytes.toBytes("vy"), Bytes.toBytes(record.vy))
          dataPut.addColumn(CF, Bytes.toBytes("vz"), Bytes.toBytes(record.vz))
          dataPut.addColumn(CF, Bytes.toBytes("type"), Bytes.toBytes("geosim_point"))
          dataPut.addColumn(CF, Bytes.toBytes("raw_line"), Bytes.toBytes(record.rawLine))
          mutatorData.get.mutate(dataPut)
          dataCount += 1
        }

        if (mutatorSim.isDefined) {
          val simKey = s"${record.simId}_${dataRowKey}"
          val simPut = new Put(Bytes.toBytes(simKey))
          simPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorSim.get.mutate(simPut)
          simCount += 1
        }

        if (mutatorTime.isDefined) {
          val bucket = TimeBucket.dayBucket(record.timeMillis)
          val timeKey = f"${bucket}_${record.timeMillis}%013d_${dataRowKey}"
          val timePut = new Put(Bytes.toBytes(timeKey))
          timePut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorTime.get.mutate(timePut)
          timeCount += 1
        }

        if (mutatorSpatial.isDefined) {
          val mappedLon = GeoSimCoordMapper.mapX(bounds, record.x)
          val mappedLat = GeoSimCoordMapper.mapY(bounds, record.y)
          val mappedAlt = GeoSimCoordMapper.mapZ(bounds, record.z)
          val blockId = Z3DEncoder.blockIdForUnifiedIndexAtLevel(lvl, mappedLon, mappedLat, mappedAlt)
          val blockIdBytes = Z3DEncoder.longToBytes(blockId)
          val spatialKeyBytes = blockIdBytes ++ Bytes.toBytes("_" + dataRowKey)
          val spatialPut = new Put(spatialKeyBytes)
          spatialPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorSpatial.get.mutate(spatialPut)
          spatialCount += 1
        }

        if (mutatorUnified.isDefined) {
          val mappedLon = GeoSimCoordMapper.mapX(bounds, record.x)
          val mappedLat = GeoSimCoordMapper.mapY(bounds, record.y)
          val mappedAlt = GeoSimCoordMapper.mapZ(bounds, record.z)

          val unifiedParts = UnifiedIndexKey.buildPartsForSensorId(
            record.simId,
            record.timeMillis,
            mappedLon,
            mappedLat,
            mappedAlt,
            dataRowKey,
            lvl
          )

          val unifiedRowKeyBytes = UnifiedIndexKey.toRowKeyBytes(unifiedParts)
          val unifiedPut = new Put(unifiedRowKeyBytes)
          unifiedPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorUnified.get.mutate(unifiedPut)
          unifiedCount += 1
        }

        if (mutatorTempUnified.isDefined) {
          val mappedLon = GeoSimCoordMapper.mapX(bounds, record.x)
          val mappedLat = GeoSimCoordMapper.mapY(bounds, record.y)
          val mappedAlt = GeoSimCoordMapper.mapZ(bounds, record.z)

          val tempBucket = GeoSimTempBucket.bucketOf(record.t, tempBucketParams)

          val tempUnifiedParts = UnifiedIndexKey.buildPartsForTempBucket(
            tempBucket,
            record.timeMillis,
            mappedLon,
            mappedLat,
            mappedAlt,
            dataRowKey,
            lvl
          )

          val tempUnifiedRowKeyBytes = UnifiedIndexKey.toRowKeyBytes(tempUnifiedParts)
          val tempUnifiedPut = new Put(tempUnifiedRowKeyBytes)
          tempUnifiedPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorTempUnified.get.mutate(tempUnifiedPut)
          unifiedCount += 1
        }

        if (mutatorTempBucket.isDefined) {
          val tempBucket = GeoSimTempBucket.bucketOf(record.t, tempBucketParams)
          val rk = f"${tempBucket}%08d_${dataRowKey}"
          val tempBucketPut = new Put(Bytes.toBytes(rk))
          tempBucketPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorTempBucket.get.mutate(tempBucketPut)
          tempBucketCount += 1
        }

        if (mutatorVxUnified.isDefined) {
          val mappedLon = GeoSimCoordMapper.mapX(bounds, record.x)
          val mappedLat = GeoSimCoordMapper.mapY(bounds, record.y)
          val mappedAlt = GeoSimCoordMapper.mapZ(bounds, record.z)

          val vxBucket = GeoSimVelocityBucket.bucketOf(record.vx, velBucketParams.v0, velBucketParams.dvx, velBucketParams.method)
          val vxEnc = GeoSimVelocityBucket.encodeBucket(vxBucket)

          val vxParts = UnifiedIndexKey.buildPartsForVxBucket(
            vxEnc,
            record.timeMillis,
            mappedLon,
            mappedLat,
            mappedAlt,
            dataRowKey,
            lvl
          )

          val vxRowKeyBytes = UnifiedIndexKey.toRowKeyBytes(vxParts)
          val vxPut = new Put(vxRowKeyBytes)
          vxPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorVxUnified.get.mutate(vxPut)
          vxUnifiedCount += 1
        }

        if (mutatorVyUnified.isDefined) {
          val mappedLon = GeoSimCoordMapper.mapX(bounds, record.x)
          val mappedLat = GeoSimCoordMapper.mapY(bounds, record.y)
          val mappedAlt = GeoSimCoordMapper.mapZ(bounds, record.z)

          val vyBucket = GeoSimVelocityBucket.bucketOf(record.vy, velBucketParams.v0, velBucketParams.dvy, velBucketParams.method)
          val vyEnc = GeoSimVelocityBucket.encodeBucket(vyBucket)

          val vyParts = UnifiedIndexKey.buildPartsForVyBucket(
            vyEnc,
            record.timeMillis,
            mappedLon,
            mappedLat,
            mappedAlt,
            dataRowKey,
            lvl
          )

          val vyRowKeyBytes = UnifiedIndexKey.toRowKeyBytes(vyParts)
          val vyPut = new Put(vyRowKeyBytes)
          vyPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorVyUnified.get.mutate(vyPut)
          vyUnifiedCount += 1
        }

        if (mutatorVzUnified.isDefined) {
          val mappedLon = GeoSimCoordMapper.mapX(bounds, record.x)
          val mappedLat = GeoSimCoordMapper.mapY(bounds, record.y)
          val mappedAlt = GeoSimCoordMapper.mapZ(bounds, record.z)

          val vzBucket = GeoSimVelocityBucket.bucketOf(record.vz, velBucketParams.v0, velBucketParams.dvz, velBucketParams.method)
          val vzEnc = GeoSimVelocityBucket.encodeBucket(vzBucket)

          val vzParts = UnifiedIndexKey.buildPartsForVzBucket(
            vzEnc,
            record.timeMillis,
            mappedLon,
            mappedLat,
            mappedAlt,
            dataRowKey,
            lvl
          )

          val vzRowKeyBytes = UnifiedIndexKey.toRowKeyBytes(vzParts)
          val vzPut = new Put(vzRowKeyBytes)
          vzPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorVzUnified.get.mutate(vzPut)
          vzUnifiedCount += 1
        }

        if (mutatorVxBucket.isDefined) {
          val vxBucket = GeoSimVelocityBucket.bucketOf(record.vx, velBucketParams.v0, velBucketParams.dvx, velBucketParams.method)
          val vxEnc = GeoSimVelocityBucket.encodeBucket(vxBucket)
          val vxBucketKeyBytes = Bytes.toBytes(vxEnc) ++ Bytes.toBytes("_") ++ Bytes.toBytes(dataRowKey)
          val vxBucketPut = new Put(vxBucketKeyBytes)
          vxBucketPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorVxBucket.get.mutate(vxBucketPut)
          vxBucketIdxCount += 1
        }

        if (mutatorVyBucket.isDefined) {
          val vyBucket = GeoSimVelocityBucket.bucketOf(record.vy, velBucketParams.v0, velBucketParams.dvy, velBucketParams.method)
          val vyEnc = GeoSimVelocityBucket.encodeBucket(vyBucket)
          val vyBucketKeyBytes = Bytes.toBytes(vyEnc) ++ Bytes.toBytes("_") ++ Bytes.toBytes(dataRowKey)
          val vyBucketPut = new Put(vyBucketKeyBytes)
          vyBucketPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorVyBucket.get.mutate(vyBucketPut)
          vyBucketIdxCount += 1
        }

        if (mutatorVzBucket.isDefined) {
          val vzBucket = GeoSimVelocityBucket.bucketOf(record.vz, velBucketParams.v0, velBucketParams.dvz, velBucketParams.method)
          val vzEnc = GeoSimVelocityBucket.encodeBucket(vzBucket)
          val vzBucketKeyBytes = Bytes.toBytes(vzEnc) ++ Bytes.toBytes("_") ++ Bytes.toBytes(dataRowKey)
          val vzBucketPut = new Put(vzBucketKeyBytes)
          vzBucketPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(dataRowKey))
          mutatorVzBucket.get.mutate(vzBucketPut)
          vzBucketIdxCount += 1
        }

        count += 1
        if (count % batchSize == 0) {
          mutatorData.foreach(_.flush())
          mutatorSim.foreach(_.flush())
          mutatorTime.foreach(_.flush())
          mutatorSpatial.foreach(_.flush())
          mutatorUnified.foreach(_.flush())
          mutatorTempUnified.foreach(_.flush())
          mutatorTempBucket.foreach(_.flush())
          mutatorVxUnified.foreach(_.flush())
          mutatorVyUnified.foreach(_.flush())
          mutatorVzUnified.foreach(_.flush())
          mutatorVxBucket.foreach(_.flush())
          mutatorVyBucket.foreach(_.flush())
          mutatorVzBucket.foreach(_.flush())
        }
      }

      mutatorData.foreach(_.flush())
      mutatorSim.foreach(_.flush())
      mutatorTime.foreach(_.flush())
      mutatorSpatial.foreach(_.flush())
      mutatorUnified.foreach(_.flush())
      mutatorTempUnified.foreach(_.flush())
      mutatorTempBucket.foreach(_.flush())
      mutatorVxUnified.foreach(_.flush())
      mutatorVyUnified.foreach(_.flush())
      mutatorVzUnified.foreach(_.flush())
      mutatorVxBucket.foreach(_.flush())
      mutatorVyBucket.foreach(_.flush())
      mutatorVzBucket.foreach(_.flush())

      println(s"[Partition] Total: $count, Data: $dataCount, SimIdx: $simCount, TimeIdx: $timeCount, SpatialIdx: $spatialCount, UnifiedIdx: $unifiedCount, TempBucketIdx: $tempBucketCount, VxUnified: $vxUnifiedCount, VyUnified: $vyUnifiedCount, VzUnified: $vzUnifiedCount, VxBucketIdx: $vxBucketIdxCount, VyBucketIdx: $vyBucketIdxCount, VzBucketIdx: $vzBucketIdxCount")

    } finally {
      mutatorData.foreach(_.close())
      mutatorSim.foreach(_.close())
      mutatorTime.foreach(_.close())
      mutatorSpatial.foreach(_.close())
      mutatorUnified.foreach(_.close())
      mutatorTempUnified.foreach(_.close())
      mutatorTempBucket.foreach(_.close())
      mutatorVxUnified.foreach(_.close())
      mutatorVyUnified.foreach(_.close())
      mutatorVzUnified.foreach(_.close())
      mutatorVxBucket.foreach(_.close())
      mutatorVyBucket.foreach(_.close())
      mutatorVzBucket.foreach(_.close())
      connection.close()
    }
  }

  private def createMutator(connection: org.apache.hadoop.hbase.client.Connection,
                            tableName: String): BufferedMutator = {
    val params = new BufferedMutatorParams(TableName.valueOf(tableName))
      .writeBufferSize(4 * 1024 * 1024)
    connection.getBufferedMutator(params)
  }

  private def createTempUnifiedTable(connection: org.apache.hadoop.hbase.client.Connection,
                                     datasetOpt: Option[String],
                                     unifiedLevelOpt: Option[Int]): Unit = {
    val admin = connection.getAdmin
    try {
      val tableName = HBaseTableManager.geoSimPointTempUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)
      val splitKeys = HBaseTableManager.unifiedSplits(16)
      
      val tn = TableName.valueOf(tableName)
      if (!admin.tableExists(tn)) {
        val cfDesc = org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.newBuilder(HBaseTableManager.CF_BYTES)
          .setMaxVersions(1)
          .build()
        
        val tableDesc = org.apache.hadoop.hbase.client.TableDescriptorBuilder.newBuilder(tn)
          .setColumnFamily(cfDesc)
          .build()
        
        if (splitKeys.nonEmpty) {
          admin.createTable(tableDesc, splitKeys)
        } else {
          admin.createTable(tableDesc)
        }
        println(s"[CreateTable] Created temp unified table: $tableName")
      } else {
        println(s"[CreateTable] Temp unified table exists: $tableName")
      }
    } finally {
      admin.close()
    }
  }

  private def writeTempMeta(connection: org.apache.hadoop.hbase.client.Connection,
                            datasetOpt: Option[String]): Unit = {
    val tableName = HBaseTableManager.geoSimPointDataTableName(datasetOpt)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))
    
    try {
      val put = new Put(Bytes.toBytes(GeoSimCoordMapper.META_ROWKEY))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(GeoSimCoordMapper.QUAL_TEMP_BUCKET_WIDTH_K), Bytes.toBytes(GeoSimTempBucket.DEFAULT_TEMP_BUCKET_WIDTH_K))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(GeoSimCoordMapper.QUAL_TEMP_BUCKET_T0_K), Bytes.toBytes(GeoSimTempBucket.DEFAULT_TEMP_BUCKET_T0_K))
      put.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes(GeoSimCoordMapper.QUAL_TEMP_BUCKET_METHOD), Bytes.toBytes(GeoSimTempBucket.DEFAULT_TEMP_BUCKET_METHOD))
      table.put(put)
    } finally {
      table.close()
    }
  }

  private def createVelUnifiedTables(connection: org.apache.hadoop.hbase.client.Connection,
                                      datasetOpt: Option[String],
                                      unifiedLevelOpt: Option[Int]): Unit = {
    val admin = connection.getAdmin
    try {
      val vxTableName = HBaseTableManager.geoSimPointVxUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)
      val vyTableName = HBaseTableManager.geoSimPointVyUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)
      val vzTableName = HBaseTableManager.geoSimPointVzUnifiedIdxTableName(datasetOpt, unifiedLevelOpt)
      
      val splitKeys = HBaseTableManager.unifiedSplits(16)
      
      Seq(vxTableName, vyTableName, vzTableName).foreach { tableName =>
        val tn = TableName.valueOf(tableName)
        if (!admin.tableExists(tn)) {
          val cfDesc = org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.newBuilder(HBaseTableManager.CF_BYTES)
            .setMaxVersions(1)
            .build()
          
          val tableDesc = org.apache.hadoop.hbase.client.TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(cfDesc)
            .build()
          
          if (splitKeys.nonEmpty) {
            admin.createTable(tableDesc, splitKeys)
          } else {
            admin.createTable(tableDesc)
          }
          println(s"[GeoSim-Point] Created/verified vel unified table: $tableName")
        } else {
          println(s"[GeoSim-Point] Vel unified table exists: $tableName")
        }
      }
    } finally {
      admin.close()
    }
  }
}
