// src/main/scala/ingest/GeoSimVoxelIngestJob.scala
package ingest

import model.GeoSimVoxelLine
import index.{GeoSimVoxelGrid, VolumeTimeBucketUtc, VolumeUnifiedIndexKey, GridMeta, GeoSimTempBucket, GeoSimVoxelTempUnifiedIndexKey, GeoSimVoxelVelocityUnifiedIndexKey}
import storage.HBaseTableManager

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Put, ConnectionFactory, BufferedMutator, BufferedMutatorParams}
import org.apache.hadoop.hbase.util.Bytes

object GeoSimVoxelIngestJob {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: GeoSimVoxelIngestJob <input_path> <zk_quorum> <batch_size> [--dataset <id>] [--unified-level <int>] [--indexes <list>]")
      println("  --indexes: comma-separated list, supports 'incremental', 'unified', or 'incremental,unified' (default: both)")
      System.exit(1)
    }

    val inputPath = args(0)
    val zkQuorum = args(1)
    val batchSize = args(2).toInt

    var datasetOpt: Option[String] = None
    var unifiedLevelOpt: Option[Int] = None
    var indexes = Set("incremental", "unified")

    var i = 3
    while (i < args.length) {
      val key = args(i)
      val value = if (i + 1 < args.length) args(i + 1) else ""

      key match {
        case "--dataset" =>
          datasetOpt = Some(value)
          i += 2
        case "--unified-level" =>
          try {
            val level = value.toInt
            if (level != 4 && level != 5 && level != 6 && level != 7 && level != 8) {
              println(s"[Error] GeoSimVoxel only supports unifiedLevel 4/5/6/7/8, got: $level")
              System.exit(1)
            }
            unifiedLevelOpt = Some(level)
            i += 2
          } catch {
            case _: NumberFormatException =>
              println(s"[Error] Invalid --unified-level value: $value")
              System.exit(1)
          }
        case "--indexes" =>
          val validValues = Set("incremental", "unified", "temp_unified", "vel_unified", "vel_incremental")
          val parts = value.split(",").map(_.trim.toLowerCase).filter(_.nonEmpty)
          val invalid = parts.filterNot(validValues.contains)
          if (invalid.nonEmpty) {
            println(s"[Error] Invalid --indexes values: ${invalid.mkString(", ")}")
            println(s"[Error] Valid values: ${validValues.mkString(", ")}")
            System.exit(1)
          }
          indexes = parts.toSet
          i += 2
        case _ =>
          i += 1
      }
    }

    println("=" * 60)
    println("[Config] GeoSimVoxelIngestJob")
    println(s"  dataset: ${datasetOpt.getOrElse("(default)")}")
    println(s"  unifiedLevel: ${unifiedLevelOpt.getOrElse("(default)")}")
    println(s"  indexes: ${indexes.mkString(", ")}")
    println("=" * 60)

    val spark = SparkSession.builder()
      .appName("GeoSimVoxelIngest")
      .getOrCreate()

    try {
      val connection = HBaseTableManager.createConnection(zkQuorum)
      try {
        HBaseTableManager.createVolumeTablesForDataset(connection, datasetOpt, unifiedLevelOpt)
        if (indexes.contains("incremental")) {
          HBaseTableManager.createGeoSimVoxelIncrementalTablesForDataset(connection, datasetOpt, unifiedLevelOpt)
        }
        if (indexes.contains("temp_unified")) {
          HBaseTableManager.createGeoSimVoxelTempUnifiedTableForDataset(connection, datasetOpt, unifiedLevelOpt)
        }
        if (indexes.contains("incremental")) {
          HBaseTableManager.createGeoSimVoxelTempBucketIdxTableForDataset(connection, datasetOpt)
        }
        if (indexes.contains("vel_unified")) {
          HBaseTableManager.createGeoSimVoxelVelocityUnifiedTablesForDataset(connection, datasetOpt, unifiedLevelOpt)
        }
        if (indexes.contains("vel_incremental")) {
          HBaseTableManager.createGeoSimVoxelVelocityBucketTablesForDataset(connection, datasetOpt)
        }
      } finally {
        connection.close()
      }

      val firstLine = spark.sparkContext.textFile(inputPath).first()
      val delimiter = GeoSimVoxelLine.detectDelimiter(firstLine)
      val delimiterName = if (delimiter == '\t') "tab" else "comma"
      println(s"[Detect] Delimiter: $delimiterName")

      val headerLine = if (firstLine.trim.startsWith("sim_id")) {
        firstLine.trim
      } else {
        "raw_line"
      }
      println(s"[Detect] Header: $headerLine")

      val records = spark.sparkContext
        .textFile(inputPath)
        .flatMap(GeoSimVoxelLine.fromLine(_, delimiter))
        .persist()

      val totalCount = records.count()
      println(s"[Parse] Total records parsed: $totalCount")

      val firstRecord = records.take(1).headOption
      if (firstRecord.isEmpty) {
        println("[Error] No valid records found")
        System.exit(1)
      }

      val dx0 = firstRecord.get.dx
      val dy0 = firstRecord.get.dy
      val dz0 = firstRecord.get.dz
      println(s"[Step] dx0=$dx0, dy0=$dy0, dz0=$dz0")

      val epsX = math.max(1.0, math.abs(dx0)) * 1e-6
      val epsY = math.max(1.0, math.abs(dy0)) * 1e-6
      val epsZ = math.max(1.0, math.abs(dz0)) * 1e-6

      val mismatchCount = records.map { r =>
        val dxMismatch = math.abs(r.dx - dx0) > epsX
        val dyMismatch = math.abs(r.dy - dy0) > epsY
        val dzMismatch = math.abs(r.dz - dz0) > epsZ
        if (dxMismatch || dyMismatch || dzMismatch) 1 else 0
      }.reduce(_ + _)

      if (mismatchCount > 0) {
        println(s"[Warn] Found $mismatchCount records with step mismatch")
        println(s"[Warn] epsX=$epsX, epsY=$epsY, epsZ=$epsZ")
      }

      val minI = records.map { r =>
        GeoSimVoxelGrid.quantizeIndex(r.xMin, 0.0, dx0)
      }.reduce(math.min)

      val minJ = records.map { r =>
        GeoSimVoxelGrid.quantizeIndex(r.yMin, 0.0, dy0)
      }.reduce(math.min)

      val minK = records.map { r =>
        GeoSimVoxelGrid.quantizeIndex(r.zMin, 0.0, dz0)
      }.reduce(math.min)

      val biasI = if (minI < 0) -minI else 0
      val biasJ = if (minJ < 0) -minJ else 0
      val biasK = if (minK < 0) -minK else 0

      println(s"[Bias] biasI=$biasI, biasJ=$biasJ, biasK=$biasK")

      val level = unifiedLevelOpt.getOrElse(4)
      val (bx, by, bz) = GeoSimVoxelGrid.blockSizeForLevel(level)

      val meta = GridMeta(
        x0 = 0.0,
        y0 = 0.0,
        z0 = 0.0,
        dx0 = dx0,
        dy0 = dy0,
        dz0 = dz0,
        biasI = biasI,
        biasJ = biasJ,
        biasK = biasK,
        blockX = bx,
        blockY = by,
        blockZ = bz,
        delimiterName = delimiterName
      )

      val metaConnection = HBaseTableManager.createConnection(zkQuorum)
      try {
        GeoSimVoxelGrid.writeMeta(metaConnection, datasetOpt, meta, headerLine)
        println("[Meta] Grid metadata written to HBase")
      } finally {
        metaConnection.close()
      }

      records.foreachPartition { partition =>
        writePartitionToHBase(partition, zkQuorum, batchSize, datasetOpt, unifiedLevelOpt.getOrElse(4), meta, indexes)
      }

      println("Data ingestion completed!")

    } finally {
      spark.stop()
    }
  }

  private def writePartitionToHBase(partition: Iterator[GeoSimVoxelLine],
                                   zkQuorum: String,
                                   batchSize: Int,
                                   dataset: Option[String],
                                   unifiedLevel: Int,
                                   meta: GridMeta,
                                   indexes: Set[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    val connection = ConnectionFactory.createConnection(conf)

    val mutatorBrick = createMutator(connection, HBaseTableManager.volumeBrickTableName(dataset))
    val mutatorUnified = if (indexes.contains("unified")) {
      Some(createMutator(connection, HBaseTableManager.volumeUnifiedIdxTableName(dataset, Some(unifiedLevel))))
    } else {
      None
    }

    val mutatorSim = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelSimIdxTableName(dataset)))
    } else {
      None
    }

    val mutatorTime = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelTimeIdxTableName(dataset)))
    } else {
      None
    }

    val mutatorSpatial = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelSpatialIdxTableName(dataset, Some(unifiedLevel))))
    } else {
      None
    }

    val mutatorTempUnified = if (indexes.contains("temp_unified")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelTempUnifiedIdxTableName(dataset, Some(unifiedLevel))))
    } else {
      None
    }

    val mutatorTempBucket = if (indexes.contains("incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelTempBucketIdxTableName(dataset)))
    } else {
      None
    }

    val mutatorVxUnified = if (indexes.contains("vel_unified")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelVxUnifiedIdxTableName(dataset, Some(unifiedLevel))))
    } else {
      None
    }

    val mutatorVyUnified = if (indexes.contains("vel_unified")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelVyUnifiedIdxTableName(dataset, Some(unifiedLevel))))
    } else {
      None
    }

    val mutatorVzUnified = if (indexes.contains("vel_unified")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelVzUnifiedIdxTableName(dataset, Some(unifiedLevel))))
    } else {
      None
    }

    val mutatorVxBucket = if (indexes.contains("vel_incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelVxBucketIdxTableName(dataset)))
    } else {
      None
    }

    val mutatorVyBucket = if (indexes.contains("vel_incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelVyBucketIdxTableName(dataset)))
    } else {
      None
    }

    val mutatorVzBucket = if (indexes.contains("vel_incremental")) {
      Some(createMutator(connection, HBaseTableManager.geoSimVoxelVzBucketIdxTableName(dataset)))
    } else {
      None
    }

    val CF = HBaseTableManager.CF_BYTES
    val tempBucketParams = GeoSimTempBucket.defaultParams
    val velBucketParams = GeoSimVoxelGrid.readVelBucketParamsFromMeta(meta)

    try {
      var count = 0
      var unifiedCount = 0
      var simCount = 0
      var timeCount = 0
      var spatialCount = 0
      var tempUnifiedCount = 0
      var tempBucketCount = 0
      var vxVelUnifiedCount = 0
      var vyVelUnifiedCount = 0
      var vzVelUnifiedCount = 0
      var vxBucketCount = 0
      var vyBucketCount = 0
      var vzBucketCount = 0

      partition.foreach { record =>
        val (iB, jB, kB) = GeoSimVoxelGrid.computeIJK(meta, record.xMin, record.yMin, record.zMin)
        val brickRowKey = s"${record.simId}|${record.timeMillis}|$iB|$jB|$kB"

        val brickPut = new Put(Bytes.toBytes(brickRowKey))
        brickPut.addColumn(CF, Bytes.toBytes("sim_id"), Bytes.toBytes(record.simId))
        brickPut.addColumn(CF, Bytes.toBytes("time_millis"), Bytes.toBytes(record.timeMillis))
        brickPut.addColumn(CF, Bytes.toBytes("x_min"), Bytes.toBytes(record.xMin))
        brickPut.addColumn(CF, Bytes.toBytes("x_max"), Bytes.toBytes(record.xMax))
        brickPut.addColumn(CF, Bytes.toBytes("y_min"), Bytes.toBytes(record.yMin))
        brickPut.addColumn(CF, Bytes.toBytes("y_max"), Bytes.toBytes(record.yMax))
        brickPut.addColumn(CF, Bytes.toBytes("z_min"), Bytes.toBytes(record.zMin))
        brickPut.addColumn(CF, Bytes.toBytes("z_max"), Bytes.toBytes(record.zMax))
        brickPut.addColumn(CF, Bytes.toBytes("T"), Bytes.toBytes(record.t))
        brickPut.addColumn(CF, Bytes.toBytes("vx"), Bytes.toBytes(record.vx))
        brickPut.addColumn(CF, Bytes.toBytes("vy"), Bytes.toBytes(record.vy))
        brickPut.addColumn(CF, Bytes.toBytes("vz"), Bytes.toBytes(record.vz))
        brickPut.addColumn(CF, Bytes.toBytes("raw_line"), Bytes.toBytes(record.rawLine))
        mutatorBrick.mutate(brickPut)

        val zCell = GeoSimVoxelGrid.computeZCellFromIJK(meta, iB, jB, kB, unifiedLevel)
        val dayBucket = VolumeTimeBucketUtc.dayBucket(record.timeMillis)
        val timeOfDay = VolumeTimeBucketUtc.timeOfDay(record.timeMillis)

        mutatorUnified.foreach { mutator =>
          val unifiedRowKeyBytes = VolumeUnifiedIndexKey.toRowKeyBytes(
            record.simId, zCell, dayBucket, timeOfDay, brickRowKey
          )
          val unifiedPut = new Put(unifiedRowKeyBytes)
          unifiedPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(unifiedPut)
          unifiedCount += 1
        }

        mutatorSim.foreach { mutator =>
          val simRowKey = s"${record.simId}_$brickRowKey"
          val simPut = new Put(Bytes.toBytes(simRowKey))
          simPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(simPut)
          simCount += 1
        }

        mutatorTime.foreach { mutator =>
          val timeRowKey = f"${dayBucket}%08d_${timeOfDay}%08d_$brickRowKey"
          val timePut = new Put(Bytes.toBytes(timeRowKey))
          timePut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(timePut)
          timeCount += 1
        }

        mutatorSpatial.foreach { mutator =>
          val zCellBytes = Bytes.toBytes(zCell)
          val spatialRowKeyBytes = zCellBytes ++ Bytes.toBytes("_" + brickRowKey)
          val spatialPut = new Put(spatialRowKeyBytes)
          spatialPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(spatialPut)
          spatialCount += 1
        }

        mutatorTempUnified.foreach { mutator =>
          val tempBucket = GeoSimTempBucket.bucketOf(record.t, tempBucketParams)
          val tempUnifiedRowKeyBytes = GeoSimVoxelTempUnifiedIndexKey.toRowKeyBytes(
            tempBucket, zCell, dayBucket, timeOfDay, brickRowKey
          )
          val tempUnifiedPut = new Put(tempUnifiedRowKeyBytes)
          tempUnifiedPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(tempUnifiedPut)
          tempUnifiedCount += 1
        }

        mutatorTempBucket.foreach { mutator =>
          val tempBucket = GeoSimTempBucket.bucketOf(record.t, tempBucketParams)
          val tempBucketRowKey = f"${tempBucket}%08d_$brickRowKey"
          val tempBucketPut = new Put(Bytes.toBytes(tempBucketRowKey))
          tempBucketPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(tempBucketPut)
          tempBucketCount += 1
        }

        mutatorVxUnified.foreach { mutator =>
          val v0 = 0.0
          val dvx = 5e-6
          val bx = math.floor((record.vx - v0) / dvx).toInt
          val enc = bx ^ 0x80000000
          val vxUnifiedRowKeyBytes = GeoSimVoxelVelocityUnifiedIndexKey.vxRowKeyBytes(enc, zCell, dayBucket, timeOfDay, brickRowKey)
          val vxUnifiedPut = new Put(vxUnifiedRowKeyBytes)
          vxUnifiedPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(vxUnifiedPut)
          vxVelUnifiedCount += 1
        }

        mutatorVyUnified.foreach { mutator =>
          val v0 = 0.0
          val dvy = 5e-6
          val by = math.floor((record.vy - v0) / dvy).toInt
          val enc = by ^ 0x80000000
          val vyUnifiedRowKeyBytes = GeoSimVoxelVelocityUnifiedIndexKey.vyRowKeyBytes(enc, zCell, dayBucket, timeOfDay, brickRowKey)
          val vyUnifiedPut = new Put(vyUnifiedRowKeyBytes)
          vyUnifiedPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(vyUnifiedPut)
          vyVelUnifiedCount += 1
        }

        mutatorVzUnified.foreach { mutator =>
          val v0 = 0.0
          val dvz = 5e-5
          val bz = math.floor((record.vz - v0) / dvz).toInt
          val enc = bz ^ 0x80000000
          val vzUnifiedRowKeyBytes = GeoSimVoxelVelocityUnifiedIndexKey.vzRowKeyBytes(enc, zCell, dayBucket, timeOfDay, brickRowKey)
          val vzUnifiedPut = new Put(vzUnifiedRowKeyBytes)
          vzUnifiedPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(vzUnifiedPut)
          vzVelUnifiedCount += 1
        }

        mutatorVxBucket.foreach { mutator =>
          val v0 = velBucketParams._1
          val dvx = velBucketParams._3
          val vxBucketRaw = math.floor((record.vx - v0) / dvx).toInt
          val vxBucketEnc = vxBucketRaw ^ 0x80000000
          val vxBucketRowKeyBytes = Bytes.toBytes(vxBucketEnc) ++ Bytes.toBytes("_" + brickRowKey)
          val vxBucketPut = new Put(vxBucketRowKeyBytes)
          vxBucketPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(vxBucketPut)
          vxBucketCount += 1
        }

        mutatorVyBucket.foreach { mutator =>
          val v0 = velBucketParams._1
          val dvy = velBucketParams._4
          val vyBucketRaw = math.floor((record.vy - v0) / dvy).toInt
          val vyBucketEnc = vyBucketRaw ^ 0x80000000
          val vyBucketRowKeyBytes = Bytes.toBytes(vyBucketEnc) ++ Bytes.toBytes("_" + brickRowKey)
          val vyBucketPut = new Put(vyBucketRowKeyBytes)
          vyBucketPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(vyBucketPut)
          vyBucketCount += 1
        }

        mutatorVzBucket.foreach { mutator =>
          val v0 = velBucketParams._1
          val dvz = velBucketParams._5
          val vzBucketRaw = math.floor((record.vz - v0) / dvz).toInt
          val vzBucketEnc = vzBucketRaw ^ 0x80000000
          val vzBucketRowKeyBytes = Bytes.toBytes(vzBucketEnc) ++ Bytes.toBytes("_" + brickRowKey)
          val vzBucketPut = new Put(vzBucketRowKeyBytes)
          vzBucketPut.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(brickRowKey))
          mutator.mutate(vzBucketPut)
          vzBucketCount += 1
        }

        count += 1
        if (count % batchSize == 0) {
          mutatorBrick.flush()
          mutatorUnified.foreach(_.flush())
          mutatorSim.foreach(_.flush())
          mutatorTime.foreach(_.flush())
          mutatorSpatial.foreach(_.flush())
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

      mutatorBrick.flush()
      mutatorUnified.foreach(_.flush())
      mutatorSim.foreach(_.flush())
      mutatorTime.foreach(_.flush())
      mutatorSpatial.foreach(_.flush())
      mutatorTempUnified.foreach(_.flush())
      mutatorTempBucket.foreach(_.flush())
      mutatorVxUnified.foreach(_.flush())
      mutatorVyUnified.foreach(_.flush())
      mutatorVzUnified.foreach(_.flush())
      mutatorVxBucket.foreach(_.flush())
      mutatorVyBucket.foreach(_.flush())
      mutatorVzBucket.foreach(_.flush())

      val (bxSize, bySize, bzSize) = GeoSimVoxelGrid.blockSizeForLevel(unifiedLevel)
      println(s"[Partition] Wrote $count records")
      println(s"[Partition]   brick: $count")
      println(s"[Partition]   unified: $unifiedCount")
      println(s"[Partition]   incremental(sim): $simCount")
      println(s"[Partition]   incremental(time): $timeCount")
      println(s"[Partition]   incremental(spatial): $spatialCount")
      println(s"[Partition]   temp_unified_written: $tempUnifiedCount")
      println(s"[Partition]   temp_bucket_written: $tempBucketCount")
      println(s"[Partition]   vx_vel_unified_written: $vxVelUnifiedCount")
      println(s"[Partition]   vy_vel_unified_written: $vyVelUnifiedCount")
      println(s"[Partition]   vz_vel_unified_written: $vzVelUnifiedCount")
      println(s"[Partition]   vxBucketIdxCount: $vxBucketCount")
      println(s"[Partition]   vyBucketIdxCount: $vyBucketCount")
      println(s"[Partition]   vzBucketIdxCount: $vzBucketCount")
      println(s"[Partition]   unifiedLevel: $unifiedLevel")
      println(s"[Partition]   blockSize($unifiedLevel): ${bxSize}x${bySize}x${bzSize}")

    } finally {
      mutatorBrick.close()
      mutatorUnified.foreach(_.close())
      mutatorSim.foreach(_.close())
      mutatorTime.foreach(_.close())
      mutatorSpatial.foreach(_.close())
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
}
