package ingest

import model.VolumeModelRecord
import util.PorosityPayloadCodec
import storage.HBaseTableManager
import index.{VolumeTimeBucketUtc, VolumeZCell, VolumeUnifiedIndexKey}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

object VolumeVoxelIngestJob {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: VolumeVoxelIngestJob <input_csv_path> <zk_quorum> <batch_size> [--dataset <id>] [--indexes unified] [--unified-level <level>] [--build-bloom]")
      sys.exit(1)
    }

    val inputPath = args(0)
    val zkQuorum = args(1)
    val batchSize = args(2).toInt

    var dataset: Option[String] = None
    var indexes: Option[String] = None
    var unifiedLevel: Option[Int] = None
    var buildBloom = false

    var i = 3
    while (i < args.length) {
      args(i) match {
        case "--dataset" =>
          if (i + 1 < args.length) {
            dataset = Some(args(i + 1))
            i += 1
          }
        case "--indexes" =>
          if (i + 1 < args.length) {
            indexes = Some(args(i + 1))
            i += 1
          }
        case "--unified-level" =>
          if (i + 1 < args.length) {
            unifiedLevel = Some(args(i + 1).toInt)
            i += 1
          }
        case "--build-bloom" =>
          buildBloom = true
        case _ => // Ignore unknown parameters
      }
      i += 1
    }

    println(s"[Voxel Import] Parsing arguments:")
    println(s"  → Input path: $inputPath")
    println(s"  → ZK cluster: $zkQuorum")
    println(s"  → Batch size: $batchSize")
    println(s"  → Dataset: ${dataset.getOrElse("default")}")
    println(s"  → Index type: ${indexes.getOrElse("none")}")
    println(s"  → Unified index level: ${unifiedLevel.getOrElse(4)}")
    println(s"  → Build Bloom: $buildBloom")
    if (buildBloom) {
      println(s"[Voxel Import] Note: Bloom filter will be implemented in Phase 3, skipped this time")
    }

    val spark = SparkSession.builder()
      .appName("VolumeVoxelIngestJob")
      .getOrCreate()

    try {
      processVolumeData(spark, inputPath, zkQuorum, batchSize, dataset, unifiedLevel, indexes)
    } finally {
      spark.stop()
    }
  }

  def processVolumeData(
    spark: SparkSession,
    inputPath: String,
    zkQuorum: String,
    batchSize: Int,
    dataset: Option[String],
    unifiedLevel: Option[Int],
    indexes: Option[String]
  ): Unit = {
    import spark.implicits._

    // Read file and detect delimiter
    val firstLine = spark.sparkContext.textFile(inputPath).first()
    val delimiter = if (firstLine.contains("\t")) '\t' else ','
    println(s"[Voxel Import] Detected delimiter: ${if (delimiter == '\t') "tab" else ","}")

    // Read data, skip header
    val rawData = spark.sparkContext.textFile(inputPath)
    val dataLines = if (firstLine.startsWith("model_id")) {
      rawData.mapPartitionsWithIndex { (idx, iter) =>
        if (idx == 0) iter.drop(1) else iter
      }
    } else {
      rawData
    }

    // Statistics variables
    var totalLines = 0
    var successCount = 0
    var failCount = 0
    var modelTypeCount = scala.collection.mutable.Map[String, Int]()
    var totalBricks = 0
    var emptyBricks = 0
    var unifiedIndexCount = 0  // unified index write count
    var skippedUnifiedIndexCount = 0  // Skipped (all NoData) index count
    var skippedUnifiedIndexDueToStepSize = 0  // Skipped (step size mismatch) index count
    var failReasons = scala.collection.mutable.ListBuffer[String]()
    val shouldWriteUnifiedIndex = indexes.contains("unified")
    
    // Step size validation tolerance
    val epsilon = 1e-9

    val results = dataLines.mapPartitions { iter =>
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", zkQuorum)
      
      val connection = ConnectionFactory.createConnection(conf)
      val metaTableName = HBaseTableManager.volumeMetaTableName(dataset)
      val brickTableName = HBaseTableManager.volumeBrickTableName(dataset)
      
      val metaTable = connection.getTable(TableName.valueOf(metaTableName))
      val brickTable = connection.getTable(TableName.valueOf(brickTableName))
      
      val metaBufferedMutator = connection.getBufferedMutator(TableName.valueOf(metaTableName))
      val brickBufferedMutator = connection.getBufferedMutator(TableName.valueOf(brickTableName))
      
      // unified index table mutator (created only when needed)
      var unifiedIdxBufferedMutator: BufferedMutator = null
      val shouldWriteUnifiedIndex = indexes.contains("unified")
      if (shouldWriteUnifiedIndex) {
        val unifiedIdxTableName = HBaseTableManager.volumeUnifiedIdxTableName(dataset, unifiedLevel)
        unifiedIdxBufferedMutator = connection.getBufferedMutator(TableName.valueOf(unifiedIdxTableName))
      }

      val localStats = scala.collection.mutable.Map[String, Any]()
      
      var localTotalLines = 0
      var localSuccessCount = 0
      var localFailCount = 0
      var localModelTypeCount = scala.collection.mutable.Map[String, Int]()
      var localTotalBricks = 0
      var localEmptyBricks = 0
      var localUnifiedIndexCount = 0  // unified index write count
      var localSkippedUnifiedIndexCount = 0  // Skipped (all NoData) index count
      var localSkippedUnifiedIndexDueToStepSize = 0  // Skipped (step size mismatch) index count
      var localFailReasons = scala.collection.mutable.ListBuffer[String]()
      
      // Step size validation tolerance
      val epsilon = 1e-9

      // Define brick dimensions
      val Bx = 16
      val By = 16
      val Bz = 8

      iter.foreach { line =>
        localTotalLines += 1

        VolumeModelRecord.parseLine(line, delimiter) match {
          case Right(record) =>
            localSuccessCount += 1
            localModelTypeCount(record.modelType) = localModelTypeCount.getOrElse(record.modelType, 0) + 1

            // Decode payload
            var valuesModel: Array[Float] = null
            var decodeError: Option[String] = None

            try {
              valuesModel = PorosityPayloadCodec.decodeToFloatArray(record.payloadB64, record.nx, record.ny, record.nz)
            } catch {
              case e: Exception =>
                localFailCount += 1
                localFailReasons += s"Payload decode failed: ${e.getMessage}"
            }

            decodeError match {
              case Some(error) =>
                localFailReasons += error
              case None =>
                if (valuesModel.length != record.nx * record.ny * record.nz) {
                  localFailCount += 1
                  localFailReasons += s"Payload length (${valuesModel.length}) does not match nx*ny*nz (${record.nx * record.ny * record.nz})"
                } else {
              // Write to meta table
              val metaRowKey = s"${record.modelId}|${record.timeMillis}"
              val metaPut = new Put(Bytes.toBytes(metaRowKey))
              metaPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("model_type"), Bytes.toBytes(record.modelType))
              metaPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("time_iso"), Bytes.toBytes(record.timeIso))
              metaPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("time_millis"), Bytes.toBytes(record.timeMillis))
              metaPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("bbox"), Bytes.toBytes(s"${record.lonMin},${record.latMin},${record.zMin},${record.lonMax},${record.latMax},${record.zMax}"))
              metaPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("nx"), Bytes.toBytes(record.nx))
              metaPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("ny"), Bytes.toBytes(record.ny))
              metaPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("nz"), Bytes.toBytes(record.nz))
              metaPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("unified_level"), Bytes.toBytes(unifiedLevel.getOrElse(4).toLong))
              
              metaBufferedMutator.mutate(metaPut)

              // Split into bricks
              val xBlocks = math.ceil(record.nx.toDouble / Bx).toInt
              val yBlocks = math.ceil(record.ny.toDouble / By).toInt
              val zBlocks = math.ceil(record.nz.toDouble / Bz).toInt
              
              // Calculate model's spatial step size (used for zCell calculation)
              val dlon = (record.lonMax - record.lonMin) / record.nx
              val dlat = (record.latMax - record.latMin) / record.ny
              val dz = (record.zMax - record.zMin) / record.nz
              
              // Step size validation: check if derived step size matches global fixed step size
              val stepSizeValid = math.abs(dlon - VolumeZCell.FIXED_DLON) < epsilon &&
                                  math.abs(dlat - VolumeZCell.FIXED_DLAT) < epsilon &&
                                  math.abs(dz - VolumeZCell.FIXED_DZ) < epsilon
              
              // Print step size validation result for the first record
              if (localUnifiedIndexCount == 0 && shouldWriteUnifiedIndex) {
                println(f"[Voxel Import] Global fixed step size: dlon=${VolumeZCell.FIXED_DLON}%.10f, dlat=${VolumeZCell.FIXED_DLAT}%.10f, dz=${VolumeZCell.FIXED_DZ}%.10f")
                println(f"[Voxel Import] Model ${record.modelId} derived step size: dlon=$dlon%.10f, dlat=$dlat%.10f, dz=$dz%.10f")
                println(s"[Voxel Import] Step size validation result: ${if (stepSizeValid) "passed" else "failed (will skip unified index write)"}")
              }

              for (i <- 0 until xBlocks) {
                for (j <- 0 until yBlocks) {
                  for (k <- 0 until zBlocks) {
                    val startX = i * Bx
                    val endX = math.min(startX + Bx, record.nx)
                    val startY = j * By
                    val endY = math.min(startY + By, record.ny)
                    val startZ = k * Bz
                    val endZ = math.min(startZ + Bz, record.nz)

                    // Create brick array, fill with NoData
                    val brickArray = Array.fill(Bx * By * Bz)(-9999f)

                    // Copy data
                    var isAllEmpty = true
                    for (x <- startX until endX) {
                      for (y <- startY until endY) {
                        for (z <- startZ until endZ) {
                          val modelIdx = z * record.nx * record.ny + y * record.nx + x
                          val brickX = x - startX
                          val brickY = y - startY
                          val brickZ = z - startZ
                          val brickIdx = brickZ * Bx * By + brickY * Bx + brickX
                          brickArray(brickIdx) = valuesModel(modelIdx)
                          
                          if (valuesModel(modelIdx) != -9999f) {
                            isAllEmpty = false
                          }
                        }
                      }
                    }

                    // Build brick rowkey
                    // Remove tile_ prefix, unify to pure numeric tile format: modelId|timeMillis|tileI|tileJ|tileK
                    // Purpose: allow unified's cf:dk to be directly used as brick table rowkey for lookup, reducing extra string manipulation and format inconsistency risks
                    val brickRowKey = s"${record.modelId}|${record.timeMillis}|${i}|${j}|${k}"
                    val brickPut = new Put(Bytes.toBytes(brickRowKey))
                    brickPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("tile_i"), Bytes.toBytes(i))
                    brickPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("tile_j"), Bytes.toBytes(j))
                    brickPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("tile_k"), Bytes.toBytes(k))
                    
                    // Compress brick data
                    val compressedBase64 = PorosityPayloadCodec.encodeFromFloatArray(brickArray, Bx, By, Bz)
                    val compressedBytes = java.util.Base64.getDecoder.decode(compressedBase64)
                    brickPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("payload_bytes"), compressedBytes)
                    
                    brickBufferedMutator.mutate(brickPut)
                    localTotalBricks += 1
                    
                    if (isAllEmpty) {
                      localEmptyBricks += 1
                    } else {
                      // Write unified index (only for bricks that are not entirely NoData)
                      if (shouldWriteUnifiedIndex && unifiedIdxBufferedMutator != null) {
                        // Step size validation: if step size mismatch, skip unified index write
                        if (!stepSizeValid) {
                          localSkippedUnifiedIndexDueToStepSize += 1
                          // Print warning only once
                          if (localSkippedUnifiedIndexDueToStepSize == 1) {
                            println(s"[WARN] Model ${record.modelId} step size does not match global fixed step size, skipping unified index write")
                            println(f"[WARN] Derived step size: dlon=$dlon%.10f, dlat=$dlat%.10f, dz=$dz%.10f")
                            println(f"[WARN] Fixed step size: dlon=${VolumeZCell.FIXED_DLON}%.10f, dlat=${VolumeZCell.FIXED_DLAT}%.10f, dz=${VolumeZCell.FIXED_DZ}%.10f")
                          }
                        } else {
                          // Calculate brick bbox
                          val brickLonMin = record.lonMin + startX * dlon
                          val brickLatMin = record.latMin + startY * dlat
                          val brickZMin = record.zMin + startZ * dz
                          
                          // Calculate zCell (using global fixed step size)
                          val zCell = VolumeZCell.computeZCellFromBrickBBox(brickLonMin, brickLatMin, brickZMin)
                          
                          // Calculate dayBucket and timeOfDay (UTC, dayBucket in yyyyMMdd format)
                          val instant = VolumeTimeBucketUtc.parseIsoZ(record.timeIso)
                          val dayBucket = VolumeTimeBucketUtc.dayBucket(instant)  // yyyyMMdd format, e.g. 20251105
                          val timeOfDay = VolumeTimeBucketUtc.timeOfDay(instant)
                          
                          // Print first record's dayBucket example (for verification)
                          if (localUnifiedIndexCount == 0) {
                            println(s"[Voxel Import] First unified index record dayBucket (yyyyMMdd): $dayBucket, timeIso: ${record.timeIso}")
                          }
                          
                          // Build unified index rowkey
                          val unifiedRowKey = VolumeUnifiedIndexKey.toRowKeyBytes(
                            record.modelType, zCell, dayBucket, timeOfDay,
                            record.modelId, record.timeMillis, i, j, k
                          )
                          
                          // Write unified index (HBase does not allow empty Put, must write at least one column; rowkey for sorted retrieval, dk as pointer placeholder)
                          val blockKeyStr = s"${record.modelId}|${record.timeMillis}|$i|$j|$k"
                          val unifiedPut = new Put(unifiedRowKey)
                          unifiedPut.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"), Bytes.toBytes(blockKeyStr))
                          unifiedIdxBufferedMutator.mutate(unifiedPut)
                          localUnifiedIndexCount += 1
                        }
                      }
                    }
                    
                    // Batch flush
                    if (localTotalBricks % batchSize == 0) {
                      metaBufferedMutator.flush()
                      brickBufferedMutator.flush()
                      if (unifiedIdxBufferedMutator != null) {
                        unifiedIdxBufferedMutator.flush()
                      }
                    }
                  }
                }
              }
            }
            }

          case Left(error) =>
            localFailCount += 1
            localFailReasons += error
        }
      }

      // Flush remaining data
      metaBufferedMutator.flush()
      brickBufferedMutator.flush()
      if (unifiedIdxBufferedMutator != null) {
        unifiedIdxBufferedMutator.flush()
      }

      // Close resources
      metaTable.close()
      brickTable.close()
      metaBufferedMutator.close()
      brickBufferedMutator.close()
      if (unifiedIdxBufferedMutator != null) {
        unifiedIdxBufferedMutator.close()
      }
      connection.close()

      // Return statistics
      Iterator((localTotalLines, localSuccessCount, localFailCount, localModelTypeCount, localTotalBricks, localEmptyBricks, localUnifiedIndexCount, localSkippedUnifiedIndexCount, localSkippedUnifiedIndexDueToStepSize, localFailReasons))
    }.collect()

    // Merge statistics
    results.foreach { case (tLines, sCount, fCount, mTypeCount, tBricks, eBricks, uIdxCount, sIdxCount, sIdxDueToStepSize, fReasons) =>
      totalLines += tLines
      successCount += sCount
      failCount += fCount
      totalBricks += tBricks
      emptyBricks += eBricks
      unifiedIndexCount += uIdxCount
      skippedUnifiedIndexCount += sIdxCount
      skippedUnifiedIndexDueToStepSize += sIdxDueToStepSize
      
      mTypeCount.foreach { case (modelType, count) =>
        modelTypeCount(modelType) = modelTypeCount.getOrElse(modelType, 0) + count
      }
      
      if (fReasons.nonEmpty) {
        failReasons ++= fReasons.take(10) // Keep only first 10 error reasons
      }
    }

    // Output statistics
    println("\n" + "=" * 80)
    println(s"[Voxel Import] Statistics:")
    println(s"  → Total lines: $totalLines")
    println(s"  → Successfully parsed models: $successCount")
    println(s"  → Failed parse lines: $failCount")
    println(s"  → Statistics by model type:")
    modelTypeCount.foreach { case (modelType, count) =>
      println(s"    - $modelType: $count")
    }
    println(s"  → Total bricks: $totalBricks")
    println(s"  → Empty bricks: $emptyBricks")
    if (successCount > 0) {
      println(s"  → Average bricks per model: ${totalBricks.toDouble / successCount}")
    }
    if (shouldWriteUnifiedIndex) {
      println(s"  → Unified index write count: $unifiedIndexCount")
      println(s"  → Skipped all-NoData index count: $skippedUnifiedIndexCount")
      println(s"  → Skipped step-size-mismatch index count: $skippedUnifiedIndexDueToStepSize")
    }
    println(s"  → Sample failure reasons (first 10):")
    failReasons.take(10).foreach { reason =>
      println(s"    - $reason")
    }
    println("=" * 80 + "\n")
    
    // WARN: If data was previously written to unified table with incorrect step size, need to re-import with new dataset or clear unified table before re-importing
    if (shouldWriteUnifiedIndex && skippedUnifiedIndexDueToStepSize > 0) {
      println("\n" + "=" * 80)
      println("[WARN] Important: Models with inconsistent step size detected, unified index write skipped")
      println("  If data was previously written to unified table with incorrect step size, re-import with new dataset or clear unified table before re-importing,")
      println("  otherwise new queries may still miss old data (because zCell grid is different).")
      println("=" * 80 + "\n")
    }
  }
}