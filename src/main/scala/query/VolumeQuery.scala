package query

import model._
import index.{VolumeUnifiedIndexKey, VolumeZCell, VolumeTimeBucketUtc}
import storage.HBaseTableManager
import org.apache.hadoop.hbase.client.{Connection, Get, Scan}
import org.apache.hadoop.hbase.util.Bytes
import java.util.Base64
import scala.collection.mutable.ArrayBuffer

/**
 * Volume query condition
 */
sealed trait VolumeCondition

case class ModelTypeEquals(modelType: String) extends VolumeCondition
case class TimeRange(startMs: Long, endMs: Long) extends VolumeCondition
case class SpatialBBox(lonMin: Double, latMin: Double, zMin: Double,
                       lonMax: Double, latMax: Double, zMax: Double) extends VolumeCondition

/**
 * Volume query statistics (read-only, for exporting to TXT)
 */
case class VolumeQueryStats(
  modelType: String,
  attrHash: Int,
  timeRange: (Long, Long),
  bbox: (Double, Double, Double, Double, Double, Double),
  unifiedTableName: String,
  brickTableName: String,
  metaTableName: String,
  zCellsCount: Int,
  dayBucketsCount: Int,
  dayBucketRange: (Int, Int),
  scanTaskCount: Int,
  scanDurationMs: Long,
  totalUnifiedRows: Int,
  totalDkCount: Int,
  totalBrickSuccess: Int,
  oldFormatCompatCount: Int,
  resultCount: Int
)

/**
 * Volume query class (supports unified index)
 */
class VolumeQuery(
  zkQuorum: String,
  enableUnifiedIndex: Boolean = true,
  useUnifiedParallel: Boolean = false,
  unifiedThreadPoolSize: Int = 8,
  dataset: Option[String] = None,
  unifiedLevel: Option[Int] = None
) {
  
  private val hbaseConn: Connection = HBaseTableManager.createConnection(zkQuorum)
  
  // Get table names (dynamically constructed via HBaseTableManager, hardcoding strictly prohibited)
  private val unifiedTableName = HBaseTableManager.volumeUnifiedIdxTableName(dataset, unifiedLevel)
  private val brickTableName = HBaseTableManager.volumeBrickTableName(dataset)
  private val metaTableName = HBaseTableManager.volumeMetaTableName(dataset)
  
  // Statistics (read-only)
  private var queryStats: Option[VolumeQueryStats] = None
  
  /**
   * Get query statistics (read-only)
   */
  def getQueryStats: Option[VolumeQueryStats] = queryStats
  
  /**
   * Execute query
   */
  def query(conditions: Seq[VolumeCondition]): Seq[VolumeBrickResult] = {
    if (enableUnifiedIndex) {
      queryUsingUnifiedIndex(conditions)
    } else {
      queryUsingIncrementalFilter(conditions)
    }
  }
  
  /**
   * Query using unified index
   */
  private def queryUsingUnifiedIndex(conditions: Seq[VolumeCondition]): Seq[VolumeBrickResult] = {
    val results = ArrayBuffer[VolumeBrickResult]()
    
    // Extract query conditions
    val modelTypeOpt = conditions.collectFirst { case ModelTypeEquals(mt) => mt }
    val timeRangeOpt = conditions.collectFirst { case TimeRange(s, e) => (s, e) }
    val bboxOpt = conditions.collectFirst { 
      case SpatialBBox(lonMin, latMin, zMin, lonMax, latMax, zMax) => 
        (lonMin, latMin, zMin, lonMax, latMax, zMax)
    }
    
    if (modelTypeOpt.isEmpty || timeRangeOpt.isEmpty || bboxOpt.isEmpty) {
      println("Error: Missing required query conditions")
      return results.toSeq
    }
    
    val modelType = modelTypeOpt.get
    val (startMs, endMs) = timeRangeOpt.get
    val (lonMin, latMin, zMin, lonMax, latMax, zMax) = bboxOpt.get
    
    // Calculate attrHash
    val attrHash = VolumeUnifiedIndexKey.fnv1a32(modelType)
    
    println("\n" + "=" * 80)
    println("Executing unified index query")
    println("=" * 80)
    println(s"  model_type: $modelType")
    println(s"  attrHash: $attrHash (decimal) / 0x${attrHash.toHexString} (hex)")
    println(s"  time_range: $startMs ~ $endMs")
    println(f"  bbox: ($lonMin%.6f, $latMin%.6f, $zMin%.2f) ~ ($lonMax%.6f, $latMax%.6f, $zMax%.2f)")
    println(s"  unified table: $unifiedTableName")
    println(s"  brick table: $brickTableName")
    println(s"  meta table: $metaTableName")
    
    // WARN notice: dayBucket encoding switched from epochDay to yyyyMMdd
    println("\n" + "=" * 80)
    println("[WARN] Important: dayBucket encoding has switched from epochDay to yyyyMMdd format")
    println("  Old unified index table data will not be matched by new queries!")
    println("  You must re-import data or rebuild the unified index table.")
    println("  Recommend using a new dataset or clearing the old unified table before re-importing.")
    println("=" * 80 + "\n")
    
    // WARN notice: fixed step size change
    println("\n" + "=" * 80)
    println("[WARN] Important: volume unified index has switched to global fixed step size")
    println(f"  Fixed step size: dlon=${VolumeZCell.FIXED_DLON}%.10f, dlat=${VolumeZCell.FIXED_DLAT}%.10f, dz=${VolumeZCell.FIXED_DZ}%.10f")
    println("  If you have previously written to the unified table with incorrect step size, you need to re-import using a new dataset or clear the unified table before re-importing,")
    println("  otherwise new queries may not match old data (due to different zCell grids).")
    println("=" * 80 + "\n")
    
    // Use VolumeZCell to enumerate zCells covered by query bbox (force use of global fixed step size)
    // zCell grid must be unified across the entire dataset, derivation based on query bbox or model local step size is prohibited, otherwise it will cause index grid misalignment
    val zCells = VolumeZCell.enumerateZCellsForBBox(
      lonMin, latMin, zMin, lonMax, latMax, zMax
    )
    
    // Get fixed step size for log output
    val dlon = VolumeZCell.FIXED_DLON
    val dlat = VolumeZCell.FIXED_DLAT
    val dz = VolumeZCell.FIXED_DZ
    
    println(s"  Enumerated zCells count: ${zCells.size}")
    println(s"  Using global fixed step size: dlon=$dlon, dlat=$dlat, dz=$dz")
    
    println("\n" + "=" * 80)
    println("Diagnostic log, only for troubleshooting step size/enumeration issues")
    println("=" * 80)
    
    println("\n----- ZCELL DIAG: Section 1: Runtime step size and brick span -----")
    println(f"  dlon   = $dlon%.10f (global fixed step size)")
    println(f"  dlat   = $dlat%.10f (global fixed step size)")
    println(f"  dz     = $dz%.10f (global fixed step size)")
    println(s"  Bx     = ${VolumeZCell.Bx}")
    println(s"  By     = ${VolumeZCell.By}")
    println(s"  Bz     = ${VolumeZCell.Bz}")
    val brickDlon = dlon * VolumeZCell.Bx
    val brickDlat = dlat * VolumeZCell.By
    val brickDz = dz * VolumeZCell.Bz
    println(f"  brickDlon = $brickDlon%.10f (dlon*Bx)")
    println(f"  brickDlat = $brickDlat%.10f (dlat*By)")
    println(f"  brickDz   = $brickDz%.10f (dz*Bz)")
    println(s"  GLOBAL_LON0 = ${VolumeZCell.GLOBAL_LON0}")
    println(s"  GLOBAL_LAT0 = ${VolumeZCell.GLOBAL_LAT0}")
    println(s"  GLOBAL_Z0   = ${VolumeZCell.GLOBAL_Z0}")
    
    println("\n----- ZCELL DIAG: Section 2: brick index range and theoretical zCell count -----")
    val bxStart = math.floor((lonMin - VolumeZCell.GLOBAL_LON0) / brickDlon).toLong
    val bxEnd = math.floor((lonMax - VolumeZCell.GLOBAL_LON0) / brickDlon).toLong
    val byStart = math.floor((latMin - VolumeZCell.GLOBAL_LAT0) / brickDlat).toLong
    val byEnd = math.floor((latMax - VolumeZCell.GLOBAL_LAT0) / brickDlat).toLong
    val bzStart = math.floor((zMin - VolumeZCell.GLOBAL_Z0) / brickDz).toLong
    val bzEnd = math.floor((zMax - VolumeZCell.GLOBAL_Z0) / brickDz).toLong
    println(s"  bxStart..bxEnd = $bxStart .. $bxEnd")
    println(s"  byStart..byEnd = $byStart .. $byEnd")
    println(s"  bzStart..bzEnd = $bzStart .. $bzEnd")
    val theoreticalCount = (bxEnd - bxStart + 1) * (byEnd - byStart + 1) * (bzEnd - bzStart + 1)
    println(s"  Theoretical zCell count = $theoreticalCount ((bxEnd-bxStart+1) * (byEnd-byStart+1) * (bzEnd-bzStart+1))")
    println(s"  Actual enumerated count   = ${zCells.size}")
    if (theoreticalCount != zCells.size) {
      println("  [WARN] Step size/boundary calculation may be inconsistent (theoretical count does not match actual count)")
    }
    
    println("\n----- ZCELL DIAG: Section 3: Verify inclusion (critical) -----")
    val knownZCell = 0x4b0910c0L
    val containsKnownZCell = zCells.contains(knownZCell)
    println(s"  Known existing zCell (knownZCell) = $knownZCell (decimal) / 0x${knownZCell.toHexString} (hex)")
    println(s"  zCells.contains(knownZCell) = $containsKnownZCell")
    if (!containsKnownZCell) {
      println("  [WARN] Enumerated zCells do not contain known existing zCell, indicating runtime step size/origin/enumeration scope is inconsistent with write side, resulting in scan returning 0.")
      println("  [WARN] GLOBAL origin may be inconsistent or data was not written with fixed step size, please check import logs.")
    }
    
    println("\n----- ZCELL DIAG: Section 4: First 10 zCell samples from enumeration set -----")
    val sampleSize = math.min(10, zCells.size)
    for (i <- 0 until sampleSize) {
      val zCell = zCells(i)
      println(s"  zCell[$i] = $zCell (decimal) / 0x${zCell.toHexString} (hex)")
    }
    
    println("=" * 80 + "\n")
    
    // Use VolumeTimeBucketUtc (UTC) to calculate time bucket range (yyyyMMdd format)
    val dayBuckets = VolumeTimeBucketUtc.enumerateDays(startMs, endMs)
    
    println(s"  Enumerated dayBuckets count: ${dayBuckets.size}")
    println(s"  day_bucket_range (yyyyMMdd): ${dayBuckets.head} ~ ${dayBuckets.last}")
    
    // Build scan ranges (Cartesian product of zCell × dayBucket)
    val scanRanges = scala.collection.mutable.ArrayBuffer[(Array[Byte], Array[Byte])]()
    
    for (zCell <- zCells) {
      for (dayBucket <- dayBuckets) {
        // Build scan range for each (zCell, dayBucket) combination
        val (startRow, stopRow) = VolumeUnifiedIndexKey.buildScanRangeForDay(
          modelType, zCell, dayBucket
        )
        scanRanges += ((startRow, stopRow))
      }
    }
    
    println(s"  Total scan tasks: ${scanRanges.size}")
    
    // Execute scan
    val unifiedTable = hbaseConn.getTable(org.apache.hadoop.hbase.TableName.valueOf(unifiedTableName))
    val brickTable = hbaseConn.getTable(org.apache.hadoop.hbase.TableName.valueOf(brickTableName))
    
    var totalUnifiedRows = 0
    var totalDkCount = 0
    var totalBrickGets = 0
    var totalBrickSuccess = 0
    var oldFormatCompatCount = 0
    var scanStartTime = 0L
    var scanDurationMs = 0L
    
    try {
      scanStartTime = System.currentTimeMillis()
      
      scanRanges.zipWithIndex.foreach { case ((startRow, stopRow), idx) =>
        // Remove intermediate progress logs to avoid flooding output
        // println(s"  Executing scan ${idx + 1}/${scanRanges.size}...")
        
        val scan = new Scan()
        scan.setStartRow(startRow)
        scan.setStopRow(stopRow)
        scan.setCaching(1000)
        scan.setBatch(100)
        
        val scanner = unifiedTable.getScanner(scan)
        try {
          val iterator = scanner.iterator()
          var scanUnifiedRows = 0
          var scanDkCount = 0
          var scanBrickSuccess = 0
          
          while (iterator.hasNext) {
            val result = iterator.next()
            totalUnifiedRows += 1
            scanUnifiedRows += 1
            
            try {
              // Read cf:dk from unified table (column family cf, qualifier dk)
              val dkBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("dk"))
              
              if (dkBytes != null) {
                totalDkCount += 1
                scanDkCount += 1
                
                val blockKey = new String(dkBytes, java.nio.charset.StandardCharsets.UTF_8)
                
                // Secondary filter: check time range (parsed from blockKey)
                val parts = blockKey.split("\\|")
                if (parts.length == 5) {
                  val blockTimeMillis = parts(1).toLong
                  
                  if (blockTimeMillis >= startMs && blockTimeMillis <= endMs) {
                    // Look up brick table to read cf:payload_bytes
                    // Temporary compatibility strategy: first try new format (no tile_), if Get returns empty then try old format (with tile_)
                    val brickGet = new Get(Bytes.toBytes(blockKey))
                    brickGet.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("payload_bytes"))
                    
                    val brickResult = brickTable.get(brickGet)
                    var payloadBytes = brickResult.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("payload_bytes"))
                    
                    totalBrickGets += 1
                    
                    // If new format fetch fails, try old format (with tile_ prefix)
                    if (payloadBytes == null) {
                      val modelId = parts(0)
                      val timeMillis = parts(1)
                      val tileI = parts(2)
                      val tileJ = parts(3)
                      val tileK = parts(4)
                      
                      // Old format: modelId|timeMillis|tile_i|tile_j|tile_k
                      val oldFormatBlockKey = s"${modelId}|${timeMillis}|tile_${tileI}|${tileJ}|${tileK}"
                      val oldFormatGet = new Get(Bytes.toBytes(oldFormatBlockKey))
                      oldFormatGet.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("payload_bytes"))
                      
                      val oldFormatResult = brickTable.get(oldFormatGet)
                      payloadBytes = oldFormatResult.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("payload_bytes"))
                      
                      if (payloadBytes != null) {
                        oldFormatCompatCount += 1
                        println(s"    [WARN] Triggered old rowkey format compatibility (with tile_ prefix), recommend re-importing data to unify rowkey")
                      }
                    }
                    
                    if (payloadBytes != null) {
                      totalBrickSuccess += 1
                      scanBrickSuccess += 1
                      
                      val payloadB64 = Base64.getEncoder.encodeToString(payloadBytes)
                      
                      // Parse blockKey to get brick information
                      val modelId = parts(0)
                      val blockTimeMillis = parts(1).toLong
                      val tileI = parts(2).toInt
                      val tileJ = parts(3).toInt
                      val tileK = parts(4).toInt
                      
                      // Calculate brick spatial range (simplified, using original bbox)
                      val brickLonMin = lonMin
                      val brickLatMin = latMin
                      val brickZMin = zMin
                      val brickLonMax = lonMax
                      val brickLatMax = latMax
                      val brickZMax = zMax
                      
                      results += VolumeBrickResult(
                        modelId = modelId,
                        timeIso = java.time.Instant.ofEpochMilli(blockTimeMillis).toString,
                        timeMillis = blockTimeMillis,
                        modelType = modelType,
                        tileI = tileI,
                        tileJ = tileJ,
                        tileK = tileK,
                        lonMin = brickLonMin,
                        latMin = brickLatMin,
                        zMin = brickZMin,
                        lonMax = brickLonMax,
                        latMax = brickLatMax,
                        zMax = brickZMax,
                        nx = 1,  // TODO: Parse from brick data
                        ny = 1,
                        nz = 1,
                        payloadB64 = payloadB64
                      )
                    }
                  }
                }
              }
            } catch {
              case e: Exception =>
                println(s"    Warning: Failed to process unified row: ${e.getMessage}")
            }
          }
          
          // Remove single scan result log to avoid flooding output
          // println(s"    Scan completed: unified rows=$scanUnifiedRows, dk count=$scanDkCount, successful brick lookups=$scanBrickSuccess")
          
        } finally {
          scanner.close()
        }
      }
      
      val scanEndTime = System.currentTimeMillis()
      val scanDurationMs = scanEndTime - scanStartTime
      
      println("\n" + "-" * 80)
      println("Query statistics:")
      println(s"  Total scan tasks: ${scanRanges.size}")
      println(s"  Total scan time: ${scanDurationMs} ms (${scanDurationMs / 1000.0} s)")
      println(s"  Actually scanned unified rows: $totalUnifiedRows")
      println(s"  Read dk count: $totalDkCount")
      println(s"  Successful brick lookups: $totalBrickSuccess")
      if (oldFormatCompatCount > 0) {
        println(s"  [WARN] Old rowkey format compatibility triggered count: $oldFormatCompatCount")
      }
      println("-" * 80)
      
    } finally {
      unifiedTable.close()
      brickTable.close()
    }
    
    println(s"\nQuery completed, returned ${results.size} brick records")
    println("=" * 80 + "\n")
    
    // Save statistics (read-only)
    queryStats = Some(VolumeQueryStats(
      modelType = modelType,
      attrHash = attrHash,
      timeRange = (startMs, endMs),
      bbox = (lonMin, latMin, zMin, lonMax, latMax, zMax),
      unifiedTableName = unifiedTableName,
      brickTableName = brickTableName,
      metaTableName = metaTableName,
      zCellsCount = zCells.size,
      dayBucketsCount = dayBuckets.size,
      dayBucketRange = (dayBuckets.head, dayBuckets.last),
      scanTaskCount = scanRanges.size,
      scanDurationMs = scanDurationMs,
      totalUnifiedRows = totalUnifiedRows,
      totalDkCount = totalDkCount,
      totalBrickSuccess = totalBrickSuccess,
      oldFormatCompatCount = oldFormatCompatCount,
      resultCount = results.size
    ))
    
    results.toSeq
  }
  
  /**
   * Query using incremental filter (fallback)
   */
  private def queryUsingIncrementalFilter(conditions: Seq[VolumeCondition]): Seq[VolumeBrickResult] = {
    println("\n[Warning] Incremental filter query not yet implemented, please use unified index")
    Seq.empty
  }
  
  /**
   * Parse blockKey to get brick information
   */
  private def parseBlockKeyInfo(blockKey: String): BrickInfo = {
    // blockKey format: modelId_timeIso_tileI_tileJ_tileK_nx_ny_nz
    val parts = blockKey.split("_")
    if (parts.length >= 8) {
      BrickInfo(
        modelId = parts(0),
        timeIso = parts(1),
        tileI = parts(2).toInt,
        tileJ = parts(3).toInt,
        tileK = parts(4).toInt,
        nx = parts(5).toInt,
        ny = parts(6).toInt,
        nz = parts(7).toInt
      )
    } else {
      // Default values
      BrickInfo(
        modelId = "unknown",
        timeIso = "unknown",
        tileI = 0,
        tileJ = 0,
        tileK = 0,
        nx = 1,
        ny = 1,
        nz = 1
      )
    }
  }
  
  /**
   * Close query and release resources
   */
  def close(): Unit = {
    hbaseConn.close()
  }
}

/**
 * Brick information
 */
private case class BrickInfo(
  modelId: String,
  timeIso: String,
  tileI: Int,
  tileJ: Int,
  tileK: Int,
  nx: Int,
  ny: Int,
  nz: Int
)
