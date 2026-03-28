// src/main/scala/index/Z3DEncoder.scala

package index

import scala.collection.mutable.ListBuffer

object Z3DEncoder {
  // Basic configuration
  val LON_MIN = 110.0; val LON_MAX = 120.0
  val LAT_MIN = 21.0;  val LAT_MAX = 25.0
  val ALT_MIN = 0.0;   val ALT_MAX = 1000.0
  val BITS_PER_DIM = 21
  val MAX_VALUE = (1 << BITS_PER_DIM) - 1

  // ============= Unified index block ID related configuration =============
  // Unified index uses a fixed block level to ensure block IDs are consistent between import and query
  val UNIFIED_BLOCK_LEVEL = 19
  val UNIFIED_BLOCK_SIZE = 1 << (BITS_PER_DIM - UNIFIED_BLOCK_LEVEL)

  /**
   * Calculates block ID for unified index (with specified level)
   * Maps a point coordinate (lon, lat, alt) to a block at the specified level and encodes it as a Z3D block ID
   *
   * Block ID property: all points within the same block get the same block ID
   * This allows import and query to match using the same zCell value
   */
  def blockIdForUnifiedIndexAtLevel(level: Int,
                                    lon: Double, lat: Double, alt: Double): Long = {
    val (x, y, z) = normalize(lon, lat, alt)
    val cellSize = 1 << (BITS_PER_DIM - level)
    val xLo = (x / cellSize) * cellSize
    val yLo = (y / cellSize) * cellSize
    val zLo = (z / cellSize) * cellSize
    split(xLo) | (split(yLo) << 1) | (split(zLo) << 2)
  }

  /**
   * Calculates block ID for unified index (using default fixed level)
   * Wrapper for blockIdForUnifiedIndexAtLevel, maintains backward compatibility
   */
  def blockIdForUnifiedIndex(lon: Double, lat: Double, alt: Double): Long =
    blockIdForUnifiedIndexAtLevel(UNIFIED_BLOCK_LEVEL, lon, lat, alt)

  /**
   * Converts a Long value to a big-endian byte array (8 bytes)
   */
  def longToBytes(value: Long): Array[Byte] = {
    Array(
      ((value >> 56) & 0xFF).toByte,
      ((value >> 48) & 0xFF).toByte,
      ((value >> 40) & 0xFF).toByte,
      ((value >> 32) & 0xFF).toByte,
      ((value >> 24) & 0xFF).toByte,
      ((value >> 16) & 0xFF).toByte,
      ((value >> 8) & 0xFF).toByte,
      (value & 0xFF).toByte
    )
  }

  /**
   * Converts a byte array back to a Long value
   */
  def bytesToLong(bytes: Array[Byte]): Long = {
    ((bytes(0) & 0xFFL) << 56) |
    ((bytes(1) & 0xFFL) << 48) |
    ((bytes(2) & 0xFFL) << 40) |
    ((bytes(3) & 0xFFL) << 32) |
    ((bytes(4) & 0xFFL) << 24) |
    ((bytes(5) & 0xFFL) << 16) |
    ((bytes(6) & 0xFFL) << 8) |
    (bytes(7) & 0xFFL)
  }

  def normalize(lon: Double, lat: Double, alt: Double): (Int, Int, Int) = {
    val normLon = math.max(0, math.min(MAX_VALUE, ((lon - LON_MIN) / (LON_MAX - LON_MIN) * MAX_VALUE).toInt))
    val normLat = math.max(0, math.min(MAX_VALUE, ((lat - LAT_MIN) / (LAT_MAX - LAT_MIN) * MAX_VALUE).toInt))
    val normAlt = math.max(0, math.min(MAX_VALUE, ((alt - ALT_MIN) / (ALT_MAX - ALT_MIN) * MAX_VALUE).toInt))
    (normLon, normLat, normAlt)
  }

  def encode(lon: Double, lat: Double, alt: Double): Long = {
    val (x, y, z) = normalize(lon, lat, alt)
    split(x) | (split(y) << 1) | (split(z) << 2)
  }

  private def split(value: Int): Long = {
    var x = value.toLong
    var res = 0L
    for (i <- 0 until BITS_PER_DIM) res |= ((x >> i) & 1L) << (3 * i)
    res
  }

  /**
   * [Fixed version] Generates a list of Z3D ranges covering a BBox
   *
   * Fixes:
   * 1. Uses explicit boundary coordinates (xLo, yLo, zLo, xHi, yHi, zHi) instead of level
   * 2. Correctly handles boundary conditions for child node subdivision
   * 3. Avoids missing boundary points
   */
  def ranges(lonMin: Double, latMin: Double, altMin: Double,
           lonMax: Double, latMax: Double, altMax: Double,
           maxRanges: Int = 2000): Seq[(Long, Long)] = {

  // 1. First normalize to [0, MAX_VALUE]
  val (xMin, yMin, zMin) = normalize(lonMin, latMin, altMin)
  val (xMax, yMax, zMax) = normalize(lonMax, latMax, altMax)

  val results = new ListBuffer[(Long, Long)]()

  // Node with explicit boundaries
  case class Node(xLo: Int, yLo: Int, zLo: Int, xHi: Int, yHi: Int, zHi: Int)

  val stack = scala.collection.mutable.Stack[Node]()

  // Root node covers the entire discrete space
  stack.push(Node(0, 0, 0, MAX_VALUE, MAX_VALUE, MAX_VALUE))

  while (stack.nonEmpty) {
    val node = stack.pop()

    // 2. Check if it intersects with the query box (closed interval)
    val intersects =
      xMax >= node.xLo && xMin <= node.xHi &&
      yMax >= node.yLo && yMin <= node.yHi &&
      zMax >= node.zLo && zMin <= node.zHi

    if (intersects) {
      // 3. Check if the current node is fully contained by the query box
      val contained =
        xMin <= node.xLo && xMax >= node.xHi &&
        yMin <= node.yLo && yMax >= node.yHi &&
        zMin <= node.zLo && zMax >= node.zHi

      // 4. Check if already subdivided to a single point
      val isLeaf =
        node.xLo == node.xHi &&
        node.yLo == node.yHi &&
        node.zLo == node.zHi

      // 5. Decision: output range or continue subdivision
      //
      // Three cases where we must output directly (no further subdivision):
      //   a) Fully contained -> safe to output the entire block
      //   b) Already a single point -> cannot subdivide further
      //   c) maxRanges limit reached -> output a coarser block to avoid missing data
      if (contained || isLeaf || results.size >= maxRanges) {
        val minId =
          split(node.xLo) |
          (split(node.yLo) << 1) |
          (split(node.zLo) << 2)

        val maxId =
          split(node.xHi) |
          (split(node.yHi) << 1) |
          (split(node.zHi) << 2)

        results += ((minId, maxId))
      } else {
        // 6. Can still subdivide and range count not yet at limit -> octree split into 8 child blocks
        val xMid = node.xLo + (node.xHi - node.xLo) / 2
        val yMid = node.yLo + (node.yHi - node.yLo) / 2
        val zMid = node.zLo + (node.zHi - node.zLo) / 2

        // Guard: if a dimension has already degenerated to a single point, do not split in that dimension
        val canSplitX = node.xLo < node.xHi
        val canSplitY = node.yLo < node.yHi
        val canSplitZ = node.zLo < node.zHi

        // Push 8 child blocks onto stack (note: Stack's pop order is reverse of push order)
        for (k <- 0 until 8) {
          val (xl, xh) =
            if (!canSplitX) (node.xLo, node.xHi)
            else if ((k & 1) == 0) (node.xLo, xMid) else (xMid + 1, node.xHi)

          val (yl, yh) =
            if (!canSplitY) (node.yLo, node.yHi)
            else if ((k & 2) == 0) (node.yLo, yMid) else (yMid + 1, node.yHi)

          val (zl, zh) =
            if (!canSplitZ) (node.zLo, node.zHi)
            else if ((k & 4) == 0) (node.zLo, zMid) else (zMid + 1, node.zHi)

          if (xl <= xh && yl <= yh && zl <= zh) {
            stack.push(Node(xl, yl, zl, xh, yh, zh))
          }
        }
      }
    }
  }

  // 7. Sort and merge continuous/adjacent ranges to reduce scan count
  mergeRanges(results.toSeq.sortBy(_._1))
}

  private def mergeRanges(ranges: Seq[(Long, Long)]): Seq[(Long, Long)] = {
    if (ranges.isEmpty) return Seq.empty
    val merged = new ListBuffer[(Long, Long)]()
    var (currStart, currEnd) = ranges.head
    ranges.tail.foreach { case (start, end) =>
      if (start <= currEnd + 1) currEnd = math.max(currEnd, end)
      else { merged += ((currStart, currEnd)); currStart = start; currEnd = end }
    }
    merged += ((currStart, currEnd))
    merged.toSeq
  }

  // ============= Multi-level grid block methods (for unified index) =============

  /**
   * Structure representing a 3D grid block
   * level: hierarchy level, 0 is coarsest, BITS_PER_DIM-1 is finest
   * (xLo, yLo, zLo, xHi, yHi, zHi): cubic range of this grid block in normalized coordinates (closed interval)
   */
  case class Cell(level: Int, xLo: Int, yLo: Int, zLo: Int, xHi: Int, yHi: Int, zHi: Int) {
    def contains(x: Int, y: Int, z: Int): Boolean = {
      xLo <= x && x <= xHi && yLo <= y && y <= yHi && zLo <= z && z <= zHi
    }
  }

  /**
   * Directly enumerates multi-level grid block IDs from BBox (with specified level)
   *
   * Uses direct block grid enumeration strategy (no recursion):
   * - Calculates all block grid cells covering the BBox (at the specified level)
   * - Computes Z3D block ID for each block grid cell
   * - Uses the exact same block ID definition as blockIdForUnifiedIndexAtLevel() to ensure import and query alignment
   *
   * @param lonMin, latMin, altMin Minimum values of the query BBox
   * @param lonMax, latMax, altMax Maximum values of the query BBox
   * @param level Block level (determines block size)
   * @param maxCells Maximum number of blocks to return
   * @return List of Z3D values representing candidate blocks (deduplicated)
   */
  def cellsAtLevel(lonMin: Double, latMin: Double, altMin: Double,
                   lonMax: Double, latMax: Double, altMax: Double,
                   level: Int,
                   maxCells: Int = 2000): Seq[Long] = {

    // Normalize BBox to [0, MAX_VALUE]
    val (xMin, yMin, zMin) = normalize(lonMin, latMin, altMin)
    val (xMax, yMax, zMax) = normalize(lonMax, latMax, altMax)

    val selectedCells = scala.collection.mutable.Set[Long]()

    // Block size (based on specified level)
    val cellSize = 1 << (BITS_PER_DIM - level)

    // Calculate block grid index range covering the BBox
    val xBlockMin = xMin / cellSize
    val xBlockMax = xMax / cellSize
    val yBlockMin = yMin / cellSize
    val yBlockMax = yMax / cellSize
    val zBlockMin = zMin / cellSize
    val zBlockMax = zMax / cellSize

    // Directly enumerate block grid: triple nested loop traverses all intersecting blocks
    var bx = xBlockMin
    while (bx <= xBlockMax && selectedCells.size < maxCells) {
      var by = yBlockMin
      while (by <= yBlockMax && selectedCells.size < maxCells) {
        var bz = zBlockMin
        while (bz <= zBlockMax && selectedCells.size < maxCells) {
          // Calculate the lower-left corner coordinates of this block (consistent with blockIdForUnifiedIndexAtLevel)
          val xLo = bx * cellSize
          val yLo = by * cellSize
          val zLo = bz * cellSize

          // Calculate block ID (consistent with blockIdForUnifiedIndexAtLevel)
          val cellZ =
            split(xLo) |
            (split(yLo) << 1) |
            (split(zLo) << 2)

          selectedCells += cellZ
          bz += 1
        }
        by += 1
      }
      bx += 1
    }

    selectedCells.toSeq.sorted
  }

  /**
   * Directly enumerates multi-level grid block IDs from BBox (using default fixed level)
   *
   * Uses direct block grid enumeration strategy (no recursion):
   * - Calculates all block grid cells covering the BBox (at UNIFIED_BLOCK_LEVEL fixed level)
   * - Computes Z3D block ID for each block grid cell
   * - Uses the exact same block ID definition as blockIdForUnifiedIndex() to ensure import and query alignment
   *
   * @param lonMin, latMin, altMin Minimum values of the query BBox
   * @param lonMax, latMax, altMax Maximum values of the query BBox
   * @param maxDepth Maximum subdivision level (unused in this version, retained for backward compatibility)
   * @param maxCells Maximum number of blocks to return
   * @return List of Z3D values representing candidate blocks (deduplicated)
   */
  def cells(lonMin: Double, latMin: Double, altMin: Double,
           lonMax: Double, latMax: Double, altMax: Double,
           maxDepth: Int = 14,
           maxCells: Int = 2000): Seq[Long] =
    cellsAtLevel(lonMin, latMin, altMin, lonMax, latMax, altMax,
                 UNIFIED_BLOCK_LEVEL, maxCells)
}