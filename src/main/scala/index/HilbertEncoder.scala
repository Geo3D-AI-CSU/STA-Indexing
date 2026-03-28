package index

import scala.collection.mutable.ListBuffer

object HilbertEncoder {

  // Configuration consistent with Ingest
  val LON_MIN = 110.0; val LON_MAX = 120.0
  val LAT_MIN = 21.0;  val LAT_MAX = 25.0
  val ALT_MIN = 0.0;   val ALT_MAX = 1000.0

  val BITS_PER_DIM = 21
  val MAX_VALUE = (1 << BITS_PER_DIM) - 1

  def normalize(lon: Double, lat: Double, alt: Double): (Int, Int, Int) = {
    val normLon = math.max(0, math.min(MAX_VALUE, ((lon - LON_MIN) / (LON_MAX - LON_MIN) * MAX_VALUE).toInt))
    val normLat = math.max(0, math.min(MAX_VALUE, ((lat - LAT_MIN) / (LAT_MAX - LAT_MIN) * MAX_VALUE).toInt))
    val normAlt = math.max(0, math.min(MAX_VALUE, ((alt - ALT_MIN) / (ALT_MAX - ALT_MIN) * MAX_VALUE).toInt))
    (normLon, normLat, normAlt)
  }

  /**
   * Encoding logic (unchanged)
   * Physical coordinates -> Hilbert index
   */
  def encode(lon: Double, lat: Double, alt: Double): Long = {
    val (x, y, z) = normalize(lon, lat, alt)
    hilbertEncode(x, y, z, BITS_PER_DIM)
  }

  private def hilbertEncode(x: Int, y: Int, z: Int, bits: Int): Long = {
    var result = 0L
    // Process from high bit to low bit
    for (i <- (bits - 1) to 0 by -1) {
      val xBit = (x >> i) & 1
      val yBit = (y >> i) & 1
      val zBit = (z >> i) & 1
      
      // Core: based on physical bits, reverse lookup the corresponding Hilbert bits
      val (rotX, rotY, rotZ) = rotateBack(xBit, yBit, zBit)
      
      result <<= 3
      result |= ((rotX << 2) | (rotY << 1) | rotZ)
    }
    result
  }

  /**
   * [Core Fix] Range query
   * Logic correction: during octree recursion, child nodes must be visited in Hilbert curve order (0->7),
   * so that the generated ranges are continuous and correct.
   */
  def ranges(lonMin: Double, latMin: Double, altMin: Double,
             lonMax: Double, latMax: Double, altMax: Double,
             maxRanges: Int = 2000): Seq[(Long, Long)] = {

    val (xMin, yMin, zMin) = normalize(lonMin, latMin, altMin)
    val (xMax, yMax, zMax) = normalize(lonMax, latMax, altMax)

    val results = new ListBuffer[(Long, Long)]()

    // idx: Hilbert index prefix of the current node
    case class Node(idx: Long, level: Int, x: Int, y: Int, z: Int)
    val stack = scala.collection.mutable.Stack[Node]()

    val maxLevel = BITS_PER_DIM - 1
    val halfDim = 1 << maxLevel

    // Initialization: here we also need to push in Hilbert order, but since the root node covers the entire map,
    // we manually push the 8 root nodes in reverse Hilbert order so that pop order is sequential
    for (h <- 7 to 0 by -1) {
      // Parse the physical offset corresponding to Hilbert value h
      val hx = (h >> 2) & 1
      val hy = (h >> 1) & 1
      val hz = h & 1
      // rotate: Hilbert bit -> physical bit (this is the key to correct ranges)
      val (px, py, pz) = rotate(hx, hy, hz)
      
      val nx = if (px == 1) halfDim else 0
      val ny = if (py == 1) halfDim else 0
      val nz = if (pz == 1) halfDim else 0
      
      // Note: idx is initialized to h
      stack.push(Node(h.toLong, maxLevel, nx, ny, nz))
    }

    while (stack.nonEmpty && results.size < maxRanges) {
      val node = stack.pop()
      val dim = 1 << node.level
      
      val nxMax = node.x + dim - 1
      val nyMax = node.y + dim - 1
      val nzMax = node.z + dim - 1

      // 1. Intersection detection (using physical coordinates)
      val intersects = xMax >= node.x && xMin <= nxMax &&
                       yMax >= node.y && yMin <= nyMax &&
                       zMax >= node.z && zMin <= nzMax

      if (intersects) {
        // 2. Containment detection
        val contained = xMin <= node.x && xMax >= nxMax &&
                        yMin <= node.y && yMax >= nyMax &&
                        zMin <= node.z && zMax >= nzMax

        if (contained) {
          // [Optimization] For this version of Hilbert encoding, the indices of fully contained nodes are continuous
          // Minimum index: current node idx followed by 0s
          // Maximum index: current node idx followed by 1s
          // Each level contributes 3 bits
          val shift = 3 * node.level
          val minIdx = node.idx << shift
          val maxIdx = (node.idx << shift) | ((1L << shift) - 1)
          results += ((minIdx, maxIdx))
        } else if (node.level > 0) {
          // 3. Subdivide
          // Key point: child nodes must be processed in Hilbert order (0..7)
          // To make Stack pop order 0->7, push order must be 7->0
          val subLevel = node.level - 1
          val subDim = 1 << subLevel

          for (h <- 7 to 0 by -1) {
            // h is the Hilbert local index (0-7) of the child node at the current level
            val hx = (h >> 2) & 1
            val hy = (h >> 1) & 1
            val hz = h & 1
            
            // Convert Hilbert local index to physical offset (using rotate)
            val (px, py, pz) = rotate(hx, hy, hz)
            
            val childX = node.x + (if (px == 1) subDim else 0)
            val childY = node.y + (if (py == 1) subDim else 0)
            val childZ = node.z + (if (pz == 1) subDim else 0)
            
            // Calculate the child node's full index: ParentIdx << 3 | h
            val childIdx = (node.idx << 3) | h
            
            stack.push(Node(childIdx, subLevel, childX, childY, childZ))
          }
        } else {
          // Leaf node (Level 0)
          results += ((node.idx, node.idx))
        }
      }
    }

    mergeRanges(results.toSeq.sortBy(_._1))
  }

  // Range merging logic
  private def mergeRanges(ranges: Seq[(Long, Long)]): Seq[(Long, Long)] = {
    if (ranges.isEmpty) return Seq.empty
    val merged = new ListBuffer[(Long, Long)]()
    var (currStart, currEnd) = ranges.head
    ranges.tail.foreach { case (start, end) =>
      if (start <= currEnd + 1) {
        currEnd = math.max(currEnd, end)
      } else {
        merged += ((currStart, currEnd))
        currStart = start
        currEnd = end
      }
    }
    merged += ((currStart, currEnd))
    merged.toSeq
  }

  // --- Rotation/Mapping Tables (preserving original logic) ---
  
  /**
   * Hilbert -> Physical
   * Given Hilbert coordinate components (hx, hy, hz), returns physical offsets (px, py, pz)
   * Used for range generation
   */
  private def rotate(x: Int, y: Int, z: Int): (Int, Int, Int) = {
    val bits = (x << 2) | (y << 1) | z
    val rotated = ROTATE_TABLE(bits)
    ((rotated >> 2) & 1, (rotated >> 1) & 1, rotated & 1)
  }

  /**
   * Physical -> Hilbert
   * Given physical bits (px, py, pz), returns Hilbert bits (hx, hy, hz)
   * Used for encoding
   */
  private def rotateBack(x: Int, y: Int, z: Int): (Int, Int, Int) = {
    val bits = (x << 2) | (y << 1) | z
    val unrotated = UNROTATE_TABLE(bits)
    ((unrotated >> 2) & 1, (unrotated >> 1) & 1, unrotated & 1)
  }

  // Mapping tables remain unchanged (Input: 0-7, Output: 0-7)
  private val ROTATE_TABLE = Array[Int](0, 1, 2, 6, 4, 5, 7, 3)
  private val UNROTATE_TABLE = Array[Int](0, 1, 2, 7, 4, 5, 3, 6)
}