// src/main/scala/index/VolumeZCell.scala
package index

/**
 * ZCell calculation utility for Volume
 *
 * Used for spatial dimension partitioning in volume unified index, based on Morton encoding
 * zCell must align with brick grid, level=4
 */
object VolumeZCell {
  
  // Brick dimensions (consistent with VolumeVoxelIngestJob)
  val Bx = 16
  val By = 16
  val Bz = 8
  
  // Global fixed origin (longitude, latitude, depth)
  val GLOBAL_LON0 = 0.0
  val GLOBAL_LAT0 = 0.0
  val GLOBAL_Z0 = 0.0
  
  // Global fixed step sizes (unified grid parameters for volume system)
  // zCell grid must be unified across the entire dataset; deriving from query bbox or model-local step sizes is prohibited to avoid index grid misalignment
  val FIXED_DLON = 0.00390625
  val FIXED_DLAT = 0.00390625
  val FIXED_DZ = 7.8125
  
  /**
   * Morton encoding: encodes 3D coordinates (bx, by, bz) into a Long value
   * Uses bit-interleaving algorithm
   *
   * @param bx Brick index in x direction
   * @param by Brick index in y direction
   * @param bz Brick index in z direction
   * @return Morton-encoded Long value (8 bytes, big-endian)
   */
  def encodeMorton(bx: Long, by: Long, bz: Long): Long = {
    var x = bx
    var y = by
    var z = bz
    var result: Long = 0L
    
    // Bit interleaving: interleave bits of x, y, z into result
    var i = 0
    while (i < 21) { // 64 bits / 3 dimensions ≈ 21 bits per dimension
      result |= (x & 1L) << (3 * i)
      result |= (y & 1L) << (3 * i + 1)
      result |= (z & 1L) << (3 * i + 2)
      x >>= 1
      y >>= 1
      z >>= 1
      i += 1
    }
    
    result
  }
  
  /**
   * Computes zCell for a given position (using global fixed step sizes)
   *
   * @param lon Longitude
   * @param lat Latitude
   * @param z Depth
   * @return zCell (Morton encoded)
   */
  def computeZCell(lon: Double, lat: Double, z: Double): Long = {
    computeZCell(lon, lat, z, FIXED_DLON, FIXED_DLAT, FIXED_DZ)
  }
  
  /**
   * Computes zCell for a given position
   *
   * @param lon Longitude
   * @param lat Latitude
   * @param z Depth
   * @param dlon Longitude step size (longitude span per voxel)
   * @param dlat Latitude step size (latitude span per voxel)
   * @param dz Depth step size (depth span per voxel)
   * @return zCell (Morton encoded)
   */
  def computeZCell(lon: Double, lat: Double, z: Double, 
                   dlon: Double, dlat: Double, dz: Double): Long = {
    // Calculate brick-level grid indices
    val brickDlon = dlon * Bx
    val brickDlat = dlat * By
    val brickDz = dz * Bz
    
    val bx = math.floor((lon - GLOBAL_LON0) / brickDlon).toLong
    val by = math.floor((lat - GLOBAL_LAT0) / brickDlat).toLong
    val bz = math.floor((z - GLOBAL_Z0) / brickDz).toLong
    
    encodeMorton(bx, by, bz)
  }
  
  /**
   * Computes zCell from brick's bbox (using global fixed step sizes)
   *
   * @param lonMin Brick's minimum longitude
   * @param latMin Brick's minimum latitude
   * @param zMin Brick's minimum depth
   * @return zCell (Morton encoded)
   */
  def computeZCellFromBrickBBox(lonMin: Double, latMin: Double, zMin: Double): Long = {
    computeZCell(lonMin, latMin, zMin, FIXED_DLON, FIXED_DLAT, FIXED_DZ)
  }
  
  /**
   * Computes zCell from brick's bbox
   *
   * @param lonMin Brick's minimum longitude
   * @param latMin Brick's minimum latitude
   * @param zMin Brick's minimum depth
   * @param dlon Longitude step size
   * @param dlat Latitude step size
   * @param dz Depth step size
   * @return zCell (Morton encoded)
   */
  def computeZCellFromBrickBBox(lonMin: Double, latMin: Double, zMin: Double,
                                 dlon: Double, dlat: Double, dz: Double): Long = {
    computeZCell(lonMin, latMin, zMin, dlon, dlat, dz)
  }
  
  /**
   * Enumerates all zCells covered by the query bbox (using global fixed step sizes)
   *
   * @param lonMin Query bbox minimum longitude
   * @param latMin Query bbox minimum latitude
   * @param zMin Query bbox minimum depth
   * @param lonMax Query bbox maximum longitude
   * @param latMax Query bbox maximum latitude
   * @param zMax Query bbox maximum depth
   * @return List of zCells (Morton encoded)
   */
  def enumerateZCellsForBBox(lonMin: Double, latMin: Double, zMin: Double,
                              lonMax: Double, latMax: Double, zMax: Double): List[Long] = {
    enumerateZCellsForBBox(lonMin, latMin, zMin, lonMax, latMax, zMax, FIXED_DLON, FIXED_DLAT, FIXED_DZ)
  }
  
  /**
   * Enumerates all zCells covered by the query bbox
   *
   * @param lonMin Query bbox minimum longitude
   * @param latMin Query bbox minimum latitude
   * @param zMin Query bbox minimum depth
   * @param lonMax Query bbox maximum longitude
   * @param latMax Query bbox maximum latitude
   * @param zMax Query bbox maximum depth
   * @param dlon Longitude step size
   * @param dlat Latitude step size
   * @param dz Depth step size
   * @return List of zCells (Morton encoded)
   */
  def enumerateZCellsForBBox(lonMin: Double, latMin: Double, zMin: Double,
                              lonMax: Double, latMax: Double, zMax: Double,
                              dlon: Double, dlat: Double, dz: Double): List[Long] = {
    val brickDlon = dlon * Bx
    val brickDlat = dlat * By
    val brickDz = dz * Bz
    
    // Calculate brick index range covered by bbox (closed interval)
    val bxStart = math.floor((lonMin - GLOBAL_LON0) / brickDlon).toLong
    val bxEnd = math.floor((lonMax - GLOBAL_LON0) / brickDlon).toLong
    
    val byStart = math.floor((latMin - GLOBAL_LAT0) / brickDlat).toLong
    val byEnd = math.floor((latMax - GLOBAL_LAT0) / brickDlat).toLong
    
    val bzStart = math.floor((zMin - GLOBAL_Z0) / brickDz).toLong
    val bzEnd = math.floor((zMax - GLOBAL_Z0) / brickDz).toLong
    
    // Triple loop to enumerate all zCells
    val result = scala.collection.mutable.ListBuffer[Long]()
    
    for (bx <- bxStart to bxEnd) {
      for (by <- byStart to byEnd) {
        for (bz <- bzStart to bzEnd) {
          result += encodeMorton(bx, by, bz)
        }
      }
    }
    
    result.toList
  }
  
  /**
   * Computes zCell list for a given model bbox
   *
   * @param lonMin Model minimum longitude
   * @param latMin Model minimum latitude
   * @param zMin Model minimum depth
   * @param lonMax Model maximum longitude
   * @param latMax Model maximum latitude
   * @param zMax Model maximum depth
   * @param nx Number of voxels in x direction
   * @param ny Number of voxels in y direction
   * @param nz Number of voxels in z direction
   * @return List of zCells (Morton encoded)
   */
  def enumerateZCellsForModel(lonMin: Double, latMin: Double, zMin: Double,
                              lonMax: Double, latMax: Double, zMax: Double,
                              nx: Int, ny: Int, nz: Int): List[Long] = {
    val dlon = (lonMax - lonMin) / nx
    val dlat = (latMax - latMin) / ny
    val dz = (zMax - zMin) / nz
    
    enumerateZCellsForBBox(lonMin, latMin, zMin, lonMax, latMax, zMax, dlon, dlat, dz)
  }
}
