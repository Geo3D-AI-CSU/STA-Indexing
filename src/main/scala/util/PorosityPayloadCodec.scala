// src/main/scala/util/PorosityPayloadCodec.scala
package util

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.Base64
import java.util.zip.{Inflater, Deflater}

/**
 * Payload encoding/decoding utility
 *
 * Encoding protocol (compatible with numpy tobytes(order="C")):
 * 1. float32 data type (4 bytes per value)
 * 2. C-order memory layout (i fastest, then j, then k)
 * 3. zlib compression
 * 4. base64 encoding
 *
 * Used for encoding/decoding voxel model payload_b64 field
 */
object PorosityPayloadCodec {

  /**
   * Decode payload_b64 to float32 array
   * @param payloadB64 Base64 encoded compressed data
   * @param nx X dimension
   * @param ny Y dimension
   * @param nz Z dimension
   * @return float32 array, length must be nx * ny * nz
   * @throws IllegalArgumentException If decoded length doesn't match
   */
  def decodeToFloatArray(payloadB64: String, nx: Int, ny: Int, nz: Int): Array[Float] = {
    val expectedSize = nx * ny * nz

    // 1. Base64 decode
    val compressedBytes = Base64.getDecoder.decode(payloadB64)

    // 2. zlib decompress
    val inflater = new Inflater()
    inflater.setInput(compressedBytes)

    val outputStream = new ByteArrayOutputStream()
    val buffer = new Array[Byte](1024)

    try {
      while (!inflater.finished()) {
        val count = inflater.inflate(buffer)
        if (count > 0) {
          outputStream.write(buffer, 0, count)
        }
      }
    } finally {
      inflater.end()
    }

    val decompressedBytes = outputStream.toByteArray

    // 3. Validate length
    if (decompressedBytes.length != expectedSize * 4) {
      throw new IllegalArgumentException(
        s"Decoded length mismatch: expected ${expectedSize * 4} bytes (${expectedSize} float32), actual ${decompressedBytes.length} bytes"
      )
    }

    // 4. Convert to float32 array (C-order, little-endian)
    val result = new Array[Float](expectedSize)
    val byteBuffer = ByteBuffer.wrap(decompressedBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    for (i <- 0 until expectedSize) {
      result(i) = byteBuffer.getFloat()
    }

    result
  }

  /**
   * Decode from zlib compressed byte array to float32 array
   * @param compressedBytes zlib compressed byte array
   * @param nx X dimension
   * @param ny Y dimension
   * @param nz Z dimension
   * @return float32 array, length must be nx * ny * nz
   * @throws IllegalArgumentException If decoded length doesn't match
   */
  def decodeFromCompressedBytes(compressedBytes: Array[Byte], nx: Int, ny: Int, nz: Int): Array[Float] = {
    val expectedSize = nx * ny * nz

    // 1. zlib decompress
    val inflater = new Inflater()
    inflater.setInput(compressedBytes)

    val outputStream = new ByteArrayOutputStream()
    val buffer = new Array[Byte](1024)

    try {
      while (!inflater.finished()) {
        val count = inflater.inflate(buffer)
        if (count > 0) {
          outputStream.write(buffer, 0, count)
        }
      }
    } finally {
      inflater.end()
    }

    val decompressedBytes = outputStream.toByteArray

    // 2. Validate length
    if (decompressedBytes.length != expectedSize * 4) {
      throw new IllegalArgumentException(
        s"Decoded length mismatch: expected ${expectedSize * 4} bytes (${expectedSize} float32), actual ${decompressedBytes.length} bytes"
      )
    }

    // 3. Convert to float32 array (C-order, little-endian)
    val result = new Array[Float](expectedSize)
    val byteBuffer = ByteBuffer.wrap(decompressedBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    for (i <- 0 until expectedSize) {
      result(i) = byteBuffer.getFloat()
    }

    result
  }

  /**
   * Encode float32 array to payload_b64
   * @param values float32 array
   * @param nx X dimension
   * @param ny Y dimension
   * @param nz Z dimension
   * @return Base64 encoded compressed data
   * @throws IllegalArgumentException If array length doesn't match
   */
  def encodeFromFloatArray(values: Array[Float], nx: Int, ny: Int, nz: Int): String = {
    val expectedSize = nx * ny * nz

    if (values.length != expectedSize) {
      throw new IllegalArgumentException(
        s"Array length mismatch: expected $expectedSize, actual ${values.length}"
      )
    }

    // 1. Convert to byte array (C-order, little-endian)
    val byteBuffer = ByteBuffer.allocate(values.length * 4).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    values.foreach(v => byteBuffer.putFloat(v))
    val rawBytes = byteBuffer.array()

    // 2. zlib compress
    val deflater = new Deflater(Deflater.BEST_SPEED)
    deflater.setInput(rawBytes)
    deflater.finish()

    val outputStream = new ByteArrayOutputStream()
    val buffer = new Array[Byte](1024)

    try {
      while (!deflater.finished()) {
        val count = deflater.deflate(buffer)
        if (count > 0) {
          outputStream.write(buffer, 0, count)
        }
      }
    } finally {
      deflater.end()
    }

    val compressedBytes = outputStream.toByteArray

    // 3. Base64 encode
    Base64.getEncoder.encodeToString(compressedBytes)
  }
}