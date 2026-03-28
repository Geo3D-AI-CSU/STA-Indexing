// src/main/scala/storage/HBaseWriter.scala
package storage

import model.SensorRecord
import index.{Z3DEncoder, TimeBucket}

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Connection, BufferedMutator, BufferedMutatorParams}
import org.apache.hadoop.hbase.util.Bytes

class HBaseWriter(connection: Connection) {
  
  private val CF = HBaseTableManager.CF_BYTES
  
  def writeRecords(records: Iterator[SensorRecord], batchSize: Int = 1000): Unit = {
    
    val mutatorData = createMutator(HBaseTableManager.TABLE_DATA)
    val mutatorSensor = createMutator(HBaseTableManager.TABLE_IDX_SENSOR)
    val mutatorTime = createMutator(HBaseTableManager.TABLE_IDX_TIME)
    val mutatorSpatial = createMutator(HBaseTableManager.TABLE_IDX_SPATIAL)
    
    try {
      var count = 0
      
      records.foreach { record =>
        mutatorData.mutate(createDataPut(record))
        mutatorSensor.mutate(createSensorIdxPut(record))
        mutatorTime.mutate(createTimeIdxPut(record))
        mutatorSpatial.mutate(createSpatialIdxPut(record))
        
        count += 1
        
        if (count % batchSize == 0) {
          flushAll(mutatorData, mutatorSensor, mutatorTime, mutatorSpatial)
          println(s"[HBaseWriter] Flushed $count records")
        }
      }
      
      flushAll(mutatorData, mutatorSensor, mutatorTime, mutatorSpatial)
      println(s"[HBaseWriter] Total written: $count records")
      
    } finally {
      closeAll(mutatorData, mutatorSensor, mutatorTime, mutatorSpatial)
    }
  }
  
  private def createMutator(tableName: String): BufferedMutator = {
    val params = new BufferedMutatorParams(TableName.valueOf(tableName))
      .writeBufferSize(4 * 1024 * 1024)
    connection.getBufferedMutator(params)
  }
  
  private def createDataPut(record: SensorRecord): Put = {
    val put = new Put(Bytes.toBytes(record.rowKey))
    put.addColumn(CF, Bytes.toBytes("time"), Bytes.toBytes(record.time))
    put.addColumn(CF, Bytes.toBytes("sensor_id"), Bytes.toBytes(record.sensorId))
    put.addColumn(CF, Bytes.toBytes("lon"), Bytes.toBytes(record.longitude))
    put.addColumn(CF, Bytes.toBytes("lat"), Bytes.toBytes(record.latitude))
    put.addColumn(CF, Bytes.toBytes("alt"), Bytes.toBytes(record.altitude))
    put.addColumn(CF, Bytes.toBytes("type"), Bytes.toBytes(record.sensorType))
    put
  }
  
  private def createSensorIdxPut(record: SensorRecord): Put = {
    val rowKey = s"${record.sensorId}_${record.rowKey}"
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(record.rowKey))
    put
  }
  
  private def createTimeIdxPut(record: SensorRecord): Put = {
    val bucket = TimeBucket.dayBucket(record.time)
    val rowKey = f"${bucket}_${record.time}%013d_${record.rowKey}"
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(record.rowKey))
    put
  }
  
  private def createSpatialIdxPut(record: SensorRecord): Put = {
    val z3d = Z3DEncoder.encode(record.longitude, record.latitude, record.altitude)
    val rowKey = f"${z3d}%016x_${record.rowKey}"
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(CF, Bytes.toBytes("dk"), Bytes.toBytes(record.rowKey))
    put
  }
  
  private def flushAll(mutators: BufferedMutator*): Unit = {
    mutators.foreach(_.flush())
  }
  
  private def closeAll(mutators: BufferedMutator*): Unit = {
    mutators.foreach(m => try { m.close() } catch { case _: Exception => })
  }
}

object HBaseWriter {
  def create(zkQuorum: String): HBaseWriter = {
    val connection = HBaseTableManager.createConnection(zkQuorum)
    new HBaseWriter(connection)
  }
  
  def apply(connection: Connection): HBaseWriter = {
    new HBaseWriter(connection)
  }
}