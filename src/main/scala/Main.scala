// src/main/scala/Main.scala

import model.{SensorRecord, VolumeBrickResult}
import storage.{HBaseTableManager, HBaseWriter}
import query.IncrementalFilterQuery
import query.GeoSimPointUnifiedQuery
import query.GeoSimPointTempUnifiedQuery
import query.GeoSimPointVelocityUnifiedQuery
import query.GeoSimPointIncrementalFilterQuery
import query.GeoSimPointVelocityIncrementalFilterQuery
import query.GeoSimVoxelUnifiedQuery
import query.GeoSimVoxelIncrementalFilterQuery
import query.GeoSimVoxelTempUnifiedQuery
import query.GeoSimVoxelVelocityUnifiedQuery
import query.GeoSimVoxelVelocityIncrementalFilterQuery
import query.VolumeQuery
import query.SelectivityEstimator.{SensorIdEquals, TypeEquals, TimeRange => SelectivityTimeRange, SpatialBBox => SelectivitySpatialBBox, QueryCondition}
import query.{ModelTypeEquals => VolumeModelTypeEquals, TimeRange => VolumeTimeRange, SpatialBBox => VolumeSpatialBBox}
import index.Z3DEncoder
import util.PorosityPayloadCodec
import spark.SparkGeoSimPointUnifiedQuery
import spark.SparkGeoSimVoxelUnifiedQuery
import spark.SparkGeoSimVoxelTempUnifiedQuery
import spark.SparkGeoSimVoxelVelocityUnifiedQuery

import java.io.{File, PrintWriter}
import java.time.Instant
import java.time.format.DateTimeFormatter

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._

object Main {

  /**
   * Parse optional parameters --dataset, --unified-level, --force, etc.
   * Returns (datasetOpt, unifiedLevelOpt, indexesOpt, forceOpt)
   * where forceOpt is Option[Boolean], Some(true) if --force exists, otherwise None
   */
  private def parseOptionalParams(args: Array[String]): (Option[String], Option[Int], Option[Boolean]) = {
    var datasetOpt: Option[String] = None
    var unifiedLevelOpt: Option[Int] = None
    var forceOpt: Option[Boolean] = None

    var i = 0
    while (i < args.length) {
      val key = args(i)
      val value = if (i + 1 < args.length) args(i + 1) else ""

      key match {
        case "--dataset" =>
          datasetOpt = Some(value)
          i += 2
        case "--unified-level" =>
          try {
            unifiedLevelOpt = Some(value.toInt)
            i += 2
          } catch {
            case _: NumberFormatException =>
              i += 2
          }
        case "--indexes" =>
          // This parameter is not currently returned in this method, used only during ingestion
          i += 2
        case "--force" =>
          forceOpt = Some(true)
          i += 1
        case _ =>
          i += 1
      }
    }

    (datasetOpt, unifiedLevelOpt, forceOpt)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      printUsage()
      System.exit(1)
    }

    val command = args(0).toLowerCase
    val zkQuorum = args(1)

    command match {
      case "init" =>
        initTables(zkQuorum)

      case "query" =>
        if (args.length >= 6) {
          val outputFormat = args(5).toLowerCase
          val outputDir = if (args.length >= 7) args(6) else "/test/sensor-spatial-index/result"
          val (datasetOpt, unifiedLevelOpt, _) = parseOptionalParams(args.drop(7))
          runQueryWithExport(zkQuorum, args(2), args(3), args(4), outputFormat, outputDir, datasetOpt)
        } else if (args.length >= 5) {
          val (datasetOpt, unifiedLevelOpt, _) = parseOptionalParams(args.drop(5))
          runQueryWithExport(zkQuorum, args(2), args(3), args(4), "csv", "/test/sensor-spatial-index/result", datasetOpt)
        } else {
          runQueryExample(zkQuorum)
        }

      // New: Spatial query command
      case "spatial" =>
        if (args.length >= 12) {
          val sensorId = args(2)
          val startTime = args(3)
          val endTime = args(4)
          val lonMin = args(5).toDouble
          val latMin = args(6).toDouble
          val altMin = args(7).toDouble
          val lonMax = args(8).toDouble
          val latMax = args(9).toDouble
          val altMax = args(10).toDouble
          val format = args(11).toLowerCase

          // Parse outputDir, mode, and engine
          val (outputDir, mode, engine) = if (args.length >= 15) {
            (args(12), args(13).toLowerCase, args(14).toLowerCase)
          } else if (args.length == 14) {
            (args(12), args(13).toLowerCase, "unified")
          } else if (args.length == 13) {
            val arg12 = args(12)
            if (arg12.contains("/") || arg12.contains("\\")) {
              (arg12, "serial", "unified")
            } else {
              ("/test/sensor-spatial-index/result", arg12, "unified")
            }
          } else {
            ("/test/sensor-spatial-index/result", "serial", "unified")
          }

          // Parse --dataset and --unified-level
          val (datasetOpt, unifiedLevelOpt, _) = parseOptionalParams(args.drop(15))

          // Select query method based on mode
          mode match {
            case "spark" =>
              runSpatialQueryWithSpark(zkQuorum, sensorId, startTime, endTime,
                                       lonMin, latMin, altMin, lonMax, latMax, altMax,
                                       format, outputDir, engine, datasetOpt, unifiedLevelOpt)

            case "parallel" =>
              runSpatialQuery(zkQuorum, sensorId, startTime, endTime,
                              lonMin, latMin, altMin, lonMax, latMax, altMax,
                              format, outputDir, mode, engine, datasetOpt, unifiedLevelOpt)

            case _ =>
              // Default serial mode
              runSpatialQuery(zkQuorum, sensorId, startTime, endTime,
                              lonMin, latMin, altMin, lonMax, latMax, altMax,
                              format, outputDir, mode, engine, datasetOpt, unifiedLevelOpt)
          }
        } else {
          println("Error: spatial command requires 11 arguments")
          printUsage()
        }

      // New: Spatial-only query (no sensor ID restriction)
      case "bbox" =>
        if (args.length >= 10) {
          val startTime = args(2)
          val endTime = args(3)
          val lonMin = args(4).toDouble
          val latMin = args(5).toDouble
          val altMin = args(6).toDouble
          val lonMax = args(7).toDouble
          val latMax = args(8).toDouble
          val altMax = args(9).toDouble
          val format = if (args.length >= 11) args(10).toLowerCase else "csv"
          val outputDir = if (args.length >= 12) args(11) else "/test/sensor-spatial-index/result"

          val (datasetOpt, unifiedLevelOpt, _) = parseOptionalParams(args.drop(12))

          runBBoxQuery(zkQuorum, startTime, endTime,
                       lonMin, latMin, altMin, lonMax, latMax, altMax,
                       format, outputDir, datasetOpt)
        } else {
          println("Error: bbox command requires 9 arguments")
          printUsage()
        }

      case "test" =>
        runTest(zkQuorum)

      case "drop-dataset" =>
        if (args.length >= 3) {
          // Parse --dataset and --force parameters
          val (datasetOpt, _, forceOpt) = parseOptionalParams(args.drop(2))
          datasetOpt match {
            case Some(dataset) =>
              val force = forceOpt.isDefined  // If --force flag exists, then true
              runDropDataset(zkQuorum, dataset, force)
            case None =>
              println("Error: --dataset is required for drop-dataset command")
              printUsage()
              System.exit(1)
          }
        } else {
          println("Error: drop-dataset command requires --dataset parameter")
          printUsage()
          System.exit(1)
        }

      case "diagnose" =>
        if (args.length >= 3) {
          val limitStr = args(2)
          val limit = if (limitStr.forall(_.isDigit)) limitStr.toInt else 10
          diagnoseSpatialIndex(zkQuorum, limit)
        } else {
          diagnoseSpatialIndex(zkQuorum, 10)
        }

      // New: Initialize volume tables
      case "init-volume" =>
        val (datasetOpt, unifiedLevelOpt, _) = parseOptionalParams(args.drop(2))
        runInitVolume(zkQuorum, datasetOpt, unifiedLevelOpt)

      // New: Export model validation
      case "export-model" =>
        if (args.length >= 6) {
          val modelId = args(2)
          val timeIsoZ = args(3)
          val format = args(4).toLowerCase
          val outputDir = args(5)
          val (datasetOpt, _, _) = parseOptionalParams(args.drop(6))
          runExportModel(zkQuorum, modelId, timeIsoZ, format, outputDir, datasetOpt)
        } else {
          println("Error: export-model command requires 5 arguments")
          printUsage()
        }

      // New: Geothermal simulation point query command
      case "sim-point" =>
        if (args.length >= 13) {
          val simId = args(2)
          val startTime = args(3)
          val endTime = args(4)
          val xMin = args(5).toDouble
          val yMin = args(6).toDouble
          val zMin = args(7).toDouble
          val xMax = args(8).toDouble
          val yMax = args(9).toDouble
          val zMax = args(10).toDouble
          val format = if (args.length >= 12) args(11).toLowerCase else "txt"
          val outputDir = if (args.length >= 13) args(12) else "/test/geosim-point/result"
          
          var engine: String = "unified"
          var withHeader: Boolean = true
          var mode: String = "serial"
          var datasetOpt: Option[String] = None
          var unifiedLevelOpt: Option[Int] = None
          
          var i = 13
          while (i < args.length) {
            val key = args(i)
            val value = if (i + 1 < args.length) args(i + 1) else ""
            
            key match {
              case "--engine" =>
                engine = value.toLowerCase
                i += 2
              case "--format" =>
                // format already parsed at position 11
                i += 2
              case "--with-header" =>
                withHeader = value.toBoolean
                i += 2
              case "--mode" =>
                mode = value.toLowerCase
                i += 2
              case "--dataset" =>
                datasetOpt = Some(value)
                i += 2
              case "--unified-level" =>
                try {
                  unifiedLevelOpt = Some(value.toInt)
                  i += 2
                } catch {
                  case _: NumberFormatException =>
                    i += 2
                }
              case _ =>
                i += 1
            }
          }
          
          runSimPointQuery(zkQuorum, simId, startTime, endTime, xMin, yMin, zMin, xMax, yMax, zMax, 
                          format, outputDir, engine, mode, withHeader, datasetOpt, unifiedLevelOpt)
        } else {
          println("Error: sim-point command requires 11 arguments")
          printUsage()
        }

      case "sim-voxel" =>
        if (args.length >= 12) {
          val simId = args(2)
          val startTime = args(3)
          val endTime = args(4)
          val xMin = args(5).toDouble
          val yMin = args(6).toDouble
          val zMin = args(7).toDouble
          val xMax = args(8).toDouble
          val yMax = args(9).toDouble
          val zMax = args(10).toDouble
          val format = args(11).toLowerCase
          val outputDir = if (args.length >= 13) args(12) else "/test/geosim-voxel/result"
          
          var mode = "serial"
          var engine = "unified"
          var withHeader = false
          var datasetOpt: Option[String] = None
          var unifiedLevelOpt: Option[Int] = None
          
          var i = 13
          while (i < args.length) {
            args(i) match {
              case "--mode" =>
                if (i + 1 < args.length) {
                  mode = args(i + 1).toLowerCase
                  i += 2
                } else {
                  i += 1
                }
              case "--engine" =>
                if (i + 1 < args.length) {
                  engine = args(i + 1).toLowerCase
                  i += 2
                } else {
                  i += 1
                }
              case "--with-header" =>
                if (i + 1 < args.length) {
                  withHeader = args(i + 1).toLowerCase == "true"
                  i += 2
                } else {
                  i += 1
                }
              case "--dataset" =>
                if (i + 1 < args.length) {
                  datasetOpt = Some(args(i + 1))
                  i += 2
                } else {
                  i += 1
                }
              case "--unified-level" =>
                if (i + 1 < args.length) {
                  try {
                    unifiedLevelOpt = Some(args(i + 1).toInt)
                    i += 2
                  } catch {
                    case _: NumberFormatException =>
                      println(s"[Error] Invalid --unified-level value: ${args(i + 1)}")
                      i += 2
                  }
                } else {
                  i += 1
                }
              case _ =>
                i += 1
            }
          }
          
          runSimVoxelQuery(zkQuorum, simId, startTime, endTime, xMin, yMin, zMin, xMax, yMax, zMax, format, outputDir, mode, engine, withHeader, datasetOpt, unifiedLevelOpt)
        } else {
          println("Error: sim-voxel command requires at least 12 arguments")
          printUsage()
        }

      case "sim-point-temp" =>
        if (args.length >= 12) {
          val tMin = args(2).toDouble
          val tMax = args(3).toDouble
          val startTime = args(4)
          val endTime = args(5)
          val xMin = args(6).toDouble
          val yMin = args(7).toDouble
          val zMin = args(8).toDouble
          val xMax = args(9).toDouble
          val yMax = args(10).toDouble
          val zMax = args(11).toDouble
          val format = if (args.length >= 13) args(12).toLowerCase else "csv"
          val outputDir = if (args.length >= 14) args(13) else "/test/geosim-point/result"

          var mode = "serial"
          var engine = "unified"
          var datasetOpt: Option[String] = None
          var unifiedLevelOpt: Option[Int] = None

          var i = 14
          while (i < args.length) {
            args(i) match {
              case "--engine" =>
                if (i + 1 < args.length) {
                  engine = args(i + 1).toLowerCase
                  i += 2
                } else {
                  i += 1
                }
              case "--mode" =>
                if (i + 1 < args.length) {
                  mode = args(i + 1).toLowerCase
                  i += 2
                } else {
                  i += 1
                }
              case "--dataset" =>
                if (i + 1 < args.length) {
                  datasetOpt = Some(args(i + 1))
                  i += 2
                } else {
                  i += 1
                }
              case "--unified-level" =>
                if (i + 1 < args.length) {
                  try {
                    unifiedLevelOpt = Some(args(i + 1).toInt)
                    i += 2
                  } catch {
                    case _: NumberFormatException =>
                      println(s"[Error] Invalid --unified-level value: ${args(i + 1)}")
                      i += 2
                  }
                } else {
                  i += 1
                }
              case _ =>
                i += 1
            }
          }

          runSimPointTempQuery(zkQuorum, tMin, tMax, startTime, endTime, xMin, yMin, zMin, xMax, yMax, zMax, format, outputDir, mode, engine, datasetOpt, unifiedLevelOpt)
        } else {
          println("Error: sim-point-temp command requires at least 11 arguments")
          printUsage()
        }

      case "sim-voxel-temp" =>
        if (args.length >= 13) {
          val tMin = args(2).toDouble
          val tMax = args(3).toDouble
          val startTime = args(4)
          val endTime = args(5)
          val xMin = args(6).toDouble
          val yMin = args(7).toDouble
          val zMin = args(8).toDouble
          val xMax = args(9).toDouble
          val yMax = args(10).toDouble
          val zMax = args(11).toDouble
          val format = if (args.length >= 14) args(12).toLowerCase else "csv"
          val outputDir = if (args.length >= 15) args(13) else "/test/geosim-voxel/result"

          var mode = "serial"
          var engine = "unified"
          var datasetOpt: Option[String] = None
          var unifiedLevelOpt: Option[Int] = None

          var i = 15
          while (i < args.length) {
            args(i) match {
              case "--mode" =>
                if (i + 1 < args.length) {
                  mode = args(i + 1).toLowerCase
                  i += 2
                } else {
                  i += 1
                }
              case "--engine" =>
                if (i + 1 < args.length) {
                  engine = args(i + 1).toLowerCase
                  i += 2
                } else {
                  i += 1
                }
              case "--dataset" =>
                if (i + 1 < args.length) {
                  datasetOpt = Some(args(i + 1))
                  i += 2
                } else {
                  i += 1
                }
              case "--unified-level" =>
                if (i + 1 < args.length) {
                  try {
                    unifiedLevelOpt = Some(args(i + 1).toInt)
                    i += 2
                  } catch {
                    case _: NumberFormatException =>
                      println(s"[Error] Invalid --unified-level value: ${args(i + 1)}")
                      i += 2
                  }
                } else {
                  i += 1
                }
              case _ =>
                i += 1
            }
          }

          runSimVoxelTempQuery(zkQuorum, tMin, tMax, startTime, endTime, xMin, yMin, zMin, xMax, yMax, zMax, format, outputDir, mode, engine, datasetOpt, unifiedLevelOpt)
        } else {
          println("Error: sim-voxel-temp command requires at least 12 arguments")
          printUsage()
        }

      case "sim-voxel-vel" =>
        if (args.length >= 17) {
          val vxMin = args(2).toDouble
          val vxMax = args(3).toDouble
          val vyMin = args(4).toDouble
          val vyMax = args(5).toDouble
          val vzMin = args(6).toDouble
          val vzMax = args(7).toDouble
          val startTime = args(8)
          val endTime = args(9)
          val xMin = args(10).toDouble
          val yMin = args(11).toDouble
          val zMin = args(12).toDouble
          val xMax = args(13).toDouble
          val yMax = args(14).toDouble
          val zMax = args(15).toDouble
          val format = if (args.length >= 18) args(16).toLowerCase else "csv"
          val outputDir = if (args.length >= 19) args(17) else "/test/geosim-voxel/result"

          var datasetOpt: Option[String] = None
          var unifiedLevelOpt: Option[Int] = None
          var mode = "serial"
          var engine = "unified"

          var i = 18
          while (i < args.length) {
            args(i) match {
              case "--dataset" =>
                if (i + 1 < args.length) {
                  datasetOpt = Some(args(i + 1))
                  i += 2
                } else {
                  i += 1
                }
              case "--unified-level" =>
                if (i + 1 < args.length) {
                  try {
                    unifiedLevelOpt = Some(args(i + 1).toInt)
                    i += 2
                  } catch {
                    case _: NumberFormatException =>
                      println(s"[Error] Invalid --unified-level value: ${args(i + 1)}")
                        i += 2
                  }
                } else {
                  i += 1
                }
              case "--mode" =>
                if (i + 1 < args.length) {
                  mode = args(i + 1).toLowerCase
                  if (mode != "serial" && mode != "parallel" && mode != "spark") {
                    println(s"[Error] Invalid --mode value: ${args(i + 1)}. Must be serial, parallel, or spark")
                    mode = "serial"
                  }
                  i += 2
                } else {
                  i += 1
                }
              case "--engine" =>
                if (i + 1 < args.length) {
                  engine = args(i + 1).toLowerCase
                  i += 2
                } else {
                  i += 1
                }
              case _ =>
                i += 1
            }
          }

          runSimVoxelVelQuery(zkQuorum, vxMin, vxMax, vyMin, vyMax, vzMin, vzMax, startTime, endTime, xMin, yMin, zMin, xMax, yMax, zMax, format, outputDir, datasetOpt, unifiedLevelOpt, mode, engine)
        } else {
          println("Error: sim-voxel-vel command requires at least 16 arguments")
          printUsage()
        }

      case "sim-point-vel" =>
        if (args.length >= 14) {
          val vxMin = args(2).toDouble
          val vxMax = args(3).toDouble
          val vyMin = args(4).toDouble
          val vyMax = args(5).toDouble
          val vzMin = args(6).toDouble
          val vzMax = args(7).toDouble
          val startTime = args(8)
          val endTime = args(9)
          val xMin = args(10).toDouble
          val yMin = args(11).toDouble
          val zMin = args(12).toDouble
          val xMax = args(13).toDouble
          val yMax = args(14).toDouble
          val zMax = args(15).toDouble
          val format = if (args.length >= 17) args(16).toLowerCase else "csv"
          val outputDir = if (args.length >= 18) args(17) else "/test/geosim-point/result"

          var datasetOpt: Option[String] = None
          var unifiedLevelOpt: Option[Int] = None
          var engine = "unified"
          var mode = "serial"

          var i = 16
          while (i < args.length) {
            args(i) match {
              case "--dataset" =>
                if (i + 1 < args.length) {
                  datasetOpt = Some(args(i + 1))
                  i += 2
                } else {
                  i += 1
                }
              case "--unified-level" =>
                if (i + 1 < args.length) {
                  try {
                    unifiedLevelOpt = Some(args(i + 1).toInt)
                    i += 2
                  } catch {
                    case _: NumberFormatException =>
                      println(s"[Error] Invalid --unified-level value: ${args(i + 1)}")
                      i += 2
                  }
                } else {
                  i += 1
                }
              case "--engine" =>
                if (i + 1 < args.length) {
                  engine = args(i + 1).toLowerCase
                  i += 2
                } else {
                  i += 1
                }
              case "--mode" =>
                if (i + 1 < args.length) {
                  mode = args(i + 1).toLowerCase
                  i += 2
                } else {
                  i += 1
                }
              case _ =>
                i += 1
            }
          }

          runSimPointVelQuery(zkQuorum, vxMin, vxMax, vyMin, vyMax, vzMin, vzMax, startTime, endTime, xMin, yMin, zMin, xMax, yMax, zMax, format, outputDir, mode, engine, datasetOpt, unifiedLevelOpt)
        } else {
          println("Error: sim-point-vel command requires at least 15 arguments")
          printUsage()
        }

      case "volume" =>
        runVolumePlaceholder(zkQuorum, args.drop(1))

      case _ =>
        printUsage()
        System.exit(1)
    }
  }
  
  private def printUsage(): Unit = {
    println(
      """
        |Usage:
        |
        |  1. Initialize tables:
        |     Main init <zk_quorum>
        |
        |  2. Query by sensor ID and time:
        |     Main query <zk_quorum> <sensor_id> <start_time> <end_time> [format] [output_dir]
        |
        |  3. Query by sensor ID, time, and spatial bbox (Unified Index):
        |     Main spatial <zk_quorum> <sensor_id> <start_time> <end_time> \
        |                  <lon_min> <lat_min> <alt_min> <lon_max> <lat_max> <alt_max> \
        |                  <format> [output_dir] [mode] [engine]
        |
        |  4. Query by time and spatial bbox only (all sensors):
        |     Main bbox <zk_quorum> <start_time> <end_time> \
        |               <lon_min> <lat_min> <alt_min> <lon_max> <lat_max> <alt_max> \
        |               [format] [output_dir]
        |
        |  5. Run tests:
        |     Main test <zk_quorum>
        |
        |  6. Drop entire dataset:
        |     Main drop-dataset <zk_quorum> --dataset <id> [--force]
        |
        |Parameters:
        |  zk_quorum  : ZooKeeper address (e.g., node001:2181,node002:2181,node003:2181)
        |  sensor_id  : Sensor ID (e.g., SENS_026)
        |  start_time : Start time (e.g., 2025-11-03T04:00:00)
        |  end_time   : End time (e.g., 2025-11-03T10:00:00)
        |  lon_min/max: Longitude range (e.g., 113.0 to 115.0)
        |  lat_min/max: Latitude range (e.g., 22.0 to 24.0)
        |  alt_min/max: Altitude range in meters (e.g., 200 to 300)
        |  format     : Output format (csv or json)
        |  output_dir : Output directory (default: /test/sensor-spatial-index/result)
        |  mode       : Query execution mode for spatial command (default: serial)
        |              - serial  : Local serial unified index query
        |              - parallel: Local multi-threaded unified index query (8 threads)
        |              - spark   : Distributed Spark cluster unified index query
        |  engine     : Query engine for spatial command (default: unified)
        |              - unified    : Use unified spatial-temporal index (idx_unified)
        |              - incremental: Use traditional incremental filter path (idx_sensor/idx_time/idx_spatial)
        |  --dataset  : Dataset ID (alphanumeric and underscore only) for multi-dataset support
        |  --unified-level: Spatial index hierarchy level (default: 19) for dynamic index granularity
        |  --force    : For drop-dataset command, force deletion without dry-run
        |
        |  For sim-point command:
        |  sim_id     : Simulation point ID (e.g., SIM_001)
        |  x_min/max  : X coordinate range in meters
        |  y_min/max  : Y coordinate range in meters
        |  z_min/max  : Z coordinate range in meters
        |  format     : Output format (txt, csv, json) (default: txt)
        |  output_dir : Output directory (default: /test/geosim-point/result)
        |  --engine   : Query engine (default: unified)
        |              - unified    : Use unified spatial-temporal index
        |              - incremental: Use incremental filter path (sim_id/time/spatial)
        |  --mode     : Query execution mode for unified engine (default: serial)
        |              - serial  : Local serial unified index query
        |              - parallel: Local multi-threaded unified index query (8 threads)
        |  --with-header: Include header line in txt/csv output (default: true)
        |  --dataset  : Dataset ID for multi-dataset support
        |  --unified-level: Spatial index hierarchy level (default: 19)
        |
        |  For sim-voxel command:
        |  sim_id     : Simulation voxel ID (e.g., SIM_001)
        |  x_min/max  : X coordinate range in meters
        |  y_min/max  : Y coordinate range in meters
        |  z_min/max  : Z coordinate range in meters
        |  format     : Output format (txt, csv, json) (default: txt)
        |  output_dir : Output directory (default: /test/geosim-voxel/result)
        |  mode       : Query execution mode (default: serial)
        |              - serial  : Local serial unified index query
        |              - parallel: Local multi-threaded unified index query (8 threads)
        |              - spark   : Distributed Spark cluster unified index query
        |  engine     : Query engine (default: unified)
        |              - unified    : Use unified spatial-temporal index
        |              - incremental: Use incremental filter path (sim_id/time/spatial)
        |  with-header : Include header line in csv output (default: false)
        |  --dataset  : Dataset ID for multi-dataset support
        |  --unified-level: Spatial index hierarchy level (default: 4)
        |Examples:
        |  Main init node001:2181
        |
        |  Main query node001:2181 SENS_026 2025-11-03T04:00:00 2025-11-03T07:00:00 csv
        |
        |  Main spatial node001:2181 SENS_026 2025-11-03T04:00:00 2025-11-03T07:00:00 \
        |               114.0 23.2 200 114.2 23.3 220 json
        |
        |  Main bbox node001:2181 2025-11-03T04:00:00 2025-11-03T10:00:00 \
        |            113.5 22.5 200 114.5 23.5 400 csv
        |
        |  Main test node001:2181
        |
        |  Main drop-dataset node001:2181 --dataset my_dataset
        |
        |  Main drop-dataset node001:2181 --dataset my_dataset --force
        |
        |  8. GeoSim point query:
        |     Main sim-point <zk_quorum> <sim_id> <start_time> <end_time> \
        |                  <x_min> <y_min> <z_min> <x_max> <y_max> <z_max> \
        |                  [format] [output_dir] [--engine unified|incremental] \
        |                  [--mode serial|parallel] [--with-header true|false] \
        |                  [--dataset <id>] [--unified-level <int>]
        |
        |  9. GeoSim voxel query:
        |     Main sim-voxel <zk_quorum> <sim_id> <start_time> <end_time> \
        |                  <x_min> <y_min> <z_min> <x_max> <y_max> <z_max> \
        |                  <format> [output_dir] [mode] [engine] [with-header] \
        |                  [--dataset <id>] [--unified-level <int>]
        |
        |  10. GeoSim point temperature query:
        |     Main sim-point-temp <zk_quorum> <t_min> <t_max> <start_time> <end_time> \
        |                  <x_min> <y_min> <z_min> <x_max> <y_max> <z_max> \
        |                  <format> [output_dir] [--engine unified|incremental] \
        |                  [--mode serial|parallel|spark] [--dataset <id>] \
        |                  [--unified-level <int>]
        |  t_min/max  : Temperature range in Kelvin
        |  start_time/end_time : Time range in UTC (e.g., 2025-11-03T04:00:00)
        |  x_min/max  : X coordinate range in meters
        |  y_min/max  : Y coordinate range in meters
        |  z_min/max  : Z coordinate range in meters
        |  format     : Output format (csv, json) (default: csv)
        |  output_dir : Output directory (default: /test/geosim-point/result)
        |  engine     : Query engine (default: unified)
        |              - unified    : Unified index query (supports serial/parallel/spark)
        |              - incremental: Incremental filter query (supports serial/parallel)
        |  mode       : Query execution mode (default: serial)
        |              - serial  : Local serial query
        |              - parallel: Local multi-threaded query (8 threads)
        |              - spark   : Distributed Spark query (unified engine only)
        |  --dataset  : Dataset ID for multi-dataset support
        |  --unified-level: Spatial index hierarchy level (default: 19)
        |
        |  11. GeoSim voxel temperature query:
        |     Main sim-voxel-temp <zk_quorum> <t_min> <t_max> <start_time> <end_time> \
        |                  <x_min> <y_min> <z_min> <x_max> <y_max> <z_max> \
        |                  <format> [output_dir] [--engine unified|incremental] \
        |                  [--mode serial|parallel|spark] [--dataset <id>] \
        |                  [--unified-level <int>]
        |  t_min/max  : Temperature range in Kelvin
        |  start_time/end_time : Time range in UTC (e.g., 2025-11-03T04:00:00)
        |  x_min/max  : X coordinate range in meters
        |  y_min/max  : Y coordinate range in meters
        |  z_min/max  : Z coordinate range in meters
        |  format     : Output format (csv, json) (default: csv)
        |  output_dir : Output directory (default: /test/geosim-voxel/result)
        |  engine     : Query engine (default: unified)
        |              - unified    : Unified index query (supports serial/parallel/spark)
        |              - incremental: Incremental filter query (supports serial/parallel)
        |  mode       : Query execution mode (default: serial)
        |              - serial  : Local serial query
        |              - parallel: Local multi-threaded query (8 threads)
        |              - spark   : Distributed Spark query (unified engine only)
        |  --dataset  : Dataset ID for multi-dataset support
        |  --unified-level: Spatial index hierarchy level (default: 4)
        |
        |  12. GeoSim point velocity query:
        |     Main sim-point-vel <zk_quorum> \
        |                  <vx_min> <vx_max> <vy_min> <vy_max> <vz_min> <vz_max> \
        |                  <start_time> <end_time> \
        |                  <x_min> <y_min> <z_min> <x_max> <y_max> <z_max> \
        |                  <format> [output_dir] [mode] \
        |                  [--dataset <id>] [--unified-level <int>] [--engine <engine>]
        |  vx_min/max : Velocity X component range (e.g., -1e-5 to 1e-5)
        |  vy_min/max : Velocity Y component range (e.g., -1e-5 to 1e-5)
        |  vz_min/max : Velocity Z component range (e.g., -1e-4 to 1e-4)
        |  start_time/end_time : Time range in UTC (e.g., 2025-11-03T04:00:00)
        |  x_min/max  : X coordinate range in meters
        |  y_min/max  : Y coordinate range in meters
        |  z_min/max  : Z coordinate range in meters
        |  format     : Output format (csv, json) (default: csv)
        |  output_dir : Output directory (default: /test/geosim-point/result)
        |  mode       : Query execution mode (default: serial)
        |              - serial  : Local serial query
        |              - parallel: Local multi-threaded query (8 threads)
        |              - spark   : Spark distributed query (requires engine=unified)
        |  --engine   : Query engine (default: unified)
        |              - unified    : Unified index query (supports serial/parallel/spark)
        |              - incremental: Incremental filter query (supports serial)
        |  --dataset  : Dataset ID for multi-dataset support
        |  --unified-level: Spatial index hierarchy level (default: 19)
        |
        |  13. GeoSim voxel velocity query:
        |     Main sim-voxel-vel <zk_quorum> \
        |                  <vx_min> <vx_max> <vy_min> <vy_max> <vz_min> <vz_max> \
        |                  <start_time> <end_time> \
        |                  <x_min> <y_min> <z_min> <x_max> <y_max> <z_max> \
        |                  <format> <output_dir> \
        |                  [--dataset <id>] [--unified-level <int>]
        |  vx_min/max : Velocity X component range (e.g., -1e-6 to 1e-6)
        |  vy_min/max : Velocity Y component range (e.g., -1e-6 to 1e-6)
        |  vz_min/max : Velocity Z component range (e.g., -1e-5 to 1e-5)
        |  start_time/end_time : Time range in UTC (e.g., 2025-11-03T04:00:00)
        |  x_min/max  : X coordinate range in meters
        |  y_min/max  : Y coordinate range in meters
        |  z_min/max  : Z coordinate range in meters
        |  format     : Output format (csv, json) (default: csv)
        |  output_dir : Output directory (default: /test/geosim-voxel/result)
        |  --dataset  : Dataset ID for multi-dataset support
        |  --unified-level: Spatial index hierarchy level (default: 4)
        |""".stripMargin)
  }
  
  private def initTables(zkQuorum: String): Unit = {
    println(s"Initializing HBase tables...")
    println(s"ZooKeeper: $zkQuorum")
    
    val connection = HBaseTableManager.createConnection(zkQuorum)
    try {
      HBaseTableManager.createTables(connection)
      println("All tables created successfully!")
    } finally {
      connection.close()
    }
  }
  
  /**
   * Complete query: Space + Attribute + Time
   */
  private def runSpatialQuery(zkQuorum: String, sensorId: String,
                               startTime: String, endTime: String,
                               lonMin: Double, latMin: Double, altMin: Double,
                               lonMax: Double, latMax: Double, altMax: Double,
                               format: String, outputDir: String, mode: String, engine: String,
                               dataset: Option[String] = None, unifiedLevel: Option[Int] = None): Unit = {
    println(s"\n${"=" * 60}")
    println("Spatial query (sensor + time + space)")
    println(s"${"=" * 60}")
    println(s"  Sensor ID: $sensorId")
    println(s"  Time range: $startTime ~ $endTime")
    println(s"  Space range:")
    println(f"    Longitude: $lonMin%.6f ~ $lonMax%.6f")
    println(f"    Latitude: $latMin%.6f ~ $latMax%.6f")
    println(f"    Altitude: $altMin%.2f ~ $altMax%.2f m")
    println(s"  Output format: $format -> $outputDir")

    // Print execution mode based on engine and mode
    val engineLabel = engine.toLowerCase match {
      case "incremental" => "Traditional incremental filter (idx_sensor/idx_time/idx_spatial)"
      case _             => "Unified index (idx_unified)"
    }
    val modeLabel = mode.toLowerCase match {
      case "parallel" => "Local parallel (8 threads)"
      case _          => "Local serial"
    }
    println(s"  Query engine: $engineLabel")
    println(s"  Execution mode: $modeLabel")

    // Print dataset and unifiedLevel configuration
    if (dataset.isDefined || unifiedLevel.isDefined) {
      println(s"  dataset: ${dataset.getOrElse("(default)")}")
      println(s"  unifiedLevel: ${unifiedLevel.getOrElse("(default)")}")
    }

    // Tip when incremental mode and mode != serial
    if (engine.toLowerCase == "incremental" && mode.toLowerCase != "serial") {
      println(s"  [Tip] unified-specific mode '$mode' is not supported in incremental mode, will execute as incremental single-thread.")
    }

    println(s"${"=" * 60}\n")

    val useUnifiedParallel = mode.toLowerCase == "parallel"
    val enableUnifiedIndex = engine.toLowerCase != "incremental"

    val query = new IncrementalFilterQuery(
      zkQuorum,
      enableUnifiedIndex = enableUnifiedIndex,
      useUnifiedParallel = useUnifiedParallel,
      unifiedThreadPoolSize = 8,
      dataset = dataset,
      unifiedLevel = unifiedLevel
    )

    try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
      val startTs = java.time.LocalDateTime.parse(startTime, formatter)
        .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli
      val endTs = java.time.LocalDateTime.parse(endTime, formatter)
        .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli

      // Build complete conditions: attribute + time + space
      val conditions: Seq[QueryCondition] = Seq(
        SensorIdEquals(sensorId),
        SelectivityTimeRange(startTs, endTs),
        SelectivitySpatialBBox(lonMin, latMin, altMin, lonMax, latMax, altMax)
      )

      println("Execution plan:")
      val results = query.query(conditions).toList
      println(s"\n${"=" * 60}")

      printAndExportResults(results, sensorId, format, outputDir, "spatial")

    } finally {
      query.close()
    }
  }

  /**
   * Execute unified index spatial query using Spark cluster
   */
  private def runSpatialQueryWithSpark(zkQuorum: String, sensorId: String,
                                        startTime: String, endTime: String,
                                        lonMin: Double, latMin: Double, altMin: Double,
                                        lonMax: Double, latMax: Double, altMax: Double,
                                        format: String, outputDir: String, engine: String,
                                        dataset: Option[String] = None, unifiedLevel: Option[Int] = None): Unit = {
    println(s"\n${"=" * 60}")
    println("Spark cluster spatial query (sensor + time + space)")
    println(s"${"=" * 60}")
    println(s"  Sensor ID: $sensorId")
    println(s"  Time range: $startTime ~ $endTime")
    println(s"  Space range:")
    println(f"    Longitude: $lonMin%.6f ~ $lonMax%.6f")
    println(f"    Latitude: $latMin%.6f ~ $latMax%.6f")
    println(f"    Altitude: $altMin%.2f ~ $altMax%.2f m")
    println(s"  Output format: $format -> $outputDir")

    // Print dataset and unifiedLevel configuration
    if (dataset.isDefined || unifiedLevel.isDefined) {
      println(s"  dataset: ${dataset.getOrElse("(default)")}")
      println(s"  unifiedLevel: ${unifiedLevel.getOrElse("(default)")}")
    }

    // Check engine parameter
    if (engine.toLowerCase == "incremental") {
      println(s"  [Tip] Spark mode currently only supports unified engine, 'incremental' parameter will be ignored.")
    }
    println(s"  Execution mode: Spark cluster unified query")
    println(s"${"=" * 60}\n")

    try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
      val startTs = java.time.LocalDateTime.parse(startTime, formatter)
        .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli
      val endTs = java.time.LocalDateTime.parse(endTime, formatter)
        .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli

      // Call Spark query
      val results = spark.SparkUnifiedSpatialQuery.runSpatialQueryWithSpark(
        zkQuorum, sensorId, startTs, endTs,
        lonMin, latMin, altMin, lonMax, latMax, altMax,
        dataset, unifiedLevel
      ).toList

      println(s"\n${"=" * 60}")
      printAndExportResults(results, sensorId, format, outputDir, "spark_spatial")

    } catch {
      case ex: Exception =>
        println(s"[Error] Spark query failed: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }

  /**
   * Space + time only query (no sensor ID restriction)
   */
  private def runBBoxQuery(zkQuorum: String,
                           startTime: String, endTime: String,
                           lonMin: Double, latMin: Double, altMin: Double,
                           lonMax: Double, latMax: Double, altMax: Double,
                           format: String, outputDir: String,
                           dataset: Option[String] = None): Unit = {
    println(s"\n${"=" * 60}")
    println("Spatial query (time + space, all sensors)")
    println(s"${"=" * 60}")
    println(s"  Time range: $startTime ~ $endTime")
    println(s"  Space range:")
    println(f"    Longitude: $lonMin%.6f ~ $lonMax%.6f")
    println(f"    Latitude: $latMin%.6f ~ $latMax%.6f")
    println(f"    Altitude: $altMin%.2f ~ $altMax%.2f m")
    println(s"  Output format: $format -> $outputDir")
    if (dataset.isDefined) {
      println(s"  dataset: ${dataset.getOrElse("(default)")}")
    }
    println(s"${"=" * 60}\n")

    val query = new IncrementalFilterQuery(zkQuorum, dataset = dataset)

    try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
      val startTs = java.time.LocalDateTime.parse(startTime, formatter)
        .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli
      val endTs = java.time.LocalDateTime.parse(endTime, formatter)
        .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli

      // Time + space only conditions
      val conditions: Seq[QueryCondition] = Seq(
        SelectivityTimeRange(startTs, endTs),
        SelectivitySpatialBBox(lonMin, latMin, altMin, lonMax, latMax, altMax)
      )

      println("Execution plan:")
      val results = query.query(conditions).toList
      println(s"\n${"=" * 60}")

      printAndExportResults(results, "all", format, outputDir, "bbox")

    } finally {
      query.close()
    }
  }

  private def runSimPointQuery(zkQuorum: String, simId: String,
                                startTime: String, endTime: String,
                                xMin: Double, yMin: Double, zMin: Double,
                                xMax: Double, yMax: Double, zMax: Double,
                                format: String, outputDir: String, engine: String, mode: String,
                                withHeader: Boolean,
                                dataset: Option[String] = None, unifiedLevel: Option[Int] = None): Unit = {
    println(s"\n${"=" * 60}")
    println("Geothermal simulation point query (sim-point)")
    println(s"${"=" * 60}")
    println(s"  sim_id: $simId")
    println(s"  time_range: $startTime ~ $endTime")
    println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
    println(s"  engine: $engine")
    println(s"  mode: $mode")
    println(s"  format: $format")
    println(s"  with-header: $withHeader")
    println(s"  output: $outputDir")
    println(s"  dataset: ${dataset.getOrElse("(default)")}")
    println(s"  unifiedLevel: ${unifiedLevel.getOrElse("(default)")}")
    println(s"${"=" * 60}\n")

    if (mode.toLowerCase == "spark" && engine.toLowerCase != "unified") {
      println(s"[Error] Spark mode only supports engine=unified, current engine=$engine")
      println("[Tip] Please use --engine unified or switch to mode=serial/parallel")
      System.exit(1)
    }

    val (rawLines, delimiterName, headerLine) = mode.toLowerCase match {
      case "spark" =>
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
        val startMs = java.time.LocalDateTime.parse(startTime, formatter)
          .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
        val endMs = java.time.LocalDateTime.parse(endTime, formatter)
          .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
        SparkGeoSimPointUnifiedQuery.runQueryWithSpark(
          zkQuorum, simId, startMs, endMs,
          xMin, yMin, zMin, xMax, yMax, zMax,
          dataset, unifiedLevel
        )
      case _ =>
        val rawLines = engine.toLowerCase match {
          case "incremental" =>
            val query = new GeoSimPointIncrementalFilterQuery(
              zkQuorum,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            try {
              val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
              val startMs = java.time.LocalDateTime.parse(startTime, formatter)
                .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
              val endMs = java.time.LocalDateTime.parse(endTime, formatter)
                .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
              query.queryRawLines(simId, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
            } finally {
              query.close()
            }
          case _ =>
            val query = new GeoSimPointUnifiedQuery(
              zkQuorum,
              useUnifiedParallel = (mode == "parallel"),
              unifiedThreadPoolSize = 8,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            try {
              val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
              val startMs = java.time.LocalDateTime.parse(startTime, formatter)
                .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
              val endMs = java.time.LocalDateTime.parse(endTime, formatter)
                .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
              query.queryRawLines(simId, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
            } finally {
              query.close()
            }
        }

        val (delimiterName, headerLine) = engine.toLowerCase match {
          case "incremental" =>
            val query = new GeoSimPointIncrementalFilterQuery(
              zkQuorum,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            try {
              val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
              val startMs = java.time.LocalDateTime.parse(startTime, formatter)
                .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
              val endMs = java.time.LocalDateTime.parse(endTime, formatter)
                .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
              query.queryRawLines(simId, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
              query.getMeta
            } finally {
              query.close()
            }
          case _ =>
            val query = new GeoSimPointUnifiedQuery(
              zkQuorum,
              useUnifiedParallel = (mode == "parallel"),
              unifiedThreadPoolSize = 8,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            try {
              val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
              val startMs = java.time.LocalDateTime.parse(startTime, formatter)
                .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
              val endMs = java.time.LocalDateTime.parse(endTime, formatter)
                .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
              query.queryRawLines(simId, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
              query.getMeta
            } finally {
              query.close()
            }
        }
        (rawLines, delimiterName, headerLine)
    }

    println(s"\n${"=" * 60}")
    println(s"[Result] Queried ${rawLines.size} records")
    println(s"${"=" * 60}\n")

    if (rawLines.nonEmpty) {
      val outputDirFile = new File(outputDir)
      if (!outputDirFile.exists()) {
        outputDirFile.mkdirs()
      }

      val timestamp = System.currentTimeMillis()
      val ext = format.toLowerCase match {
        case "csv" => "csv"
        case "json" => "json"
        case _ => "txt"
      }
      val outputFile = new File(outputDirFile, s"sim_point_${simId}_${engine}_${mode}_${timestamp}.$ext")

      format.toLowerCase match {
        case "json" =>
          val writer = new PrintWriter(outputFile)
          try {
            val fieldNames = headerLine.split(if (delimiterName == "tab") "\t" else ",")
            writer.println("[")
            rawLines.zipWithIndex.foreach { case (line, idx) =>
              val values = line.split(if (delimiterName == "tab") "\t" else ",")
              val jsonFields = fieldNames.zip(values).map { case (name, value) =>
                val escapedValue = value.replace("\\", "\\\\").replace("\"", "\\\"")
                s""""$name":"$escapedValue""""
              }.mkString(", ")
              val comma = if (idx < rawLines.size - 1) "," else ""
              writer.println(s"  {$jsonFields}$comma")
            }
            writer.println("]")
            println(s"[Export] JSON file written: ${outputFile.getAbsolutePath}")
            println(s"[Export] Record count: ${rawLines.size}")
          } finally {
            writer.close()
          }
        case "csv" =>
          val writer = new PrintWriter(outputFile)
          try {
            if (withHeader) {
              val header = if (delimiterName == "tab") headerLine.replace("\t", ",") else headerLine
              writer.println(header)
            }
            rawLines.foreach { line =>
              val outputLine = if (delimiterName == "tab") line.replace("\t", ",") else line
              writer.println(outputLine)
            }
            println(s"[Export] CSV file written: ${outputFile.getAbsolutePath}")
            println(s"[Export] Record count: ${rawLines.size}")
          } finally {
            writer.close()
          }
        case _ =>
          val writer = new PrintWriter(outputFile)
          try {
            if (withHeader) {
              writer.println(headerLine)
            }
            rawLines.foreach(writer.println)
            println(s"[Export] TXT file written: ${outputFile.getAbsolutePath}")
            println(s"[Export] Record count: ${rawLines.size}")
          } finally {
            writer.close()
          }
      }
    }
  }

  private def runSimVoxelQuery(zkQuorum: String, simId: String,
                                startTime: String, endTime: String,
                                xMin: Double, yMin: Double, zMin: Double,
                                xMax: Double, yMax: Double, zMax: Double,
                                format: String, outputDir: String, mode: String, engine: String, withHeader: Boolean,
                                dataset: Option[String] = None, unifiedLevel: Option[Int] = None): Unit = {
    println(s"\n${"=" * 60}")
    println("Geothermal simulation voxel query (sim-voxel)")
    println(s"${"=" * 60}")
    println(s"  sim_id: $simId")
    println(s"  time_range: $startTime ~ $endTime")
    println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
    println(s"  format: $format -> $outputDir")
    println(s"  mode: $mode")
    println(s"  engine: $engine")
    println(s"  withHeader: $withHeader")
    println(s"  dataset: ${dataset.getOrElse("(default)")}")
    println(s"  unifiedLevel: ${unifiedLevel.getOrElse("(default)")}")
    println(s"${"=" * 60}\n")

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    val startMs = java.time.LocalDateTime.parse(startTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    val endMs = java.time.LocalDateTime.parse(endTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli

    def normalizeToComma(s: String): String = s.replace('\t', ',')

    val (rawLines, headerLine) = engine match {
      case "unified" =>
        mode match {
          case "spark" =>
            val (lines, header) = spark.SparkGeoSimVoxelUnifiedQuery.runQueryWithSpark(
              zkQuorum, simId, startMs, endMs,
              xMin, yMin, zMin, xMax, yMax, zMax,
              dataset, unifiedLevel
            )
            (lines, header)
          case "parallel" =>
            val query = new GeoSimVoxelUnifiedQuery(
              zkQuorum,
              useUnifiedParallel = true,
              unifiedThreadPoolSize = 8,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            try {
              val lines = query.queryRawLines(simId, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
              val header = query.getHeaderLine
              (lines, header)
            } finally {
              query.close()
            }
          case "serial" =>
            val query = new GeoSimVoxelUnifiedQuery(
              zkQuorum,
              useUnifiedParallel = false,
              unifiedThreadPoolSize = 8,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            try {
              val lines = query.queryRawLines(simId, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
              val header = query.getHeaderLine
              (lines, header)
            } finally {
              query.close()
            }
          case _ =>
            println(s"[Error] Unsupported mode: $mode (supported: serial, parallel, spark)")
            System.exit(1)
            (Seq.empty, "raw_line")
        }
      case "incremental" =>
        if (mode == "spark") {
          println("[Warn] Spark mode not supported for incremental engine, falling back to serial")
        }
        val query = new GeoSimVoxelIncrementalFilterQuery(
          zkQuorum,
          dataset = dataset,
          unifiedLevel = unifiedLevel,
          threadPoolSize = 8,
          getBatchSize = 500
        )
        try {
          val lines = query.queryRawLines(simId, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
          val header = query.getHeaderLine
          (lines, header)
        } finally {
          query.close()
        }
      case _ =>
        println(s"[Error] Unsupported engine: $engine (supported: unified, incremental)")
        System.exit(1)
        (Seq.empty, "raw_line")
    }

    println(s"\n${'=' * 60}")
    println(s"[Result] Queried ${rawLines.size} records")
    println(s"${"=" * 60}\n")

    if (rawLines.nonEmpty) {
      val outputDirFile = new File(outputDir)
      if (!outputDirFile.exists()) {
        outputDirFile.mkdirs()
      }

      val timestamp = System.currentTimeMillis()
      val extension = format match {
        case "csv" => "csv"
        case "json" => "json"
        case _ => "txt"
      }
      val outputFile = new File(outputDirFile, s"sim_voxel_${simId}_${timestamp}.$extension")

      val headerOut = normalizeToComma(headerLine)

      format match {
        case "csv" =>
          val writer = new PrintWriter(outputFile)
          try {
            if (withHeader) {
              writer.println(headerOut)
            }
            rawLines.foreach { line =>
              writer.println(normalizeToComma(line))
            }
            println(s"[Export] CSV file written: ${outputFile.getAbsolutePath}")
            println(s"[Export] Record count: ${rawLines.size}")
          } finally {
            writer.close()
          }
        case "json" =>
          val writer = new PrintWriter(outputFile)
          try {
            rawLines.foreach { line =>
              writer.println(s"""{"raw_line": "$line"}""")
            }
            println(s"[Export] JSON file written: ${outputFile.getAbsolutePath}")
            println(s"[Export] Record count: ${rawLines.size}")
          } finally {
            writer.close()
          }
        case _ =>
          val writer = new PrintWriter(outputFile)
          try {
            if (withHeader) {
              writer.println(headerOut)
            }
            rawLines.foreach { line =>
              writer.println(normalizeToComma(line))
            }
            println(s"[Export] TXT file written: ${outputFile.getAbsolutePath}")
            println(s"[Export] Record count: ${rawLines.size}")
          } finally {
            writer.close()
          }
      }
    }
  }
  
  /**
   * Normal query (attribute + time)
   */
  private def runQueryWithExport(zkQuorum: String, sensorId: String,
                                  startTime: String, endTime: String,
                                  format: String, outputDir: String,
                                  dataset: Option[String] = None): Unit = {
    println(s"\n${"=" * 60}")
    println("Normal query (sensor + time)")
    println(s"${"=" * 60}")
    println(s"  Sensor ID: $sensorId")
    println(s"  Time range: $startTime ~ $endTime")
    println(s"  Output format: $format -> $outputDir")
    if (dataset.isDefined) {
      println(s"  dataset: ${dataset.getOrElse("(default)")}")
    }
    println(s"${"=" * 60}\n")

    val query = new IncrementalFilterQuery(zkQuorum, dataset = dataset)

    try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
      val startTs = java.time.LocalDateTime.parse(startTime, formatter)
        .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli
      val endTs = java.time.LocalDateTime.parse(endTime, formatter)
        .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli

      val conditions: Seq[QueryCondition] = Seq(
        SensorIdEquals(sensorId),
        SelectivityTimeRange(startTs, endTs)
      )

      println("Execution plan:")
      val results = query.query(conditions).toList
      println(s"\n${"=" * 60}")

      printAndExportResults(results, sensorId, format, outputDir, "query")

    } finally {
      query.close()
    }
  }
  
  /**
   * Print and export results
   */
  private def printAndExportResults(results: List[SensorRecord], queryName: String,
                                     format: String, outputDir: String, prefix: String): Unit = {
    println(s"\nResults (${results.size} records):")
    println("-" * 100)
    println(f"${"Time"}%-22s | ${"SensorID"}%-10s | ${"Longitude"}%-12s | ${"Latitude"}%-12s | ${"Altitude"}%-10s | ${"Type"}%-15s")
    println("-" * 100)

    // Only display first 5 records
    results.take(5).foreach { record =>
      val time = formatTimestamp(record.time)
      println(f"$time%-22s | ${record.sensorId}%-10s | ${record.longitude}%-12.6f | ${record.latitude}%-12.6f | ${record.altitude}%-10.2f | ${record.sensorType}%-15s")
    }
    println("-" * 100)
    println(s"Total: ${results.size} records")
    
    if (results.nonEmpty) {
      val dir = new File(outputDir)
      if (!dir.exists()) {
        dir.mkdirs()
        println(s"\nCreated directory: $outputDir")
      }
      
      val timestamp = System.currentTimeMillis()
      val safeQueryName = queryName.replaceAll("[^a-zA-Z0-9_]", "_")
      val baseFileName = s"${prefix}_${safeQueryName}_$timestamp"
      
      format match {
        case "csv" =>
          val filePath = s"$outputDir/$baseFileName.csv"
          exportToCsv(results, filePath)
          println(s"\n✓ Exported to CSV: $filePath")
          
        case "json" =>
          val filePath = s"$outputDir/$baseFileName.json"
          exportToJson(results, filePath)
          println(s"\n✓ Exported to JSON: $filePath")
          
        case "both" =>
          val csvPath = s"$outputDir/$baseFileName.csv"
          val jsonPath = s"$outputDir/$baseFileName.json"
          exportToCsv(results, csvPath)
          exportToJson(results, jsonPath)
          println(s"\n✓ Exported to CSV:  $csvPath")
          println(s"✓ Exported to JSON: $jsonPath")
          
        case _ =>
          val filePath = s"$outputDir/$baseFileName.csv"
          exportToCsv(results, filePath)
          println(s"\n✓ Exported to CSV: $filePath")
      }
    } else {
      println("\nNo results to export.")
    }
  }
  
  private def exportToCsv(records: List[SensorRecord], filePath: String): Unit = {
    val writer = new PrintWriter(new File(filePath))
    try {
      writer.println("Time,ID,Longitude,Latitude,Altitude,Type,RowKey")
      records.foreach { record =>
        val time = formatTimestamp(record.time)
        writer.println(s"$time,${record.sensorId},${record.longitude},${record.latitude},${record.altitude},${record.sensorType},${record.rowKey}")
      }
    } finally {
      writer.close()
    }
  }
  
  private def exportToJson(records: List[SensorRecord], filePath: String): Unit = {
    val writer = new PrintWriter(new File(filePath))
    try {
      writer.println("{")
      writer.println(s"""  "query_time": "${formatTimestamp(System.currentTimeMillis())}",""")
      writer.println(s"""  "total_count": ${records.size},""")
      writer.println("""  "records": [""")
      
      records.zipWithIndex.foreach { case (record, idx) =>
        val time = formatTimestamp(record.time)
        val comma = if (idx < records.size - 1) "," else ""
        writer.println(s"""    {
      "row_key": "${record.rowKey}",
      "time": "$time",
      "timestamp": ${record.time},
      "sensor_id": "${record.sensorId}",
      "longitude": ${record.longitude},
      "latitude": ${record.latitude},
      "altitude": ${record.altitude},
      "type": "${record.sensorType}"
    }$comma""")
      }
      
      writer.println("  ]")
      writer.println("}")
    } finally {
      writer.close()
    }
  }
  
  private def formatTimestamp(timestamp: Long): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    Instant.ofEpochMilli(timestamp)
      .atZone(java.time.ZoneId.systemDefault())
      .toLocalDateTime
      .format(formatter)
  }
  
  private def runQueryExample(zkQuorum: String): Unit = {
    println("Running spatial query example...")
    runSpatialQuery(
      zkQuorum,
      "SENS_026",
      "2025-11-03T04:00:00",
      "2025-11-03T07:00:00",
      114.0, 23.2, 200,
      114.2, 23.3, 220,
      "both",
      "/test/sensor-spatial-index/result",
      "serial",
      "unified"
    )
  }
  
  private def runTest(zkQuorum: String): Unit = {
    println("Running tests...")

    println("\n[Test 1] Z3D Encoding:")
    val testLon = 114.088821
    val testLat = 23.266778
    val testAlt = 210.495
    val z3d = index.Z3DEncoder.encode(testLon, testLat, testAlt)
    println(f"  Input: ($testLon, $testLat, $testAlt)")
    println(f"  Z3D value: $z3d (0x${z3d}%016x)")

    println("\n[Test 2] Time Bucket:")
    val testTime = System.currentTimeMillis()
    val bucket = index.TimeBucket.dayBucket(testTime)
    println(s"  Timestamp: $testTime")
    println(s"  Day bucket: $bucket")

    println("\n[Test 3] Z3D Range Calculation:")
    val ranges = index.Z3DEncoder.ranges(114.0, 23.2, 200, 114.2, 23.3, 220, 100000)
    println(s"  BBox: (114.0, 23.2, 200) to (114.2, 23.3, 220)")
    println(s"  Generated ${ranges.size} Z3D ranges")

    println("\n[Test 4] HBase Connection:")
    try {
      val connection = HBaseTableManager.createConnection(zkQuorum)
      println(s"  Connected to: $zkQuorum")
      connection.close()
      println("  ✓ Connection successful")
    } catch {
      case e: Exception =>
        println(s"  ✗ Connection failed: ${e.getMessage}")
    }

    println("\n" + "=" * 60)
    println("All tests completed!")
    println("=" * 60)
  }

  /**
   * Diagnose actual rowKey format in spatial index table
   */
  private def diagnoseSpatialIndex(zkQuorum: String, limit: Int): Unit = {
    println("=" * 80)
    println("DIAGNOSE: Spatial Index Table Structure")
    println("=" * 80)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    val connection = ConnectionFactory.createConnection(conf)

    try {
      val table = connection.getTable(TableName.valueOf(HBaseTableManager.TABLE_IDX_SPATIAL))
      try {
        val scan = new Scan()
        val scanner = table.getScanner(scan)

        var count = 0
        println("\nFirst $limit rows in spatial index:")
        println("-" * 80)

        try {
          scanner.asScala.foreach { result =>
            if (count < limit) {
              val rowKeyBytes = result.getRow
              val hexKey = rowKeyBytes.map(b => f"${b & 0xFF}%02x").mkString
              val asciiKey = new String(rowKeyBytes, "ISO-8859-1").replaceAll("[^\\x20-\\x7e]", "?")

              println(f"Row $count:")
              println(f"  RowKey (hex):   $hexKey")
              println(f"  RowKey (ascii): $asciiKey")
              println(f"  RowKey length:  ${rowKeyBytes.length} bytes")

              // Try to parse first 8 bytes as Z3D
              if (rowKeyBytes.length >= 8) {
                val z3dBytes = rowKeyBytes.take(8)
                val z3dValue = Z3DEncoder.bytesToLong(z3dBytes)
                println(f"  Z3D (parsed):   $z3dValue (0x${z3dValue}%016x)")

                // Try to parse data key
                if (rowKeyBytes.length > 9 && rowKeyBytes(8) == '_') {
                  val dataKey = Bytes.toString(rowKeyBytes, 9, rowKeyBytes.length - 9)
                  println(f"  DataKey:        $dataKey")
                } else if (rowKeyBytes.length > 8) {
                  val afterPrefix = Bytes.toString(rowKeyBytes, 8, rowKeyBytes.length - 8)
                  println(f"  After Z3D:      $afterPrefix")
                }
              }

              println()
              count += 1
            }
          }
        } finally {
          scanner.close()
        }

        println(s"Total rows scanned: $count")
      } finally {
        table.close()
      }
    } finally {
      connection.close()
    }

    println("=" * 80)
  }

  /**
   * Drop all tables corresponding to the entire dataset
   */
  private def runDropDataset(zkQuorum: String, dataset: String, force: Boolean): Unit = {
    println(s"\n${"=" * 60}")
    println("Drop dataset (Drop Dataset)")
    println(s"${"=" * 60}")
    println(s"  Dataset ID: $dataset")
    if (force) {
      println(s"  Mode: Force delete (FORCE DELETE)")
    } else {
      println(s"  Mode: Dry run (DRY-RUN)")
    }
    println(s"${"=" * 60}\n")

    val connection = HBaseTableManager.createConnection(zkQuorum)
    try {
      HBaseTableManager.dropDataset(connection, dataset, force)
    } finally {
      connection.close()
    }
  }

  /**
   * Initialize volume tables (meta table + brick table)
   */
  private def runInitVolume(zkQuorum: String, datasetOpt: Option[String], unifiedLevelOpt: Option[Int]): Unit = {
    println(s"\n${"=" * 60}")
    println("Initialize Volume tables (Init Volume Tables)")
    println(s"${"=" * 60}")
    println(s"  Dataset: ${datasetOpt.getOrElse("(default)")}")
    println(s"  unifiedLevel: ${unifiedLevelOpt.getOrElse("(default)")}")
    println(s"${"=" * 60}\n")

    val connection = HBaseTableManager.createConnection(zkQuorum)
    try {
      HBaseTableManager.createVolumeTablesForDataset(connection, datasetOpt, unifiedLevelOpt)
    } finally {
      connection.close()
    }
  }

  /**
   * Export model validation (read from HBase and rebuild CSV)
   */
  private def runExportModel(
    zkQuorum: String,
    modelId: String,
    timeIsoZ: String,
    format: String,
    outputDir: String,
    datasetOpt: Option[String]
  ): Unit = {
    println(s"\n${"=" * 60}")
    println("Export model validation (Export Model)")
    println(s"${"=" * 60}")
    println(s"  Model ID: $modelId")
    println(s"  Time: $timeIsoZ")
    println(s"  Format: $format")
    println(s"  Output directory: $outputDir")
    println(s"  Dataset: ${datasetOpt.getOrElse("(default)")}")
    println(s"${"=" * 60}\n")

    // Parse time
    val timeMillis = try {
      Instant.parse(timeIsoZ).toEpochMilli
    } catch {
      case e: Exception =>
        println(s"Error: Time parsing failed: ${e.getMessage}")
        System.exit(1)
        0L
    }

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    val connection = ConnectionFactory.createConnection(conf)

    try {
      // 1. Read model metadata from meta table
      val metaTableName = HBaseTableManager.volumeMetaTableName(datasetOpt)
      val metaTable = connection.getTable(TableName.valueOf(metaTableName))
      val metaRowKey = s"$modelId|$timeMillis"
      val metaGet = new Get(Bytes.toBytes(metaRowKey))
      val metaResult = metaTable.get(metaGet)

      if (metaResult.isEmpty) {
        println(s"Error: Model meta record not found (rowKey: $metaRowKey)")
        System.exit(1)
      }

      // Read metadata
      val modelType = Bytes.toString(metaResult.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("model_type")))
      val timeIso = Bytes.toString(metaResult.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("time_iso")))
      val bboxStr = Bytes.toString(metaResult.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("bbox")))
      val nx = Bytes.toInt(metaResult.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("nx")))
      val ny = Bytes.toInt(metaResult.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("ny")))
      val nz = Bytes.toInt(metaResult.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("nz")))

      val bboxParts = bboxStr.split(",")
      val lonMin = bboxParts(0).toDouble
      val latMin = bboxParts(1).toDouble
      val zMin = bboxParts(2).toDouble
      val lonMax = bboxParts(3).toDouble
      val latMax = bboxParts(4).toDouble
      val zMax = bboxParts(5).toDouble

      println(s"Found model metadata:")
      println(s"  model_type: $modelType")
      println(s"  time_iso: $timeIso")
      println(s"  bbox: $bboxStr")
      println(s"  dimensions: $nx x $ny x $nz")

      metaTable.close()

      // 2. Read all bricks from brick table
      val brickTableName = HBaseTableManager.volumeBrickTableName(datasetOpt)
      val brickTable = connection.getTable(TableName.valueOf(brickTableName))
      
      // Scan all bricks (using prefix scan)
      val scan = new Scan()
      scan.setRowPrefixFilter(Bytes.toBytes(s"$modelId|$timeMillis|"))
      val scanner = brickTable.getScanner(scan)

      // Brick dimensions
      val Bx = 16
      val By = 16
      val Bz = 8

      // Rebuild model array (fill with NoData first)
      val modelArray = Array.fill(nx * ny * nz)(-9999f)
      var foundBricks = 0
      var missingBricks = 0

      // Calculate theoretical total brick count
      val xBlocks = math.ceil(nx.toDouble / Bx).toInt
      val yBlocks = math.ceil(ny.toDouble / By).toInt
      val zBlocks = math.ceil(nz.toDouble / Bz).toInt
      val totalExpectedBricks = xBlocks * yBlocks * zBlocks

      try {
        scanner.asScala.foreach { result =>
          val rowKey = Bytes.toString(result.getRow)
          val parts = rowKey.split("\\|")
          
          if (parts.length >= 5) {
            val tileIStr = parts(2).replace("tile_", "")
            val tileJ = parts(3).toInt
            val tileK = parts(4).toInt
            
            val tileI = tileIStr.toInt
            
            // Decompress brick data
            val payloadBytes = result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("payload_bytes"))
            val brickArray = PorosityPayloadCodec.decodeFromCompressedBytes(payloadBytes, Bx, By, Bz)
            
            // Write back to model array
            val startX = tileI * Bx
            val startY = tileJ * By
            val startZ = tileK * Bz
            val endX = math.min(startX + Bx, nx)
            val endY = math.min(startY + By, ny)
            val endZ = math.min(startZ + Bz, nz)
            
            for (x <- startX until endX) {
              for (y <- startY until endY) {
                for (z <- startZ until endZ) {
                  val modelIdx = z * nx * ny + y * nx + x
                  val brickX = x - startX
                  val brickY = y - startY
                  val brickZ = z - startZ
                  val brickIdx = brickZ * Bx * By + brickY * Bx + brickX
                  modelArray(modelIdx) = brickArray(brickIdx)
                }
              }
            }
            
            foundBricks += 1
          }
        }
      } finally {
        scanner.close()
        brickTable.close()
      }

      // Calculate missing bricks
      missingBricks = totalExpectedBricks - foundBricks

      println(s"\nBrick statistics:")
      println(s"  Found bricks: $foundBricks")
      println(s"  Theoretical total bricks: $totalExpectedBricks")
      println(s"  Missing bricks: $missingBricks")

      // 3. Re-encode as payload_b64
      val payloadB64 = PorosityPayloadCodec.encodeFromFloatArray(modelArray, nx, ny, nz)

      // 4. Output CSV file
      val outputDirFile = new File(outputDir)
      if (!outputDirFile.exists()) {
        outputDirFile.mkdirs()
      }

      val outputFile = new File(outputDirFile, s"${modelId}_${timeIsoZ}.csv")
      val writer = new PrintWriter(outputFile)

      try {
        // Write header
        writer.println("model_id,time_iso,time_millis,model_type,lon_min,lat_min,z_min,lon_max,lat_max,z_max,nx,ny,nz,payload_b64")
        
        // Write data rows
        val csvLine = s"$modelId,$timeIso,$timeMillis,$modelType,$lonMin,$latMin,$zMin,$lonMax,$latMax,$zMax,$nx,$ny,$nz,$payloadB64"
        writer.println(csvLine)

        println(s"\n✓ Export successful: ${outputFile.getAbsolutePath}")
      } finally {
        writer.close()
      }

    } finally {
      connection.close()
    }
  }

  /**
   * Volume query command (using unified engine)
   */
  private def runSimPointTempQuery(zkQuorum: String, tMin: Double, tMax: Double,
                                   startTime: String, endTime: String,
                                   xMin: Double, yMin: Double, zMin: Double,
                                   xMax: Double, yMax: Double, zMax: Double,
                                   format: String, outputDir: String, mode: String, engine: String,
                                   dataset: Option[String] = None, unifiedLevel: Option[Int] = None): Unit = {
    println(s"\n${"=" * 60}")
    println("Geothermal simulation point temperature query (sim-point-temp)")
    println(s"${"=" * 60}")
    println(f"  temp_range: $tMin%.2fK ~ $tMax%.2fK")
    println(s"  time_range: $startTime ~ $endTime")
    println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
    println(s"  engine: $engine")
    println(s"  mode: $mode")
    println(s"  format: $format")
    println(s"  output: $outputDir")
    println(s"  dataset: ${dataset.getOrElse("(default)")}")
    println(s"  unifiedLevel: ${unifiedLevel.getOrElse("(default)")}")
    println(s"${"=" * 60}\n")

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    val startMs = java.time.LocalDateTime.parse(startTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    val endMs = java.time.LocalDateTime.parse(endTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli

    val (rawLines, delimiterName, headerLine) = engine match {
      case "incremental" =>
        if (mode == "spark") {
          println("[Error] spark mode is not supported with incremental engine")
          (Seq.empty, "", "")
        } else {
          if (mode == "parallel") {
            println("[Info] parallel mode for incremental query will execute serially")
          }
          val tempQuery = new query.GeoSimPointTempIncrementalFilterQuery(
            zkQuorum,
            dataset = dataset,
            unifiedLevel = unifiedLevel
          )
          try {
            tempQuery.queryRawLinesByTemp(tMin, tMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
          } finally {
            tempQuery.close()
          }
        }
      case "unified" =>
        mode match {
          case "spark" =>
            spark.SparkGeoSimPointTempUnifiedQuery.runQueryWithSpark(
              zkQuorum, tMin, tMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax,
              dataset, unifiedLevel
            )
          case _ =>
            val query = new GeoSimPointTempUnifiedQuery(
              zkQuorum,
              useUnifiedParallel = (mode == "parallel"),
              unifiedThreadPoolSize = 8,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            query.queryRawLinesByTemp(tMin, tMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
        }
      case _ =>
        println(s"[Error] Unknown engine: $engine (supported: unified, incremental)")
        (Seq.empty, "", "")
    }

    println(s"\n${"=" * 60}")
    println(s"[Result] Queried ${rawLines.size} records")
    println(s"${"=" * 60}\n")

    if (rawLines.nonEmpty) {
      val outputDirFile = new File(outputDir)
      if (!outputDirFile.exists()) {
        outputDirFile.mkdirs()
      }

      val timestamp = System.currentTimeMillis()
      val ext = format.toLowerCase match {
        case "csv" => "csv"
        case "json" => "json"
        case _ => "txt"
      }
      val outputFile = new File(outputDirFile, s"sim_point_temp_${engine}_${mode}_${timestamp}.$ext")

      format.toLowerCase match {
          case "json" =>
            val writer = new PrintWriter(outputFile)
            try {
              val fieldNames = headerLine.split(if (delimiterName == "tab") "\t" else ",")
              writer.println("[")
              rawLines.zipWithIndex.foreach { case (line, idx) =>
                val values = line.split(if (delimiterName == "tab") "\t" else ",")
                val jsonFields = fieldNames.zip(values).map { case (name, value) =>
                  val escapedValue = value.replace("\\", "\\\\").replace("\"", "\\\"")
                  s""""$name":"$escapedValue""""
                }.mkString(", ")
                val comma = if (idx < rawLines.size - 1) "," else ""
                writer.println(s"  {$jsonFields}$comma")
              }
              writer.println("]")
            } finally {
              writer.close()
            }

          case "csv" =>
            val writer = new PrintWriter(outputFile)
            try {
              writer.println(headerLine)
              rawLines.foreach(writer.println)
            } finally {
              writer.close()
            }

          case _ =>
            val writer = new PrintWriter(outputFile)
            try {
              rawLines.foreach(writer.println)
            } finally {
              writer.close()
            }
        }

        println(s"[Export] Export complete: ${outputFile.getAbsolutePath}")
        println(s"[Export] File size: ${outputFile.length()} bytes")
      } else {
        println("[Export] No data to export")
      }
  }

  private def runSimPointVelQuery(zkQuorum: String,
                                   vxMin: Double, vxMax: Double,
                                   vyMin: Double, vyMax: Double,
                                   vzMin: Double, vzMax: Double,
                                   startTime: String, endTime: String,
                                   xMin: Double, yMin: Double, zMin: Double,
                                   xMax: Double, yMax: Double, zMax: Double,
                                   format: String, outputDir: String, mode: String, engine: String,
                                   dataset: Option[String] = None, unifiedLevel: Option[Int] = None): Unit = {
    println(s"\n${"=" * 60}")
    println("Geothermal simulation point velocity query (sim-point-vel)")
    println(s"${"=" * 60}")
    println(f"  vxRange: $vxMin%.6e ~ $vxMax%.6e")
    println(f"  vyRange: $vyMin%.6e ~ $vyMax%.6e")
    println(f"  vzRange: $vzMin%.6e ~ $vzMax%.6e")
    println(s"  timeRange: $startTime ~ $endTime")
    println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
    println(s"  engine: $engine")
    println(s"  mode: $mode")
    println(s"  dataset: ${dataset.getOrElse("(default)")}")
    println(s"  unifiedLevel: ${unifiedLevel.getOrElse("(default)")}")
    println(s"  format: $format")
    println(s"  outputDir: $outputDir")
    println(s"${"=" * 60}\n")

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    val startMs = java.time.LocalDateTime.parse(startTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    val endMs = java.time.LocalDateTime.parse(endTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli

    if (mode == "spark" && engine != "unified") {
      println("[Error] mode=spark only supports engine=unified")
      return
    }

    val (rawLines, delimiterName, headerLine) = (mode, engine) match {
      case ("spark", "unified") =>
        spark.SparkGeoSimPointVelocityUnifiedQuery.runQueryWithSpark(
          zkQuorum, vxMin, vxMax, vyMin, vyMax, vzMin, vzMax,
          startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax,
          dataset, unifiedLevel
        ) match {
          case (lines, delim, header, _, _, _, _) => (lines, delim, header)
        }
      case (_, "incremental") =>
        val query = new GeoSimPointVelocityIncrementalFilterQuery(
          zkQuorum,
          dataset,
          unifiedLevel,
          500
        )
        try {
          query.queryRawLinesByVelocity(vxMin, vxMax, vyMin, vyMax, vzMin, vzMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
        } finally {
          query.close()
        }
      case _ =>
        val query = new GeoSimPointVelocityUnifiedQuery(
          zkQuorum,
          useUnifiedParallel = (mode == "parallel"),
          unifiedThreadPoolSize = 8,
          dataset = dataset,
          unifiedLevel = unifiedLevel
        )
        try {
          query.queryRawLinesByVelocity(vxMin, vxMax, vyMin, vyMax, vzMin, vzMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
        } finally {
          query.close()
        }
    }

    println(s"\n${"=" * 60}")
    println(s"[Result] Queried ${rawLines.size} records")
    println(s"${"=" * 60}\n")

    if (rawLines.nonEmpty) {
      val outputDirFile = new File(outputDir)
      if (!outputDirFile.exists()) {
        outputDirFile.mkdirs()
      }

      val timestamp = System.currentTimeMillis()
      val ext = format.toLowerCase match {
        case "csv" => "csv"
        case "json" => "json"
        case _ => "txt"
      }
      val outputFile = new File(outputDirFile, s"sim_point_vel_${timestamp}.$ext")

      format.toLowerCase match {
        case "json" =>
          val writer = new PrintWriter(outputFile)
          try {
            val fieldNames = headerLine.split(if (delimiterName == "tab") "\t" else ",")
            writer.println("[")
            rawLines.zipWithIndex.foreach { case (line, idx) =>
              val values = line.split(if (delimiterName == "tab") "\t" else ",")
              val jsonFields = fieldNames.zip(values).map { case (name, value) =>
                val escapedValue = value.replace("\\", "\\\\").replace("\"", "\\\"")
                s""""$name":"$escapedValue""""
              }.mkString(", ")
              val comma = if (idx < rawLines.size - 1) "," else ""
              writer.println(s"  {$jsonFields}$comma")
            }
            writer.println("]")
          } finally {
            writer.close()
          }

        case "csv" =>
          val writer = new PrintWriter(outputFile)
          try {
            writer.println(headerLine)
            rawLines.foreach(writer.println)
          } finally {
            writer.close()
          }

        case _ =>
          val writer = new PrintWriter(outputFile)
          try {
            rawLines.foreach(writer.println)
          } finally {
            writer.close()
          }
      }

      println(s"[Export] Export complete: ${outputFile.getAbsolutePath}")
      println(s"[Export] File size: ${outputFile.length()} bytes")
    } else {
      println("[Export] No data to export")
    }
  }

  private def runSimVoxelTempQuery(zkQuorum: String, tMin: Double, tMax: Double,
                                    startTime: String, endTime: String,
                                    xMin: Double, yMin: Double, zMin: Double,
                                    xMax: Double, yMax: Double, zMax: Double,
                                    format: String, outputDir: String, mode: String, engine: String,
                                    dataset: Option[String] = None, unifiedLevel: Option[Int] = None): Unit = {
    println(s"\n${"=" * 60}")
    println("Geothermal simulation voxel temperature query (sim-voxel-temp)")
    println(s"${"=" * 60}")
    println(f"  temp_range: $tMin%.2fK ~ $tMax%.2fK")
    println(s"  time_range: $startTime ~ $endTime")
    println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
    println(s"  engine: $engine")
    println(s"  mode: $mode")
    println(s"  format: $format")
    println(s"  output: $outputDir")
    println(s"  dataset: ${dataset.getOrElse("(default)")}")
    println(s"  unifiedLevel: ${unifiedLevel.getOrElse("(default)")}")
    println(s"${"=" * 60}\n")

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    val startMs = java.time.LocalDateTime.parse(startTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    val endMs = java.time.LocalDateTime.parse(endTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli

    def normalizeToComma(s: String): String = s.replace('\t', ',')

    val (rawLines, headerLine): (Seq[String], String) = engine match {
      case "incremental" =>
        if (mode == "spark") {
          println("[Warn] Spark mode not supported for incremental engine, falling back to serial")
        }
        val tempQuery = new query.GeoSimVoxelTempIncrementalFilterQuery(
          zkQuorum,
          dataset = dataset,
          unifiedLevel = unifiedLevel,
          fetchBatchSize = 500
        )
        try {
          val (lines, _, header) = tempQuery.queryRawLinesByTemp(tMin, tMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
          (lines, header)
        } finally {
          tempQuery.close()
        }
      case "unified" =>
        mode match {
          case "spark" =>
            SparkGeoSimVoxelTempUnifiedQuery.runQueryWithSpark(
              zkQuorum, tMin, tMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax,
              dataset, unifiedLevel
            )
          case _ =>
            val tempQuery = new query.GeoSimVoxelTempUnifiedQuery(
              zkQuorum,
              useUnifiedParallel = (mode == "parallel"),
              unifiedThreadPoolSize = 8,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            try {
              val lines = tempQuery.queryRawLinesByTemp(tMin, tMax, startMs, endMs, xMin, yMin, zMin, xMax, yMax, zMax)
              val header = tempQuery.getHeaderLine
              (lines, header)
            } finally {
              tempQuery.close()
            }
        }
      case _ =>
        println(s"[Error] Unknown engine: $engine (supported: unified, incremental)")
        (Seq.empty[String], "raw_line")
    }

    println(s"\n${"=" * 60}")
    println(s"[Result] Queried ${rawLines.size} records")
    println(s"${"=" * 60}\n")

    if (rawLines.nonEmpty) {
      val outputDirFile = new File(outputDir)
      if (!outputDirFile.exists()) {
        outputDirFile.mkdirs()
      }

      val timestamp = System.currentTimeMillis()
      val extension = format match {
        case "csv" => "csv"
        case "json" => "json"
        case _ => "txt"
      }
      val outputFile = new File(outputDirFile, s"sim_voxel_temp_${engine}_${mode}_${timestamp}.$extension")

      val headerOut = normalizeToComma(headerLine)

      format match {
        case "csv" =>
          val writer = new PrintWriter(outputFile)
          try {
            writer.println(headerOut)
            rawLines.foreach { line =>
              writer.println(normalizeToComma(line))
            }
            println(s"[Export] CSV file written: ${outputFile.getAbsolutePath}")
            println(s"[Export] Record count: ${rawLines.size}")
          } finally {
            writer.close()
          }
        case "json" =>
          val writer = new PrintWriter(outputFile)
          try {
            rawLines.foreach { line =>
              writer.println(s"""{"raw_line": "$line"}""")
            }
            println(s"[Export] JSON file written: ${outputFile.getAbsolutePath}")
            println(s"[Export] Record count: ${rawLines.size}")
          } finally {
            writer.close()
          }
        case _ =>
          val writer = new PrintWriter(outputFile)
          try {
            writer.println(headerOut)
            rawLines.foreach { line =>
              writer.println(normalizeToComma(line))
            }
            println(s"[Export] TXT file written: ${outputFile.getAbsolutePath}")
            println(s"[Export] Record count: ${rawLines.size}")
          } finally {
            writer.close()
          }
      }

      println(s"[Export] Export complete: ${outputFile.getAbsolutePath}")
      println(s"[Export] File size: ${outputFile.length()} bytes")
    } else {
      println("[Export] No data to export")
    }
  }

  private def runSimVoxelVelQuery(zkQuorum: String,
                                   vxMin: Double, vxMax: Double,
                                   vyMin: Double, vyMax: Double,
                                   vzMin: Double, vzMax: Double,
                                   startTime: String, endTime: String,
                                   xMin: Double, yMin: Double, zMin: Double,
                                   xMax: Double, yMax: Double, zMax: Double,
                                   format: String, outputDir: String,
                                   dataset: Option[String] = None,
                                   unifiedLevel: Option[Int] = None,
                                   mode: String = "serial",
                                   engine: String = "unified"): Unit = {
    println(s"\n${"=" * 60}")
    println("Geothermal simulation voxel velocity query (sim-voxel-vel)")
    println(s"${"=" * 60}")
    println(f"  vx_range: $vxMin%.6e ~ $vxMax%.6e")
    println(f"  vy_range: $vyMin%.6e ~ $vyMax%.6e")
    println(f"  vz_range: $vzMin%.6e ~ $vzMax%.6e")
    println(s"  time_range: $startTime ~ $endTime")
    println(f"  bbox: ($xMin%.6f, $yMin%.6f, $zMin%.2f) ~ ($xMax%.6f, $yMax%.6f, $zMax%.2f)")
    println(s"  format: $format")
    println(s"  output: $outputDir")
    println(s"  dataset: ${dataset.getOrElse("(default)")}")
    println(s"  unifiedLevel: ${unifiedLevel.getOrElse("(default)")}")
    println(s"  engine: $engine")
    println(s"  mode: $mode")
    println(s"${"=" * 60}\n")

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    val startMs = java.time.LocalDateTime.parse(startTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    val endMs = java.time.LocalDateTime.parse(endTime, formatter)
      .atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli

    def normalizeToComma(s: String): String = s.replace('\t', ',')

    val (rawLines, headerLine): (Seq[String], String) = engine match {
      case "incremental" =>
        if (mode != "serial") {
          println("[Warn] Incremental engine only supports serial mode, forcing serial")
        }
        val velQuery = new query.GeoSimVoxelVelocityIncrementalFilterQuery(
          zkQuorum,
          dataset = dataset,
          unifiedLevel = unifiedLevel,
          getBatchSize = 500
        )
        try {
          velQuery.queryRawLinesByVelocity(
            vxMin, vxMax, vyMin, vyMax, vzMin, vzMax,
            startMs, endMs,
            xMin, yMin, zMin, xMax, yMax, zMax
          )
        } finally {
          velQuery.close()
        }
      case "unified" =>
        mode match {
          case "spark" =>
            val (sparkLines, sparkHeader, _, _, _, _) = spark.SparkGeoSimVoxelVelocityUnifiedQuery.runQueryWithSpark(
              zkQuorum,
              vxMin, vxMax, vyMin, vyMax, vzMin, vzMax,
              startMs, endMs,
              xMin, yMin, zMin, xMax, yMax, zMax,
              dataset, unifiedLevel
            )
            (sparkLines, sparkHeader)
          case "parallel" =>
            val velQuery = new query.GeoSimVoxelVelocityUnifiedQuery(
              zkQuorum,
              useUnifiedParallel = true,
              unifiedThreadPoolSize = 8,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            try {
              velQuery.queryRawLinesByVelocity(
                vxMin, vxMax, vyMin, vyMax, vzMin, vzMax,
                startMs, endMs,
                xMin, yMin, zMin, xMax, yMax, zMax
              )
            } finally {
              velQuery.close()
            }
          case _ =>
            val velQuery = new query.GeoSimVoxelVelocityUnifiedQuery(
              zkQuorum,
              useUnifiedParallel = false,
              dataset = dataset,
              unifiedLevel = unifiedLevel
            )
            try {
              velQuery.queryRawLinesByVelocity(
                vxMin, vxMax, vyMin, vyMax, vzMin, vzMax,
                startMs, endMs,
                xMin, yMin, zMin, xMax, yMax, zMax
              )
            } finally {
              velQuery.close()
            }
        }
      case _ =>
        println(s"[Error] Unknown engine: $engine (supported: unified, incremental)")
        (Seq.empty[String], "raw_line")
    }

    println(s"\n${"=" * 60}")
      println(s"[Result] Queried ${rawLines.size} records")
      println(s"${"=" * 60}\n")

      if (rawLines.nonEmpty) {
        val outputDirFile = new File(outputDir)
        if (!outputDirFile.exists()) {
          outputDirFile.mkdirs()
        }

        val timestamp = System.currentTimeMillis()
        val extension = format match {
          case "csv" => "csv"
          case "json" => "json"
          case _ => "txt"
        }
        val outputFile = new File(outputDirFile, s"sim_voxel_vel_${timestamp}.$extension")

        val headerOut = normalizeToComma(headerLine)

        format match {
          case "csv" =>
            val writer = new PrintWriter(outputFile)
            try {
              writer.println(headerOut)
              rawLines.foreach { line =>
                writer.println(normalizeToComma(line))
              }
              println(s"[Export] CSV file written: ${outputFile.getAbsolutePath}")
              println(s"[Export] Record count: ${rawLines.size}")
            } finally {
              writer.close()
            }
          case "json" =>
            val writer = new PrintWriter(outputFile)
            try {
              rawLines.foreach { line =>
                writer.println(s"""{"raw_line": "$line"}""")
              }
              println(s"[Export] JSON file written: ${outputFile.getAbsolutePath}")
              println(s"[Export] Record count: ${rawLines.size}")
            } finally {
              writer.close()
            }
          case _ =>
            val writer = new PrintWriter(outputFile)
            try {
              writer.println(headerOut)
              rawLines.foreach { line =>
                writer.println(normalizeToComma(line))
              }
              println(s"[Export] TXT file written: ${outputFile.getAbsolutePath}")
              println(s"[Export] Record count: ${rawLines.size}")
            } finally {
              writer.close()
            }
        }

        println(s"[Export] Export complete: ${outputFile.getAbsolutePath}")
        println(s"[Export] File size: ${outputFile.length()} bytes")
      } else {
        println("[Export] No data to export")
      }
  }

  private def runVolumePlaceholder(zkQuorum: String, args: Array[String]): Unit = {
    println("\n" + "=" * 80)
    println("Volume query (unified engine)")
    println("=" * 80)
    
    // Parse parameters
    var modelType: Option[String] = None
    var startTime: Option[String] = None
    var endTime: Option[String] = None
    var lonMin: Option[Double] = None
    var latMin: Option[Double] = None
    var zMin: Option[Double] = None
    var lonMax: Option[Double] = None
    var latMax: Option[Double] = None
    var zMax: Option[Double] = None
    var format = "csv"
    var outputDir = "."
    var mode = "serial"
    var engine = "unified"
    var dataset: Option[String] = None
    var unifiedLevel: Option[Int] = None
    var nodata: Option[String] = None
    var useBloom = false
    var debugExportBricks = false
    
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--model-type" =>
          if (i + 1 < args.length) {
            modelType = Some(args(i + 1))
            i += 2
          } else {
            i += 1
          }
        case "--time-range" =>
          if (i + 2 < args.length) {
            startTime = Some(args(i + 1))
            endTime = Some(args(i + 2))
            i += 3
          } else {
            i += 1
          }
        case "--bbox" =>
          if (i + 6 < args.length) {
            lonMin = Some(args(i + 1).toDouble)
            latMin = Some(args(i + 2).toDouble)
            zMin = Some(args(i + 3).toDouble)
            lonMax = Some(args(i + 4).toDouble)
            latMax = Some(args(i + 5).toDouble)
            zMax = Some(args(i + 6).toDouble)
            i += 7
          } else {
            i += 1
          }
        case "--format" =>
          if (i + 1 < args.length) {
            format = args(i + 1)
            i += 2
          } else {
            i += 1
          }
        case "--output-dir" =>
          if (i + 1 < args.length) {
            outputDir = args(i + 1)
            i += 2
          } else {
            i += 1
          }
        case "--mode" =>
          if (i + 1 < args.length) {
            mode = args(i + 1)
            i += 2
          } else {
            i += 1
          }
        case "--engine" =>
          if (i + 1 < args.length) {
            engine = args(i + 1)
            i += 2
          } else {
            i += 1
          }
        case "--dataset" =>
          if (i + 1 < args.length) {
            dataset = Some(args(i + 1))
            i += 2
          } else {
            i += 1
          }
        case "--unified-level" =>
          if (i + 1 < args.length) {
            unifiedLevel = Some(args(i + 1).toInt)
            i += 2
          } else {
            i += 1
          }
        case "--nodata" =>
          if (i + 1 < args.length) {
            nodata = Some(args(i + 1))
            i += 2
          } else {
            i += 1
          }
        case "--bloom" =>
          useBloom = true
          i += 1
        case "--debug-export-bricks" =>
          debugExportBricks = true
          i += 1
        case _ =>
          i += 1
      }
    }
    
    // Validate required parameters
    if (modelType.isEmpty) {
      println("Error: --model-type parameter is required")
      return
    }
    
    if (startTime.isEmpty || endTime.isEmpty) {
      println("Error: --time-range parameter is required")
      return
    }
    
    if (lonMin.isEmpty || latMin.isEmpty || zMin.isEmpty || 
        lonMax.isEmpty || latMax.isEmpty || zMax.isEmpty) {
      println("Error: --bbox parameter is required")
      return
    }
    
    // Print query parameters
    println(s"\nQuery parameters:")
    println(s"  model_type: ${modelType.get}")
    println(s"  time_range: ${startTime.get} ~ ${endTime.get}")
    println(f"  bbox: (${lonMin.get}%.6f, ${latMin.get}%.6f, ${zMin.get}%.2f) ~ (${lonMax.get}%.6f, ${latMax.get}%.6f, ${zMax.get}%.2f)")
    println(s"  format: $format")
    println(s"  output_dir: $outputDir")
    println(s"  mode: $mode")
    println(s"  engine: $engine")
    if (dataset.isDefined) println(s"  dataset: ${dataset.get}")
    if (unifiedLevel.isDefined) println(s"  unified_level: ${unifiedLevel.get}")
    if (nodata.isDefined) println(s"  nodata: ${nodata.get}")
    if (useBloom) println(s"  bloom: enabled")
    if (debugExportBricks) println(s"  debug_export_bricks: enabled")
    
    // Create Volume query instance
    val query = new VolumeQuery(
      zkQuorum,
      enableUnifiedIndex = engine.toLowerCase != "incremental",
      useUnifiedParallel = mode.toLowerCase == "parallel",
      unifiedThreadPoolSize = 8,
      dataset = dataset,
      unifiedLevel = unifiedLevel
    )
    
    try {
      // Build query conditions
      // Support two time formats: UTC time with Z (ISO 8601) and local time without Z
      val startTs = try {
        // Try to parse UTC time with Z (e.g., 2025-11-05T00:00:00Z)
        java.time.Instant.parse(startTime.get).toEpochMilli
      } catch {
        case _: Exception =>
          // If failed, try to parse local time without Z (e.g., 2025-11-05T00:00:00)
          val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
          java.time.LocalDateTime.parse(startTime.get, formatter)
            .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli
      }

      val endTs = try {
        // Try to parse UTC time with Z (e.g., 2025-11-07T00:00:00Z)
        java.time.Instant.parse(endTime.get).toEpochMilli
      } catch {
        case _: Exception =>
          // If failed, try to parse local time without Z (e.g., 2025-11-07T00:00:00)
          val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
          java.time.LocalDateTime.parse(endTime.get, formatter)
            .atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli
      }
      
      val conditions = Seq(
        VolumeModelTypeEquals(modelType.get),
        VolumeTimeRange(startTs, endTs),
        VolumeSpatialBBox(lonMin.get, latMin.get, zMin.get, lonMax.get, latMax.get, zMax.get)
      )
      
      // Execute query
      val results = query.query(conditions).toList
      
      // Get query statistics
      val queryStats = query.getQueryStats
      
      // Output results
      if (nodata.isDefined) {
        // Output NoData mask model (default behavior)
        if (debugExportBricks) {
          // Debug mode: output brick data
          println("\n[Debug mode] Skipping NoData mask model generation, outputting brick data directly")
          exportBrickResults(results, format, outputDir)
        } else {
          // Normal mode: output NoData mask model
          val nodataValue = nodata.get.toFloat
          val queryBbox = (lonMin.get, latMin.get, zMin.get, lonMax.get, latMax.get, zMax.get)
          val timeRange = (startTime.get, endTime.get)
          
          exportNoDataModel(
            results,
            nodataValue,
            outputDir,
            zkQuorum,
            dataset,
            unifiedLevel,
            queryBbox,
            timeRange,
            modelType.get,
            unifiedHitCount = queryStats.map(_.totalUnifiedRows).getOrElse(0),
            brickLookupCount = queryStats.map(_.totalDkCount).getOrElse(0),
            queryStats = queryStats
          )
        }
      } else {
        // Output brick data
        exportBrickResults(results, format, outputDir)
      }
      
    } finally {
      query.close()
    }
    
    println("\n" + "=" * 80)
    println("Volume query completed")
    println("=" * 80)
  }
  
  /**
   * Export brick query results
   */
  private def exportBrickResults(results: Seq[VolumeBrickResult], format: String, outputDir: String): Unit = {
    val outputDirFile = new File(outputDir)
    if (!outputDirFile.exists()) {
      outputDirFile.mkdirs()
    }
    
    format.toLowerCase match {
      case "csv" =>
        val outputFile = new File(outputDirFile, s"volume_bricks_${System.currentTimeMillis()}.csv")
        val writer = new PrintWriter(outputFile)
        try {
          writer.println("model_id,time_iso,time_millis,model_type,tile_i,tile_j,tile_k,lon_min,lat_min,z_min,lon_max,lat_max,z_max,nx,ny,nz,payload_b64")
          results.foreach { r =>
            val csvLine = s"${r.modelId},${r.timeIso},${r.timeMillis},${r.modelType},${r.tileI},${r.tileJ},${r.tileK},${r.lonMin},${r.latMin},${r.zMin},${r.lonMax},${r.latMax},${r.zMax},${r.nx},${r.ny},${r.nz},${r.payloadB64}"
            writer.println(csvLine)
          }
          println(s"\n✓ Export successful: ${outputFile.getAbsolutePath}")
          println(s"  Total ${results.size} brick records")
        } finally {
          writer.close()
        }
        
      case "json" =>
        val outputFile = new File(outputDirFile, s"volume_bricks_${System.currentTimeMillis()}.json")
        val writer = new PrintWriter(outputFile)
        try {
          writer.println("[")
          results.zipWithIndex.foreach { case (r, idx) =>
            val jsonLine = s"""{"model_id":"${r.modelId}","time_iso":"${r.timeIso}","time_millis":${r.timeMillis},"model_type":"${r.modelType}","tile_i":${r.tileI},"tile_j":${r.tileJ},"tile_k":${r.tileK},"lon_min":${r.lonMin},"lat_min":${r.latMin},"z_min":${r.zMin},"lon_max":${r.lonMax},"lat_max":${r.latMax},"z_max":${r.zMax},"nx":${r.nx},"ny":${r.ny},"nz":${r.nz},"payload_b64":"${r.payloadB64}"}"""
            writer.print("  " + jsonLine)
            if (idx < results.length - 1) writer.println(",")
            else writer.println()
          }
          writer.println("]")
          println(s"\n✓ Export successful: ${outputFile.getAbsolutePath}")
          println(s"  Total ${results.size} brick records")
        } finally {
          writer.close()
        }
        
      case _ =>
        println(s"Error: Unsupported format '$format'")
    }
  }
  
  /**
   * Export NoData mask model
   */
  private def exportNoDataModel(
    results: Seq[VolumeBrickResult],
    nodataValue: Float,
    outputDir: String,
    zkQuorum: String,
    dataset: Option[String],
    unifiedLevel: Option[Int],
    queryBbox: (Double, Double, Double, Double, Double, Double),
    timeRange: (String, String),
    modelType: String,
    unifiedHitCount: Int,
    brickLookupCount: Int,
    queryStats: Option[query.VolumeQueryStats]
  ): Unit = {
    println("\n" + "=" * 80)
    println("Generating NoData mask model...")
    println("=" * 80)
    
    val outputDirFile = new File(outputDir)
    if (!outputDirFile.exists()) {
      outputDirFile.mkdirs()
    }
    
    val timestamp = System.currentTimeMillis()
    val csvFile = new File(outputDirFile, s"volume_masked_${timestamp}.csv")
    val txtFile = new File(outputDirFile, s"volume_masked_${timestamp}.txt")
    
    val Bx = 16
    val By = 16
    val Bz = 8
    
    var totalModels = 0
    var skippedMetaCount = 0
    var brickDecodeFailCount = 0
    var outputModels = 0
    
    val (qLonMin, qLatMin, qZMin, qLonMax, qLatMax, qZMax) = queryBbox
    val (qStartTime, qEndTime) = timeRange
    
    // Accumulate TXT log lines
    val txtLines = scala.collection.mutable.ArrayBuffer[String]()
    
    txtLines += "=" * 80
    txtLines += "Volume NoData Mask Model Export Statistics"
    txtLines += "=" * 80
    txtLines += s"timestamp: $timestamp"
    txtLines += s"query_id: $timestamp"
    txtLines += ""
    
    // Query parameters section
    if (queryStats.isDefined) {
      val stats = queryStats.get
      txtLines += "[Query Parameters]"
      txtLines += s"model_type: ${stats.modelType}"
      txtLines += s"attrHash: ${stats.attrHash} (decimal) / 0x${stats.attrHash.toHexString} (hex)"
      txtLines += s"time_range: ${stats.timeRange._1} ~ ${stats.timeRange._2}"
      val (lonMin, latMin, zMin, lonMax, latMax, zMax) = stats.bbox
      txtLines += f"bbox: ($lonMin%.6f, $latMin%.6f, $zMin%.2f) ~ ($lonMax%.6f, $latMax%.6f, $zMax%.2f)"
      txtLines += s"unified table name: ${stats.unifiedTableName}"
      txtLines += s"brick table name: ${stats.brickTableName}"
      txtLines += s"meta table name: ${stats.metaTableName}"
      txtLines += ""
    }
    
    // Scan statistics section
    if (queryStats.isDefined) {
      val stats = queryStats.get
      txtLines += "[Scan Statistics]"
      txtLines += s"Enumerated zCells count: ${stats.zCellsCount}"
      txtLines += s"Enumerated dayBuckets count: ${stats.dayBucketsCount}"
      txtLines += s"day_bucket_range (yyyyMMdd): ${stats.dayBucketRange._1} ~ ${stats.dayBucketRange._2}"
      txtLines += s"Total scan tasks: ${stats.scanTaskCount}"
      txtLines += ""
      txtLines += "Query Statistics:"
      txtLines += s"  Total scan tasks: ${stats.scanTaskCount}"
      txtLines += s"  Total scan time: ${stats.scanDurationMs} ms (${stats.scanDurationMs / 1000.0} s)"
      txtLines += s"  Actual unified rows scanned: ${stats.totalUnifiedRows}"
      txtLines += s"  Data keys read: ${stats.totalDkCount}"
      txtLines += s"  Successful brick lookups: ${stats.totalBrickSuccess}"
      if (stats.oldFormatCompatCount > 0) {
        txtLines += s"  [WARN] Old rowkey format compatibility triggered: ${stats.oldFormatCompatCount} times"
      }
      txtLines += ""
    }
    
    val csvWriter = new PrintWriter(csvFile)
    try {
      csvWriter.println("model_id,time,model_type,lon_min,lat_min,z_min,lon_max,lat_max,z_max,nx,ny,nz,payload")
      
      val connection = HBaseTableManager.createConnection(zkQuorum)
      try {
        val metaTableName = HBaseTableManager.volumeMetaTableName(dataset)
        val metaTable = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(metaTableName))
        
        try {
          val modelGroups = results.groupBy(r => (r.modelId, r.timeMillis))
          totalModels = modelGroups.size
          
          println(s"  Grouped models: $totalModels")
          
          // Model-level brick coverage statistics
          val modelBrickStats = scala.collection.mutable.Map[String, (Int, Int, Double)]()
          
          modelGroups.foreach { case ((modelId, timeMillis), bricks) =>
            val rowKey = s"${modelId}|${timeMillis}"
            val get = new Get(Bytes.toBytes(rowKey))
            get.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("model_type"))
            get.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("time_iso"))
            get.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("bbox"))
            get.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("nx"))
            get.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("ny"))
            get.addColumn(HBaseTableManager.CF_BYTES, Bytes.toBytes("nz"))
            
            val result = metaTable.get(get)
            
            if (result.isEmpty) {
              println(s"  [WARN] Meta info missing for model $modelId (timeMillis=$timeMillis), skipping")
              skippedMetaCount += 1
            } else {
              val metaModelType = new String(result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("model_type")))
              val timeIso = new String(result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("time_iso")))
              val bboxStr = new String(result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("bbox")))
              val nx = Bytes.toInt(result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("nx")))
              val ny = Bytes.toInt(result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("ny")))
              val nz = Bytes.toInt(result.getValue(HBaseTableManager.CF_BYTES, Bytes.toBytes("nz")))
              
              val bboxParts = bboxStr.split(",")
              val modelLonMin = bboxParts(0).toDouble
              val modelLatMin = bboxParts(1).toDouble
              val modelZMin = bboxParts(2).toDouble
              val modelLonMax = bboxParts(3).toDouble
              val modelLatMax = bboxParts(4).toDouble
              val modelZMax = bboxParts(5).toDouble
              
              val dlon = (modelLonMax - modelLonMin) / nx
              val dlat = (modelLatMax - modelLatMin) / ny
              val dz = (modelZMax - modelZMin) / nz
              
              val modelArray = Array.fill(nx * ny * nz)(nodataValue)
              
              // Calculate original total bricks
              val totalBricks = math.ceil(nx.toDouble / Bx).toInt * 
                               math.ceil(ny.toDouble / By).toInt * 
                               math.ceil(nz.toDouble / Bz).toInt
              
              // Calculate hit bricks (deduplicated)
              val hitBricks = bricks.map(b => s"${b.modelId}|${b.timeMillis}|${b.tileI}|${b.tileJ}|${b.tileK}").distinct.size
              val ratio = if (totalBricks > 0) hitBricks.toDouble / totalBricks else 0.0
              
              modelBrickStats(modelId) = (hitBricks, totalBricks, ratio)
              
              var brickSuccessCount = 0
              bricks.foreach { brick =>
                try {
                  val startX = brick.tileI * Bx
                  val startY = brick.tileJ * By
                  val startZ = brick.tileK * Bz
                  
                  val brickBytes = java.util.Base64.getDecoder.decode(brick.payloadB64)
                  val brickArray = PorosityPayloadCodec.decodeFromCompressedBytes(brickBytes, Bx, By, Bz)
                  
                  for (dx <- 0 until Bx) {
                    for (dy <- 0 until By) {
                      for (dz <- 0 until Bz) {
                        val globalX = startX + dx
                        val globalY = startY + dy
                        val globalZ = startZ + dz
                        
                        if (globalX < nx && globalY < ny && globalZ < nz) {
                          val lonC = modelLonMin + (globalX + 0.5) * dlon
                          val latC = modelLatMin + (globalY + 0.5) * dlat
                          val zC = modelZMin + (globalZ + 0.5) * dz
                          
                          if (lonC >= qLonMin && lonC < qLonMax &&
                              latC >= qLatMin && latC < qLatMax &&
                              zC >= qZMin && zC < qZMax) {
                            val modelIdx = globalZ * nx * ny + globalY * nx + globalX
                            val brickIdx = dz * Bx * By + dy * Bx + dx
                            modelArray(modelIdx) = brickArray(brickIdx)
                          }
                        }
                      }
                    }
                  }
                  
                  brickSuccessCount += 1
                } catch {
                  case e: Exception =>
                    brickDecodeFailCount += 1
                }
              }
              
              val payload = PorosityPayloadCodec.encodeFromFloatArray(modelArray, nx, ny, nz)
              val csvLine = s"$modelId,$timeIso,$metaModelType,$modelLonMin,$modelLatMin,$modelZMin,$modelLonMax,$modelLatMax,$modelZMax,$nx,$ny,$nz,$payload"
              csvWriter.println(csvLine)
              
              outputModels += 1
            }
          }
          
          // Model-level brick coverage statistics section
          txtLines += "[Model-level Brick Coverage Statistics]"
          txtLines += s"Hit models count: ${modelBrickStats.size}"
          txtLines += ""
          txtLines += "model_id, hit_bricks, total_bricks, ratio"
          modelBrickStats.foreach { case (modelId, (hit, total, ratio)) =>
            txtLines += f"$modelId, $hit, $total, $ratio%.4f"
          }
          txtLines += ""
          
          println(s"  Average bricks per model: ${results.size / totalModels}")
          println(s"  Skipped meta count: $skippedMetaCount")
          println(s"  Brick decompression failures: $brickDecodeFailCount")
          println(s"  Final output models: $outputModels")
          println(s"  Output CSV path: ${csvFile.getAbsolutePath}")
          
        } finally {
          metaTable.close()
        }
      } finally {
        connection.close()
      }
      
    } finally {
      csvWriter.close()
    }
    
    // TXT file statistics
    txtLines += "[Export Statistics]"
    txtLines += s"dataset: ${dataset.getOrElse("(default)")}"
    txtLines += s"unified-level: ${unifiedLevel.getOrElse("(default)")}"
    txtLines += s"model_type: $modelType"
    txtLines += s"time_range: $qStartTime ~ $qEndTime"
    txtLines += f"query_bbox: ($qLonMin%.6f, $qLatMin%.6f, $qZMin%.2f) ~ ($qLonMax%.6f, $qLatMax%.6f, $qZMax%.2f)"
    txtLines += s"nodata_value: $nodataValue"
    txtLines += s"unified_hit_rows: $unifiedHitCount"
    txtLines += s"brick_lookups: $brickLookupCount"
    txtLines += s"grouped_models: $totalModels"
    txtLines += s"skipped_meta: $skippedMetaCount"
    txtLines += s"brick_decompress_failures: $brickDecodeFailCount"
    txtLines += s"final_output_models: $outputModels"
    txtLines += s"output_file_paths:"
    txtLines += s"  CSV: ${csvFile.getAbsolutePath}"
    txtLines += s"  TXT: ${txtFile.getAbsolutePath}"
    txtLines += "=" * 80
    
    val txtWriter = new PrintWriter(txtFile)
    try {
      txtWriter.println(txtLines.mkString("\n"))
      println(s"  Output TXT path: ${txtFile.getAbsolutePath}")
      
    } finally {
      txtWriter.close()
    }
    
    println("\n" + "=" * 80)
    println("NoData mask model generation completed")
    println("=" * 80)
  }
}