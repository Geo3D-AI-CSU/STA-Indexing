# Unified Spatio-Temporal-Attribute Indexing System for 3D Geospatial Data


## Description

This project provides a unified indexing framework for 3D geospatial simulation data. It supports both scattered point data and volumetric (voxel) data, enabling efficient queries across spatial, temporal, and attribute dimensions.

The system integrates multiple indexing strategies into a single queryable structure and supports sub-second retrieval from large-scale datasets stored in Apache HBase.

## Features

Multi-dimensional unified indexing using composite keys combining time, space, and attributes.

Support for both scattered point data and voxel data with configurable spatial granularity.

Multiple query strategies:
- Incremental filtering (SimId → Space → Time)
- Unified composite index scanning
- Attribute-specific indexes (temperature, velocity, category)

Flexible execution modes:
- Serial execution
- Multi-threaded parallel execution
- Spark distributed execution

Scalable storage using HBase with optimized row key design.

Support for CSV export and raw data extraction.

## Tech Stack

Language: Scala 2.12+  
Build Tool: Apache Maven 3.6+  
Storage: Apache HBase 2.0+  
Distributed Computing: Apache Spark 3.0+  
Coordination: Apache ZooKeeper  
Data Format: CSV  

## Getting Started

### Prerequisites

Ensure the following are installed:

- Java 8+
- Scala 2.12+
- Maven 3.6+
- Spark 3.0+
- HBase 2.0+
- ZooKeeper

### Environment Variables

```bash
export JAR="/test/sensor-spatial-index/target/sensor-spatial-index-1.0.0-with-dependencies.jar"
export ZK="node001:2181,node002:2181,node003:2181"
````

### Installation

```bash
git clone https://github.com/Geo3D-AI-CSU/unified-spatial-index.git
cd unified-spatial-index
mvn clean package -DskipTests
```

## Usage

### Data Initialization

Initialize voxel tables:

```bash
spark-submit \
  --class Main \
  --master local[4] \
  --driver-memory 4g \
  "$JAR" \
  init-volume \
  "$ZK" \
  --dataset 025 \
  --unified-level 4
```

Initialize point tables:

```bash
spark-submit \
  --class Main \
  --master local[4] \
  --driver-memory 4g \
  "$JAR" \
  init-point \
  "$ZK" \
  --dataset geosim_point_004 \
  --unified-level 9
```

### Data Ingestion

Ingest scattered point data:

```bash
spark-submit \
  --class ingest.GeoSimPointIngestJob \
  --master local[4] \
  --driver-memory 8g \
  "$JAR" \
  file:///test/sensor-spatial-index/data/points/sim_001_points.csv \
  "$ZK" \
  5000 \
  --dataset geosim_point_004 \
  --unified-level 9 \
  --indexes incremental,unified,temp_unified
```

Ingest voxel data:

```bash
spark-submit \
  --class ingest.GeoSimVoxelIngestJob \
  --master local[4] \
  --driver-memory 12g \
  "$JAR" \
  file:///test/sensor-spatial-index/data/voxels/sim_001_voxels.csv \
  "$ZK" \
  2000 \
  --dataset geosim_voxel_001 \
  --unified-level 8 \
  --indexes unified \
  --build-bloom
```

### Query Operations

Query scattered points (serial):

```bash
spark-submit \
  --class Main \
  --master local[2] \
  --driver-memory 4g \
  "$JAR" \
  sim-point \
  "$ZK" \
  2 \
  "2024-02-01T05:00:00" \
  "2024-08-01T06:00:00" \
  4144520 647430 -2700 \
  4144700 647670 -2300 \
  csv \
  /test/result/geosim-point \
  --mode serial \
  --dataset test_point_007 \
  --unified-level 9 \
  --engine incremental \
  --with-header true
```

Query scattered points (parallel):

```bash
spark-submit \
  --class Main \
  --master local[4] \
  --driver-memory 8g \
  "$JAR" \
  sim-point \
  "$ZK" \
  2 \
  "2024-02-01T05:00:00" \
  "2024-08-01T06:00:00" \
  4144520 647430 -2700 \
  4144700 647670 -2300 \
  csv \
  /test/result/geosim-point \
  --mode parallel \
  --dataset test_point_008 \
  --unified-level 9 \
  --engine unified \
  --with-header true
```

Query voxel data:

```bash
spark-submit \
  --class Main \
  --master local[2] \
  --driver-memory 4g \
  "$JAR" \
  volume \
  "$ZK" \
  --model-type channelized \
  --time-range "2025-11-03T00:00:00Z" "2025-11-07T00:00:00Z" \
  --bbox 113.30 22.30 0 114.00 22.90 1000 \
  --format csv \
  --output-dir /test/sensor-spatial-index/result/ \
  --mode serial \
  --engine unified \
  --dataset 025 \
  --unified-level 4 \
  --nodata -9999 \
  --bloom
```

## Dataset Management

```bash
spark-submit \
  --class Main \
  --master local[4] \
  --driver-memory 4g \
  "$JAR" \
  drop-dataset \
  "$ZK" \
  --dataset geosim_voxel_001 \
  --force
```

## Project Structure

```text
src/main/scala/
├── index/
├── ingest/
├── model/
├── query/
│   ├── *UnifiedQuery.scala
│   ├── *IncrementalFilterQuery.scala
│   └── VolumeQuery.scala
├── spark/
├── storage/
├── util/
└── Main.scala
```

## Configuration Options

Common parameters:

* `--dataset`: dataset identifier (required)
* `--unified-level`: spatial granularity
* `--mode`: serial, parallel, spark
* `--engine`: incremental or unified
* `--indexes`: index types
* `--with-header`: include CSV header

Index types:

* `incremental`
* `unified`
* `temp_unified`
* `velocity_unified`

## Performance Tuning

Recommended memory settings:

Points:

* driver: 4–8g
* executor: 4–8g

Voxels:

* driver: 8–12g
* executor: 8–12g

Spark configuration example:

```bash
--master yarn --deploy-mode client \
--num-executors 3 \
--executor-cores 4 \
--executor-memory 4g \
--driver-memory 2g
```


MIT License

```
```
