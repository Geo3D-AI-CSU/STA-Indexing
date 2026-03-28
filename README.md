# 🌐 Unified Spatio-Temporal-Attribute Indexing System for 3D Geospatial Data

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Scala](https://img.shields.io/badge/Scala-2.12%2B-blue.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.0%2B-red.svg)](https://spark.apache.org/)
[![HBase](https://img.shields.io/badge/Apache%20HBase-2.0%2B-orange.svg)](https://hbase.apache.org/)
[![Maven](https://img.shields.io/badge/Maven-3.6%2B-green.svg)](https://maven.apache.org/)

> A high-performance unified indexing framework for **3D scattered points** and **volumetric (voxel) data**, enabling efficient queries across **time**, **space**, and **multiple attributes** with flexible execution engines.

---

## 📖 Description

This project implements a comprehensive indexing solution for geospatial simulation data, supporting both **scattered point clouds** (e.g., geothermal monitoring points) and **voxel-based volumetric data** (e.g., geological models). It provides unified indexing strategies that combine temporal, spatial, and attribute dimensions into a single queryable structure, enabling sub-second retrieval from massive datasets stored in Apache HBase.

The system supports multiple query paradigms including **incremental filtering**, **unified composite indexing**, and **distributed Spark-based execution**—making it suitable for both real-time analytics and large-scale batch processing scenarios in geological, meteorological, and environmental monitoring applications.

[Insert Architecture Diagram Here]

---

## ✨ Features

- 🎯 **Multi-Dimensional Unified Indexing** — Composite index keys combining time, space (Z3D/Hilbert encoding), and attributes (velocity vectors, temperature scalars, categorical IDs)
- 🧊 **Dual Data Model Support** — Seamlessly handle both scattered points and volumetric voxel data with configurable spatial granularity (levels 4-9)
- ⚡ **Multiple Query Strategies** — 
  - **Incremental Filtering**: Layered index intersection (SimId → Space → Time)
  - **Unified Indexing**: Single composite index scan for optimal performance
  - **Attribute-specific indexes**: Velocity (Vx/Vy/Vz), Temperature, and Category-based queries
- 🚀 **Flexible Execution Engines** — Single-threaded, multi-threaded parallel, and Apache Spark distributed execution (local/YARN)
- 🔧 **Advanced Features** — Bloom filter support for performance optimization, configurable no-data values, header preservation
- 📊 **Scalable Storage** — Apache HBase backend with optimized row key design for efficient range scans
- 🗂️ **Data Export** — Support for CSV and raw format export with model extraction capabilities

---

## 🛠 Tech Stack

| Category | Technologies |
|----------|-------------|
| **Language** | Scala 2.12+ |
| **Build Tool** | Apache Maven 3.6+ |
| **Big Data Storage** | Apache HBase 2.0+ |
| **Distributed Computing** | Apache Spark 3.0+ (Local / YARN) |
| **Spatial Indexing** | Z3D Encoding, Hilbert Curve, Time Bucketing |
| **Data Formats** | CSV, Parquet (via Spark) |
| **Cluster Coordination** | Apache ZooKeeper |

---

## 🚀 Getting Started

### Prerequisites

Ensure you have the following installed and configured:

- **Java 8+** (JDK 1.8 or higher)
- **Scala 2.12+**
- **Apache Maven 3.6+**
- **Apache Spark 3.0+** (local or cluster mode)
- **Apache HBase 2.0+** (running cluster with ZooKeeper)
- **Apache ZooKeeper** (for HBase coordination)

### Environment Variables

```bash
export JAR="/test/sensor-spatial-index/target/sensor-spatial-index-1.0.0-with-dependencies.jar"
export ZK="node001:2181,node002:2181,node003:2181"
```

### Installation

1. **Clone the repository**

```bash
git clone https://github.com/yourusername/unified-spatial-index.git
cd unified-spatial-index
```

2. **Build the project with Maven**

```bash
mvn clean package -DskipTests
```

3. **Make scripts executable** (if using provided shell scripts)

```bash
chmod +x /test/sensor-spatial-index/upload_points.sh
chmod +x /test/sensor-spatial-index/upload_voxels.sh
```

---

## 💻 Usage

### 📁 Data Initialization

#### Initialize Volume (Voxel) Tables

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

#### Initialize Point Tables

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

---

### � Data Ingestion

#### Ingest Scattered Point Data

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

**Parameters:**
- `5000` — Batch size for HBase writes
- `--indexes` — Index types to build: `incremental`, `unified`, `temp_unified`, `velocity_unified`

#### Ingest Volumetric (Voxel) Data

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

#### Alternative: Volume Voxel Ingest with Bloom Filter

```bash
spark-submit \
  --class ingest.VolumeVoxelIngestJob \
  --master local[4] \
  --driver-memory 8g \
  "$JAR" \
  file:///test/sensor-spatial-index/data/geological_data_1.csv \
  "$ZK" \
  1000 \
  --dataset 025 \
  --indexes unified \
  --unified-level 4 \
  --build-bloom
```

---

### 🔍 Query Operations

#### Query Scattered Points (Serial Mode)

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

**Parameters:**
- `2` — SimId/SensorId to query
- Time range: `start_time` `end_time`
- Bounding box: `x_min y_min z_min x_max y_max z_max`
- `--mode` — `serial`, `parallel`, or `spark`
- `--engine` — `incremental` or `unified`

#### Query Scattered Points (Parallel Mode)

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

#### Query Scattered Points (Spark/YARN Mode)

```bash
spark-submit \
  --class Main \
  --master yarn \
  --deploy-mode client \
  --num-executors 3 \
  --executor-cores 4 \
  --executor-memory 4g \
  --driver-memory 2g \
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
  --mode spark \
  --dataset test_point_005 \
  --unified-level 9 \
  --engine unified \
  --with-header true
```

#### Query Volumetric Data (Unified Index)

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

#### Query Volumetric Data (YARN Cluster Mode)

```bash
spark-submit \
  --class Main \
  --master yarn \
  --driver-memory 8g \
  --executor-memory 8g \
  "$JAR" \
  volume \
  "$ZK" \
  faulted \
  "2025-11-03T00:00:00Z" \
  "2025-11-09T00:00:00Z" \
  113.20 22.10 0 \
  113.80 22.70 1000 \
  csv \
  /test/geo/result \
  spark \
  unified \
  --dataset 008 \
  --unified-level 4 \
  --nodata -9999 \
  --bloom on
```

---

### 📤 Data Export

#### Export Model Data

```bash
spark-submit \
  --class Main \
  --master local[4] \
  --driver-memory 4g \
  "$JAR" \
  export-model \
  "$ZK" \
  MODEL_001 \
  "2025-01-08T10:00:00Z" \
  csv \
  /test/sensor-spatial-index/result \
  --dataset 020
```

#### Export Subcube Data

```bash
spark-submit \
  --class Main \
  --master local[4] \
  --driver-memory 4g \
  "$JAR" \
  export-model \
  "$ZK" \
  subcube_0000_7fa86f77 \
  "2025-11-05T17:00:53Z" \
  csv \
  /test/sensor-spatial-index/result \
  --dataset 020
```

---

### 🗑️ Dataset Management

#### Drop Dataset (with force flag)

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

---

## 📁 Project Structure

```
src/main/scala/
├── index/          # Index key encoders (Z3D, UnifiedIndexKey, TimeBucket, Hilbert)
├── ingest/         # Data ingestion jobs (Point, Voxel, Volume)
├── model/          # Data models (SensorRecord, VolumeBrickResult, GeoSimPointLine, GeoSimVoxelLine)
├── query/          # Query implementations
│   ├── *UnifiedQuery.scala          # Unified index queries
│   ├── *IncrementalFilterQuery.scala # Incremental filtering queries
│   └── VolumeQuery.scala             # Volume-specific queries
├── spark/          # Spark distributed query implementations
├── storage/        # HBase table management and writers
├── util/           # Utility classes (PorosityPayloadCodec, etc.)
└── Main.scala      # Main entry point with CLI commands
```

---

## � Configuration Options

### Common Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--dataset` | Dataset identifier | required |
| `--unified-level` | Spatial index granularity (4-9) | 4 (voxel), 9 (point) |
| `--mode` | Execution mode: `serial`, `parallel`, `spark` | `serial` |
| `--engine` | Query engine: `incremental`, `unified` | `unified` |
| `--indexes` | Index types to build | `unified` |
| `--build-bloom` | Enable Bloom filter | false |
| `--with-header` | Include header in output | true |
| `--nodata` | No-data value for export | -9999 |

### Index Types

- `incremental` — Layered index: SimId → Space → Time
- `unified` — Composite index: SimId + Space + Time
- `temp_unified` — Temperature + Space + Time
- `velocity_unified` — Velocity (Vx/Vy/Vz) + Space + Time

---

## 📊 Performance Tuning

### Memory Configuration

| Data Type | Driver Memory | Executor Memory | Notes |
|-----------|--------------|-----------------|-------|
| Points | 4-8g | 4-8g | Increase for large datasets |
| Voxels | 8-12g | 8-12g | Higher memory for volume data |
| Export | 4g | 4g | Adjust based on output size |

### Spark Configuration

```bash
# Local mode (development)
--master local[4] --driver-memory 4g

# YARN mode (production)
--master yarn --deploy-mode client \
--num-executors 3 --executor-cores 4 \
--executor-memory 4g --driver-memory 2g
```

---

## 🤝 Contributing

Contributions are welcome! Whether you're fixing bugs, adding new index strategies, or improving documentation, please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

For major changes, please open an issue first to discuss what you would like to change.

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Apache Spark community for distributed computing capabilities
- Apache HBase team for the robust NoSQL storage engine
- Z-order curve and Hilbert curve research for spatial indexing inspiration
- Geospatial simulation and geothermal monitoring research communities

---

## 📞 Support

For questions, issues, or feature requests, please:

- Open an [Issue](https://github.com/yourusername/unified-spatial-index/issues)
- Contact the maintainers at [your-email@example.com]

---

<p align="center">
  <sub>Built with ❤️ for high-performance geospatial analytics</sub>
</p>
