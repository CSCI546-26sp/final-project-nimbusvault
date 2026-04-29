# NimbusVault

**Adaptive Distributed Object Store with Self-Adapting Replication**

NimbusVault is a fault-tolerant, strongly consistent distributed storage system built on a Raft-inspired metadata consensus layer. It features an adaptive replication engine that dynamically adjusts per-chunk replica counts (RF) based on real-time access patterns — promoting hot data to higher RF and demoting cold data — without manual intervention or downtime.

---

## Prerequisites

| Dependency | Version |
|---|---|
| CMake | ≥ 3.20 |
| C++ Compiler | C++17 (GCC 9+ / Clang 10+) |
| gRPC | with `grpc_cpp_plugin` |
| Protocol Buffers | `protoc` on PATH |
| RocksDB | + Snappy (for server builds) |
| Python 3 | for analysis & linearizability checks |
| spdlog / GoogleTest | fetched automatically via CMake `FetchContent` |

---

## Building

### Full build (servers + client + tests)

```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### Client-only build (no RocksDB required)

```bash
mkdir -p build && cd build
cmake .. -DNIMBUS_BUILD_CLIENT_ONLY=ON
make -j$(nproc)
```

### Build targets

| Binary | Description |
|---|---|
| `meta_server` | Metadata server node (leader or follower) |
| `storage_server` | Storage node for chunk data |
| `nimbus_cli` | Admin CLI for put/get/del/info/add-replica |
| `workload_runner` | Benchmark workload generator |
| `unit_tests` | Google Test unit test binary |

---

## Running the Cluster

### Start a local development cluster

```bash
./scripts/start_cluster.sh [--mode baseline|adaptive] \
                           [--storage-nodes N] \
                           [--build-dir DIR]
```

| Flag | Default | Description |
|---|---|---|
| `--mode=` | `adaptive` | Workload mode (`baseline` or `adaptive`) |
| `--storage-nodes=` | `3` | Number of storage nodes to start |
| `--build-dir=` | `./build` | Directory containing compiled binaries |

This starts a **3-node metadata cluster** (1 leader + 2 followers) on ports `9100–9102` and **N storage nodes** starting at port `9200`. Press `Ctrl+C` to stop.

**Logs** are written to `/tmp/nimbusvault/meta{0,1,2}/stdout.log` and `/tmp/nimbusvault/storage{0..N}/stdout.log`.

### Using the CLI

Once the cluster is running:

```bash
# Store data
./build/nimbus_cli --meta 127.0.0.1:9100 put my-file "Hello NimbusVault!"

# Read data back
./build/nimbus_cli --meta 127.0.0.1:9100 get my-file

# Inspect chunk metadata (RF, state, replicas)
./build/nimbus_cli --meta 127.0.0.1:9100 info my-file

# Delete a chunk
./build/nimbus_cli --meta 127.0.0.1:9100 del my-file

# Manually add a replica to a specific storage node
./build/nimbus_cli --meta 127.0.0.1:9100 add-replica my-file sn3
```

---

## Scripts Reference

### Demo Scripts (`scripts/`)

| Script | Usage | Description |
|---|---|---|
| `scripts/start_cluster.sh` | `bash scripts/start_cluster.sh` | Starts a 3-meta + N-storage local cluster. Foreground process; `Ctrl+C` to stop. |
| `scripts/demo.sh` | `bash scripts/demo.sh` | **Interactive 5-scene demo.** Walks through put/get, adaptive tiering (RF 3→5 promotion), fault tolerance (kill a node, reads still work), and strong consistency. Requires a running cluster. |
| `scripts/demo_watch.sh` | `bash scripts/demo_watch.sh` | **Live replica monitor.** Refreshes every 1s showing per-chunk RF, tier (HOT/WRM/CLD), state (STABLE/TRANSITIONING), and replica locations. Run in a second terminal alongside `demo.sh`. |

**Recommended demo workflow** (two terminals):

```bash
# Terminal 1: Start the cluster
bash scripts/start_cluster.sh --storage-nodes=5

# Terminal 2: Start the live monitor
bash scripts/demo_watch.sh

# Terminal 1 (new shell): Run the interactive demo
bash scripts/demo.sh
```

---

### Benchmark Scripts (`bench/scripts/`)

| Script | Usage | Description |
|---|---|---|
| `bench/scripts/run_benchmark.sh` | `bash bench/scripts/run_benchmark.sh` | Quick single benchmark run. Starts a cluster and runs the workload in default mode, and saves results in a .csv file in the same directory |

#### `run_benchmark.sh`

```bash
bash bench/scripts/run_benchmark.sh
```

### Testing Scripts

#### Unit Tests

```bash
# Build and run all unit tests
cd build && ctest --output-on-failure

# Or run directly
./build/unit_tests
```

Covers: WAL, coordinator, meta integration, recovery, placement policy, chunk store, history tracer, replica repair, and client retry logic.

#### Integration Tests

```bash
bash tests/integration/run_integration.sh [--build-dir DIR] [--tests=T1,T3,T6]
```

| Flag | Description |
|---|---|
| `--build-dir=` | Path to compiled binaries |
| `--tests=` | Comma-separated list of tests to run (default: all) |

| Test | Description |
|---|---|
| T1 | Basic put + get roundtrip |
| T2 | Delete removes chunk |
| T3 | Overwrite — second version wins |
| T4 | 50 distinct chunks all readable after write |
| T5 | Read-after-write with RF=3 |
| T6 | Partial failure tolerance — kill sn2, reads still succeed |
| T7 | add-replica via CLI, chunk remains readable |
| T8 | Meta follower catch-up — consistent view |

Starts an **isolated** cluster (ports `19300`/`19400+`), runs the selected tests, and cleans up automatically.