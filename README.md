# Micro-Partitioned Columnar SQL Query Engine (Java 21)

A zero-dependency, multi-threaded analytical SQL query engine built in **Java 21** from first principles. Designed around modern cloud data warehouse internals: immutable columnar micro-partitions, min/max zone-map pruning, column projection pruning, vectorized batch execution with selection vectors, dynamic morsel-driven parallelism, lock-free primitive hash joins, and a bounded sharded LRU chunk cache.

## Architecture

```text
SQL Query
   │
   ▼
SqlLexer + SqlParser (Hand-written recursive-descent parser -> sealed Expr AST)
   │
   ▼
QueryPlanner (Rule-based optimizer)
   ├─ Conjunctive Predicate Pushdown (splits AND / BETWEEN onto per-table scans)
   ├─ Micro-Partition Zone-Map Pruning (skips disjoint [min, max] partitions before I/O)
   ├─ Column Projection Pruning (reads only referenced column byte ranges)
   └─ Cost-Aware Build/Probe Selection (places smaller surviving relation on build side)
   │
   ▼
QueryExecutor (Morsel-driven parallel vectorized execution over ExecutorService)
   ├─ ShardedLruCache (16 lock-striped shards, bounded memory budget)
   ├─ MicroPartitionCodec (positional FileChannel reads, Plain / RLE / Dictionary / Dict+RLE)
   ├─ VectorBatch (4,096-row zero-copy slices + selection vectors, primitive arrays)
   ├─ BuildHashTable (flat open-addressing primitive long[] hash table, lock-free parallel probe)
   └─ Thread-Local HashAggregate -> Associative Parallel Merge -> Sort / Limit
```

## Key Engineering Highlights

- **Immutable `.mpart` Columnar Micro-Partitions:** Data is partitioned into 65,536-row immutable files (`part-XXXXX.mpart`). Each column independently selects **Plain**, **RLE**, **Dictionary**, or **Dictionary + RLE** encoding based on compression ratio, and records exact byte offsets plus min/max zone maps in catalog metadata.
- **True I/O Avoidance:** Unreferenced columns are never read from disk (positional `FileChannel.read(buf, offset)`), and partitions whose zone maps cannot satisfy pushed `WHERE` predicates are skipped before opening the file.
- **Zero-Boxing Vectorized Hot Paths:** Columns are represented by a sealed `ColumnVector` hierarchy backed by primitive `long[]`, `double[]`, `int[]` (epoch-day dates), and dictionary-coded `String[] + int[]` arrays. Filters operate directly on primitive loops and emit compact `int[]` selection vectors (late materialization).
- **Contention-Free Parallelism:** Surviving micro-partitions are dynamically claimed via an `AtomicInteger` morsel counter across worker threads. Hash joins probe an immutable flat open-addressing `BuildHashTable` with zero locking, and aggregations accumulate into thread-local states before a final associative merge.
- **Bounded Sharded LRU Cache:** Decoded `(table, partitionId, columnIdx)` chunks are cached across 16 independent lock stripes (`ReentrantLock` + access-ordered `LinkedHashMap`), eliminating global lock contention under parallel scans.

## Benchmarks (TPC-H SF 1.0)

Measured on **Apple M5 (10 CPU cores, 16 GB RAM)** with **OpenJDK 21.0.13.1** (`-Xmx4g`, 2 warmup runs, median of 5 timed runs). Every query result is verified numerically identical against a single-threaded row-oriented Volcano-iterator baseline reading full row-major binary files.

- **Dataset (SF 1.0):** `150,000` `customer` rows (3 partitions) · `1,500,000` `orders` rows (23 partitions) · `6,003,889` `lineitem` rows (92 partitions)
- **Storage Footprint:** `426.2 MB` columnar vs. `497.7 MB` row-oriented (`14.4%` compression savings)

### End-to-End Query Latency & I/O Reduction (10 Threads)

| Query | Description | Row Baseline | Columnar (Cold) | Columnar (Warm Cache) | Cold Speedup | Warm Speedup | Partitions Pruned | Bytes Skipped |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **TPC-H Q6** | Selective filter + global aggregate | `1,961.8 ms` | `6.2 ms` | `2.3 ms` | **318.8x** | **851.0x** | `78.3%` (72/92) | `91.5%` |
| **TPC-H Q1** | Full scan + 6/11 projection + grouped agg | `2,373.0 ms` | `65.4 ms` | `18.9 ms` | **36.3x** | **125.4x** | `0.0%` (0/92) | `58.2%` |
| **TPC-H Q3** | 3-way hash join + pruning + top-10 sort | `1,849.5 ms` | `48.1 ms` | `24.5 ms` | **38.5x** | **75.6x** | `43.2%` | `79.6%` |

### Multi-Core Thread Scaling (Cold Cache, Median Latency in ms)

| Query | 1T | 2T | 4T | 8T | 10T | Scaling (1T -> 10T) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| **TPC-H Q6** | `27.7` | `13.9` | `7.1` | `6.3` | `4.4` | **6.33x** |
| **TPC-H Q1** | `165.4` | `85.9` | `52.1` | `31.4` | `27.9` | **5.94x** |
| **TPC-H Q3** | `79.7` | `60.0` | `46.1` | `39.5` | `38.0` | **2.10x** *(bounded by sequential build phase)* |

## Quick Start

Requires **JDK 21+** (zero external runtime dependencies).

```bash
# 1. Compile and run all unit, concurrency, and baseline-equivalence tests
make test

# 2. Run the full TPC-H SF 1.0 benchmark suite (~7.65M total rows)
make benchmark

# 3. Launch the interactive SQL shell (supports EXPLAIN SELECT ... and 'demo')
make shell
```

## Project Layout

```text
src/
├── main/java/dev/wixforth/columnar/
│   ├── schema/     # ColumnType, ColumnDef, Schema, DateUtil
│   ├── vector/     # Sealed ColumnVector hierarchy + VectorBatch (selection vectors)
│   ├── storage/    # MicroPartitionCodec (.mpart), ColumnStats, ShardedLruCache, Database
│   ├── sql/        # SqlLexer, recursive-descent SqlParser, sealed Expr AST, QueryPlanner
│   ├── exec/       # Morsel-parallel QueryExecutor, VectorExprEvaluator, BuildHashTable, AggregateState
│   ├── baseline/   # Single-threaded row-oriented RowStoreTable & RowBaselineExecutor
│   ├── benchmark/  # Deterministic TpchDataGenerator & BenchmarkMain
│   └── cli/        # Interactive SqlShellMain with EXPLAIN support
└── test/java/dev/wixforth/columnar/
    └── TestSuite.java  # Encoding, pruning, parser, 16-thread cache stress, and parity tests
```

## License

MIT License — see [LICENSE](LICENSE).
