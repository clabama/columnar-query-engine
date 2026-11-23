package dev.wixforth.columnar;

import dev.wixforth.columnar.baseline.RowBaselineExecutor;
import dev.wixforth.columnar.benchmark.BenchmarkMain;
import dev.wixforth.columnar.benchmark.TpchDataGenerator;
import dev.wixforth.columnar.exec.QueryExecutor;
import dev.wixforth.columnar.exec.QueryResult;
import dev.wixforth.columnar.schema.ColumnDef;
import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.DateUtil;
import dev.wixforth.columnar.schema.Schema;
import dev.wixforth.columnar.sql.LogicalPlan;
import dev.wixforth.columnar.sql.QueryPlanner;
import dev.wixforth.columnar.sql.SelectStatement;
import dev.wixforth.columnar.sql.SqlParser;
import dev.wixforth.columnar.storage.Database;
import dev.wixforth.columnar.storage.EncodingType;
import dev.wixforth.columnar.storage.MicroPartitionCodec;
import dev.wixforth.columnar.storage.PartitionMetadata;
import dev.wixforth.columnar.storage.ShardedLruCache;
import dev.wixforth.columnar.storage.TableStorage;
import dev.wixforth.columnar.vector.ColumnVector;
import dev.wixforth.columnar.vector.DoubleColumnVector;
import dev.wixforth.columnar.vector.IntColumnVector;
import dev.wixforth.columnar.vector.LongColumnVector;
import dev.wixforth.columnar.vector.StringColumnVector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Self-contained unit, integration, equivalence, and concurrency stress test suite.
 */
public final class TestSuite {
    private static int passed = 0;
    private static int failed = 0;

    @FunctionalInterface
    interface TestCase {
        void run() throws Exception;
    }

    public static void main(String[] args) {
        runTest("Encoding round-trip (Plain, RLE, Dictionary, Dictionary+RLE)", TestSuite::testEncodingsRoundTrip);
        runTest("Zone-map micro-partition pruning & projection pruning", TestSuite::testPartitionAndProjectionPruning);
        runTest("SQL lexer & recursive-descent parser", TestSuite::testSqlParser);
        runTest("ShardedLruCache bounded eviction & 16-thread concurrency stress", TestSuite::testShardedLruCacheConcurrency);
        runTest("Single-table filters (AND, OR, BETWEEN, strings, dates) & empty results", TestSuite::testFiltersAndEdgeCases);
        runTest("2-way join with automatic build/probe cost swap", TestSuite::testTwoWayJoinSwap);
        runTest("End-to-end TPC-H Q1, Q3, Q6 equivalence vs. row baseline (1T & 8T)", TestSuite::testTpchEquivalence);
        runTest("Concurrent multi-query execution against shared Database & cache", TestSuite::testConcurrentQueries);

        System.out.printf("%nTest Summary: %d passed, %d failed%n", passed, failed);
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void runTest(String name, TestCase tc) {
        long t0 = System.nanoTime();
        try {
            tc.run();
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            System.out.printf("  [PASS] %-72s (%d ms)%n", name, ms);
            passed++;
        } catch (Throwable t) {
            System.out.printf("  [FAIL] %s: %s%n", name, t.getMessage());
            t.printStackTrace(System.out);
            failed++;
        }
    }

    private static void testEncodingsRoundTrip() throws Exception {
        Path dir = Files.createTempDirectory("test-enc-");
        try {
            Schema schema = new Schema(List.of(
                    new ColumnDef("id_plain", ColumnType.INT64),
                    new ColumnDef("grp_rle", ColumnType.INT64),
                    new ColumnDef("price", ColumnType.DOUBLE),
                    new ColumnDef("dt_rle", ColumnType.DATE),
                    new ColumnDef("flag_dict_rle", ColumnType.STRING),
                    new ColumnDef("tag_dict", ColumnType.STRING)
            ));
            int n = 1000;
            long[] ids = new long[n];
            long[] grps = new long[n];
            double[] prices = new double[n];
            int[] dates = new int[n];
            int[] flagCodes = new int[n];
            int[] tagCodes = new int[n];
            String[] flags = {"A", "B"};
            String[] tags = {"T0", "T1", "T2", "T3", "T4"};

            int baseDay = DateUtil.parseEpochDay("1995-01-01");
            for (int i = 0; i < n; i++) {
                ids[i] = i * 7L;
                grps[i] = i / 100; // long runs -> RLE
                prices[i] = i * 1.25;
                dates[i] = baseDay + (i / 200); // long runs -> RLE
                flagCodes[i] = (i < 500) ? 0 : 1; // Dictionary + RLE
                tagCodes[i] = i % tags.length; // Dictionary plain
            }

            ColumnVector[] cols = {
                    new LongColumnVector(ids, n),
                    new LongColumnVector(grps, n),
                    new DoubleColumnVector(prices, n),
                    new IntColumnVector(dates, n),
                    new StringColumnVector(flags, flagCodes, n),
                    new StringColumnVector(tags, tagCodes, n)
            };

            PartitionMetadata meta = MicroPartitionCodec.writePartition(0, dir.resolve("p0.mpart"), schema, cols, n);
            assertEquals(EncodingType.PLAIN, meta.stats(0).encoding, "id_plain encoding");
            assertEquals(EncodingType.RLE, meta.stats(1).encoding, "grp_rle encoding");
            assertEquals(EncodingType.RLE, meta.stats(3).encoding, "dt_rle encoding");
            assertEquals(EncodingType.DICTIONARY_RLE, meta.stats(4).encoding, "flag_dict_rle encoding");
            assertEquals(EncodingType.DICTIONARY, meta.stats(5).encoding, "tag_dict encoding");

            LongColumnVector rIds = (LongColumnVector) MicroPartitionCodec.readColumn(meta, 0);
            LongColumnVector rGrps = (LongColumnVector) MicroPartitionCodec.readColumn(meta, 1);
            DoubleColumnVector rPrices = (DoubleColumnVector) MicroPartitionCodec.readColumn(meta, 2);
            IntColumnVector rDates = (IntColumnVector) MicroPartitionCodec.readColumn(meta, 3);
            StringColumnVector rFlags = (StringColumnVector) MicroPartitionCodec.readColumn(meta, 4);
            StringColumnVector rTags = (StringColumnVector) MicroPartitionCodec.readColumn(meta, 5);

            for (int i = 0; i < n; i++) {
                assertEquals(ids[i], rIds.get(i), "ids[" + i + "]");
                assertEquals(grps[i], rGrps.get(i), "grps[" + i + "]");
                assertEquals(prices[i], rPrices.get(i), "prices[" + i + "]");
                assertEquals(dates[i], rDates.get(i), "dates[" + i + "]");
                assertEquals(flags[flagCodes[i]], rFlags.get(i), "flags[" + i + "]");
                assertEquals(tags[tagCodes[i]], rTags.get(i), "tags[" + i + "]");
            }
        } finally {
            deleteDir(dir);
        }
    }

    private static void testPartitionAndProjectionPruning() throws Exception {
        Path dir = Files.createTempDirectory("test-prune-");
        try {
            Database db = new Database(dir);
            Schema schema = new Schema(List.of(
                    new ColumnDef("k", ColumnType.INT64),
                    new ColumnDef("val", ColumnType.DOUBLE),
                    new ColumnDef("unused_payload", ColumnType.STRING)
            ));
            TableStorage t = db.createTable("events", schema);

            // 10 non-overlapping partitions: [0..99], [100..199], ..., [900..999]
            for (int p = 0; p < 10; p++) {
                int n = 100;
                long[] ks = new long[n];
                double[] vals = new double[n];
                int[] payloadCodes = new int[n];
                String[] dict = {"LONG_UNUSED_PAYLOAD_STRING_THAT_SHOULD_NOT_BE_READ_" + p};
                for (int i = 0; i < n; i++) {
                    ks[i] = p * 100L + i;
                    vals[i] = 1.5;
                    payloadCodes[i] = 0;
                }
                t.appendPartition(new ColumnVector[]{
                        new LongColumnVector(ks, n),
                        new DoubleColumnVector(vals, n),
                        new StringColumnVector(dict, payloadCodes, n)
                }, n);
            }

            String sql = "SELECT SUM(val) AS s FROM events WHERE k >= 350 AND k < 450";
            LogicalPlan plan = QueryPlanner.plan(SqlParser.parse(sql), db);
            assertEquals(2, plan.rootScan.survivingPartitions().size(), "only 2 of 10 partitions should survive");
            assertEquals(8, plan.rootScan.prunedPartitions(), "8 partitions pruned");
            assertEquals(2, plan.rootScan.projectedColumnIndices().length, "only k and val projected");

            try (QueryExecutor exec = new QueryExecutor(db, 4, false)) {
                QueryResult res = exec.execute(plan);
                assertEquals(1, res.rowCount(), "1 aggregate row");
                assertTrue(Math.abs(((Double) res.rows().get(0)[0]) - 150.0) < 1e-6, "sum = 100 * 1.5 = 150");
                assertTrue(res.stats().bytesRead() < t.totalBytes() * 0.35, "pruned bytes read < 35% of table");
            }
        } finally {
            deleteDir(dir);
        }
    }

    private static void testSqlParser() {
        SelectStatement stmt = SqlParser.parse(BenchmarkMain.TPCH_Q3);
        assertEquals(4, stmt.selectItems().size(), "Q3 select items");
        assertEquals(2, stmt.joins().size(), "Q3 joins");
        assertEquals(3, stmt.groupBy().size(), "Q3 group by");
        assertEquals(2, stmt.orderBy().size(), "Q3 order by");
        assertEquals(10, stmt.limit(), "Q3 limit");
    }

    private static void testShardedLruCacheConcurrency() throws Exception {
        ShardedLruCache cache = new ShardedLruCache(256 * 1024, 16); // tight 256 KB budget to force evictions
        ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int t = 0; t < 16; t++) {
                tasks.add(() -> {
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    for (int i = 0; i < 1500; i++) {
                        int part = rng.nextInt(40);
                        int col = rng.nextInt(4);
                        var key = new ShardedLruCache.CacheKey("lineitem", part, col);
                        ColumnVector v = cache.get(key);
                        if (v == null) {
                            long[] arr = new long[512];
                            arr[0] = part * 10L + col;
                            cache.put(key, new LongColumnVector(arr, arr.length));
                        } else {
                            long first = ((LongColumnVector) v).get(0);
                            if (first != part * 10L + col) {
                                throw new AssertionError("Corrupted cached vector!");
                            }
                        }
                    }
                    return null;
                });
            }
            for (Future<Void> f : pool.invokeAll(tasks)) {
                f.get();
            }
            assertTrue(cache.hits() > 0, "expected cache hits");
            assertTrue(cache.evictions() > 0, "expected bounded LRU evictions");
            assertTrue(cache.totalCachedBytes() <= 256 * 1024 + 65536, "cache stayed within byte budget");
        } finally {
            pool.shutdownNow();
        }
    }

    private static void testFiltersAndEdgeCases() throws Exception {
        Path dir = Files.createTempDirectory("test-edge-");
        try {
            Database db = new Database(dir);
            TpchDataGenerator.generate(db, null, null, 0.002, 512, 7L);

            try (QueryExecutor exec = new QueryExecutor(db, 4, true)) {
                // Empty result set via zone-map pruning
                QueryResult emptyAgg = exec.execute(
                        "SELECT COUNT(*) AS cnt, SUM(l_extendedprice) AS s FROM lineitem WHERE l_shipdate > '2099-01-01'");
                assertEquals(1, emptyAgg.rowCount(), "global agg returns 1 row");
                assertEquals(0L, emptyAgg.rows().get(0)[0], "count = 0");

                // BETWEEN + OR predicate
                QueryResult btw = exec.execute(
                        "SELECT l_returnflag, COUNT(*) AS c FROM lineitem "
                                + "WHERE l_quantity BETWEEN 10 AND 12 AND (l_returnflag = 'A' OR l_returnflag = 'R') "
                                + "GROUP BY l_returnflag ORDER BY l_returnflag ASC");
                assertTrue(btw.rowCount() >= 1 && btw.rowCount() <= 2, "only A and/or R groups");
            }
        } finally {
            deleteDir(dir);
        }
    }

    private static void testTwoWayJoinSwap() throws Exception {
        Path dir = Files.createTempDirectory("test-join-");
        try {
            Database db = new Database(dir.resolve("col"));
            RowBaselineExecutor baseline = new RowBaselineExecutor();
            TpchDataGenerator.generate(db, baseline, dir.resolve("row"), 0.005, 1024, 99L);

            // Put larger table FIRST in FROM to verify cost-based build/probe swap
            String sql = """
                    SELECT o.o_orderstatus, SUM(l.l_extendedprice) AS rev, COUNT(*) AS cnt
                    FROM lineitem l
                    INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
                    WHERE o.o_orderdate < '1995-01-01'
                    GROUP BY o.o_orderstatus
                    ORDER BY o.o_orderstatus ASC
                    """;

            LogicalPlan plan = QueryPlanner.plan(SqlParser.parse(sql), db);
            assertEquals("o", plan.rootScan.alias(), "smaller surviving table 'orders' swapped to build side");
            assertEquals("l", plan.joinSteps.get(0).probeScan().alias(), "larger 'lineitem' placed on parallel probe side");

            QueryResult ref = baseline.execute(sql);
            try (QueryExecutor exec = new QueryExecutor(db, 4, true)) {
                QueryResult actual = exec.execute(plan);
                BenchmarkMain.assertResultsEqual("2-way swapped join", ref, actual);
            }
        } finally {
            deleteDir(dir);
        }
    }

    private static void testTpchEquivalence() throws Exception {
        Path dir = Files.createTempDirectory("test-tpch-");
        try {
            Database db = new Database(dir.resolve("col"));
            RowBaselineExecutor baseline = new RowBaselineExecutor();
            TpchDataGenerator.generate(db, baseline, dir.resolve("row"), 0.01, 2048, 42L);

            List<String> queries = List.of(BenchmarkMain.TPCH_Q6, BenchmarkMain.TPCH_Q1, BenchmarkMain.TPCH_Q3);
            try (QueryExecutor exec1 = new QueryExecutor(db, 1, false);
                 QueryExecutor exec8 = new QueryExecutor(db, 8, true)) {
                for (int i = 0; i < queries.size(); i++) {
                    String sql = queries.get(i);
                    QueryResult ref = baseline.execute(sql);
                    QueryResult r1 = exec1.execute(sql);
                    QueryResult r8Cold = exec8.execute(sql);
                    QueryResult r8Warm = exec8.execute(sql);
                    BenchmarkMain.assertResultsEqual("Query " + i + " (1T)", ref, r1);
                    BenchmarkMain.assertResultsEqual("Query " + i + " (8T cold)", ref, r8Cold);
                    BenchmarkMain.assertResultsEqual("Query " + i + " (8T warm)", ref, r8Warm);
                }
            }
        } finally {
            deleteDir(dir);
        }
    }

    private static void testConcurrentQueries() throws Exception {
        Path dir = Files.createTempDirectory("test-conc-");
        try {
            Database db = new Database(dir.resolve("col"), 16 * 1024 * 1024);
            RowBaselineExecutor baseline = new RowBaselineExecutor();
            TpchDataGenerator.generate(db, baseline, dir.resolve("row"), 0.005, 1024, 123L);

            QueryResult refQ6 = baseline.execute(BenchmarkMain.TPCH_Q6);
            QueryResult refQ1 = baseline.execute(BenchmarkMain.TPCH_Q1);
            QueryResult refQ3 = baseline.execute(BenchmarkMain.TPCH_Q3);

            try (QueryExecutor exec = new QueryExecutor(db, 4, true)) {
                ExecutorService clients = Executors.newFixedThreadPool(6);
                try {
                    List<Callable<Void>> calls = new ArrayList<>();
                    for (int i = 0; i < 18; i++) {
                        int mod = i % 3;
                        calls.add(() -> {
                            if (mod == 0) BenchmarkMain.assertResultsEqual("conc Q6", refQ6, exec.execute(BenchmarkMain.TPCH_Q6));
                            else if (mod == 1) BenchmarkMain.assertResultsEqual("conc Q1", refQ1, exec.execute(BenchmarkMain.TPCH_Q1));
                            else BenchmarkMain.assertResultsEqual("conc Q3", refQ3, exec.execute(BenchmarkMain.TPCH_Q3));
                            return null;
                        });
                    }
                    for (Future<Void> f : clients.invokeAll(calls)) {
                        f.get();
                    }
                } finally {
                    clients.shutdownNow();
                }
            }
        } finally {
            deleteDir(dir);
        }
    }

    private static void assertEquals(Object expected, Object actual, String msg) {
        if (!java.util.Objects.equals(expected, actual)) {
            throw new AssertionError(msg + ": expected <" + expected + "> but got <" + actual + ">");
        }
    }

    private static void assertTrue(boolean cond, String msg) {
        if (!cond) {
            throw new AssertionError("Assertion failed: " + msg);
        }
    }

    private static void deleteDir(Path dir) {
        try (var s = Files.walk(dir)) {
            s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            });
        } catch (Exception ignored) {}
    }
}
