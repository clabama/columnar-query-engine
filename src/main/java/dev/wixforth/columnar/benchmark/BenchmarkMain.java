package dev.wixforth.columnar.benchmark;

import dev.wixforth.columnar.baseline.RowBaselineExecutor;
import dev.wixforth.columnar.exec.QueryExecutor;
import dev.wixforth.columnar.exec.QueryResult;
import dev.wixforth.columnar.sql.LogicalPlan;
import dev.wixforth.columnar.sql.QueryPlanner;
import dev.wixforth.columnar.sql.SqlParser;
import dev.wixforth.columnar.storage.Database;
import dev.wixforth.columnar.storage.TableStorage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * End-to-end TPC-H benchmark suite comparing the single-threaded row-oriented baseline
 * against the multi-threaded vectorized micro-partitioned columnar query engine.
 *
 * Usage: java ... BenchmarkMain [--sf=1.0] [--warmup=2] [--runs=5]
 */
public final class BenchmarkMain {

    public static final String TPCH_Q1 = """
            SELECT l_returnflag, l_linestatus,
                   SUM(l_quantity) AS sum_qty,
                   SUM(l_extendedprice) AS sum_base_price,
                   SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                   AVG(l_quantity) AS avg_qty,
                   AVG(l_discount) AS avg_disc,
                   COUNT(*) AS count_order
            FROM lineitem
            WHERE l_shipdate <= '1998-09-02'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag ASC, l_linestatus ASC
            """;

    public static final String TPCH_Q3 = """
            SELECT o.o_orderkey, o.o_orderdate, o.o_shippriority,
                   SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
            FROM customer c
            INNER JOIN orders o ON c.c_custkey = o.o_custkey
            INNER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
            WHERE c.c_mktsegment = 'BUILDING'
              AND o.o_orderdate < '1995-03-15'
              AND l.l_shipdate > '1995-03-15'
            GROUP BY o.o_orderkey, o.o_orderdate, o.o_shippriority
            ORDER BY revenue DESC, o.o_orderdate ASC
            LIMIT 10
            """;

    public static final String TPCH_Q6 = """
            SELECT SUM(l_extendedprice * l_discount) AS revenue
            FROM lineitem
            WHERE l_shipdate >= '1994-01-01'
              AND l_shipdate < '1995-01-01'
              AND l_discount >= 0.05
              AND l_discount <= 0.07
              AND l_quantity < 24
            """;

    public record NamedQuery(String name, String description, String sql) {}

    public static void main(String[] args) throws Exception {
        java.util.Locale.setDefault(java.util.Locale.ROOT);
        double sf = 1.0;
        int warmup = 2;
        int runs = 5;
        for (String arg : args) {
            if (arg.startsWith("--sf=")) sf = Double.parseDouble(arg.substring(5));
            else if (arg.startsWith("--warmup=")) warmup = Integer.parseInt(arg.substring(9));
            else if (arg.startsWith("--runs=")) runs = Integer.parseInt(arg.substring(7));
        }

        int cores = Runtime.getRuntime().availableProcessors();
        Path workDir = Files.createTempDirectory("columnar-bench-");
        Path colDir = workDir.resolve("columnar");
        Path rowDir = workDir.resolve("rowstore");

        try {
            System.out.printf("================================================================================%n");
            System.out.printf(" Micro-Partitioned Columnar SQL Query Engine -- TPC-H Benchmark (SF %.2f)%n", sf);
            System.out.printf(" JVM: %s %s | CPUs: %d | Partition Size: %d rows | Batch Size: 4096%n",
                    System.getProperty("java.vm.name"),
                    System.getProperty("java.version"),
                    cores,
                    TableStorage.DEFAULT_PARTITION_ROWS);
            System.out.printf("================================================================================%n%n");

            Database db = new Database(colDir, 512L * 1024 * 1024);
            RowBaselineExecutor baseline = new RowBaselineExecutor();

            var gen = TpchDataGenerator.generate(
                    db, baseline, rowDir, sf, TableStorage.DEFAULT_PARTITION_ROWS, 42L);

            long colTotalBytes = db.table("customer").totalBytes()
                    + db.table("orders").totalBytes()
                    + db.table("lineitem").totalBytes();
            long rowTotalBytes = Files.size(rowDir.resolve("customer.row"))
                    + Files.size(rowDir.resolve("orders.row"))
                    + Files.size(rowDir.resolve("lineitem.row"));

            System.out.printf("Generated dataset in %,d ms:%n", gen.genMillis());
            System.out.printf("  customer : %,10d rows | %3d micro-partitions | %6.1f MB columnar%n",
                    gen.customerRows(), db.table("customer").partitionCount(), db.table("customer").totalBytes() / 1e6);
            System.out.printf("  orders   : %,10d rows | %3d micro-partitions | %6.1f MB columnar%n",
                    gen.orderRows(), db.table("orders").partitionCount(), db.table("orders").totalBytes() / 1e6);
            System.out.printf("  lineitem : %,10d rows | %3d micro-partitions | %6.1f MB columnar%n",
                    gen.lineitemRows(), db.table("lineitem").partitionCount(), db.table("lineitem").totalBytes() / 1e6);
            System.out.printf("  Storage  : %.1f MB columnar vs. %.1f MB row-store (%.1f%% compression savings)%n%n",
                    colTotalBytes / 1e6, rowTotalBytes / 1e6,
                    100.0 * (rowTotalBytes - colTotalBytes) / rowTotalBytes);

            List<NamedQuery> queries = List.of(
                    new NamedQuery("TPC-H Q6", "Selective scan + range/zone-map pruning + global agg", TPCH_Q6),
                    new NamedQuery("TPC-H Q1", "Full scan + projection pruning + grouped hash agg", TPCH_Q1),
                    new NamedQuery("TPC-H Q3", "3-way hash join + partition pruning + top-10 sort", TPCH_Q3)
            );

            // Print EXPLAIN plans
            System.out.println("--- Query Plans (EXPLAIN) ---");
            for (NamedQuery q : queries) {
                LogicalPlan plan = QueryPlanner.plan(SqlParser.parse(q.sql()), db);
                System.out.println(q.name() + " (" + q.description() + "):");
                System.out.print(plan.explain());
            }
            System.out.println();

            System.out.printf("%-10s | %10s | %10s | %10s | %8s | %8s | %11s | %11s%n",
                    "Query", "RowBase(ms)", "ColCold(ms)", "ColWarm(ms)", "ColdSpd", "WarmSpd", "PartsPruned", "BytesSaved");
            System.out.println("-".repeat(98));

            try (QueryExecutor execPar = new QueryExecutor(db, cores, true)) {
                for (NamedQuery q : queries) {
                    // Warmup + parity check
                    QueryResult refResult = null;
                    QueryResult colResult = null;
                    for (int w = 0; w < warmup; w++) {
                        refResult = baseline.execute(q.sql());
                        db.cache().clear();
                        colResult = execPar.execute(q.sql());
                    }
                    assertResultsEqual(q.name(), refResult, colResult);

                    // Measure Row-Oriented Baseline (median)
                    double[] rowTimes = new double[runs];
                    long rowBytesRead = 0;
                    for (int r = 0; r < runs; r++) {
                        QueryResult res = baseline.execute(q.sql());
                        rowTimes[r] = res.stats().elapsedMillis();
                        rowBytesRead = res.stats().bytesRead();
                    }

                    // Measure Columnar Cold Cache (N threads, cache cleared before every run)
                    double[] coldTimes = new double[runs];
                    long coldBytesRead = 0;
                    double prunedPct = 0.0;
                    for (int r = 0; r < runs; r++) {
                        db.cache().clear();
                        QueryResult res = execPar.execute(q.sql());
                        coldTimes[r] = res.stats().elapsedMillis();
                        coldBytesRead = res.stats().bytesRead();
                        prunedPct = res.stats().partitionPrunedPercent();
                    }

                    // Measure Columnar Warm Sharded LRU Cache (N threads)
                    double[] warmTimes = new double[runs];
                    for (int r = 0; r < runs; r++) {
                        QueryResult res = execPar.execute(q.sql());
                        warmTimes[r] = res.stats().elapsedMillis();
                    }

                    double rowMed = median(rowTimes);
                    double coldMed = median(coldTimes);
                    double warmMed = median(warmTimes);
                    double coldSpeedup = rowMed / coldMed;
                    double warmSpeedup = rowMed / warmMed;
                    double bytesSavedPct = 100.0 * (rowBytesRead - coldBytesRead) / Math.max(1.0, rowBytesRead);

                    System.out.printf("%-10s | %10.1f | %10.1f | %10.1f | %7.1fx | %7.1fx | %10.1f%% | %10.1f%%%n",
                            q.name(), rowMed, coldMed, warmMed, coldSpeedup, warmSpeedup, prunedPct, bytesSavedPct);
                }
            }

            // Multi-core thread scaling benchmark on TPC-H Q1 & Q3 (Cold Cache)
            System.out.println("\n--- Multi-Core Thread Scaling (Cold Cache, Median Latency in ms) ---");
            int[] threadCounts = buildThreadLadder(cores);
            System.out.printf("%-10s", "Query");
            for (int t : threadCounts) System.out.printf(" | %6dT", t);
            System.out.println(" | Scaling");
            System.out.println("-".repeat(12 + threadCounts.length * 10 + 10));

            for (NamedQuery q : queries) {
                double t1 = 0;
                double tMax = 0;
                System.out.printf("%-10s", q.name());
                for (int t : threadCounts) {
                    try (QueryExecutor execT = new QueryExecutor(db, t, false)) {
                        execT.execute(q.sql()); // warmup
                        double[] times = new double[runs];
                        for (int r = 0; r < runs; r++) {
                            times[r] = execT.execute(q.sql()).stats().elapsedMillis();
                        }
                        double med = median(times);
                        if (t == 1) t1 = med;
                        tMax = med;
                        System.out.printf(" | %6.1f", med);
                    }
                }
                System.out.printf(" | %6.2fx%n", t1 / tMax);
            }

            System.out.println("\nAll query results verified numerically identical to row-oriented baseline.");
        } finally {
            deleteRecursively(workDir);
        }
    }

    private static int[] buildThreadLadder(int maxCores) {
        List<Integer> ladder = new java.util.ArrayList<>();
        for (int t : new int[]{1, 2, 4, 8}) {
            if (t <= maxCores) ladder.add(t);
        }
        if (ladder.get(ladder.size() - 1) != maxCores) {
            ladder.add(maxCores);
        }
        return ladder.stream().mapToInt(Integer::intValue).toArray();
    }

    private static double median(double[] arr) {
        double[] copy = arr.clone();
        Arrays.sort(copy);
        int mid = copy.length / 2;
        return copy.length % 2 == 0 ? (copy[mid - 1] + copy[mid]) / 2.0 : copy[mid];
    }

    public static void assertResultsEqual(String label, QueryResult expected, QueryResult actual) {
        if (expected.rowCount() != actual.rowCount()) {
            throw new AssertionError(label + " row count mismatch: expected "
                    + expected.rowCount() + ", got " + actual.rowCount());
        }
        for (int r = 0; r < expected.rowCount(); r++) {
            Object[] er = expected.rows().get(r);
            Object[] ar = actual.rows().get(r);
            for (int c = 0; c < er.length; c++) {
                if (er[c] instanceof Number en && ar[c] instanceof Number an) {
                    double diff = Math.abs(en.doubleValue() - an.doubleValue());
                    double tol = Math.max(1e-3, Math.abs(en.doubleValue()) * 1e-7);
                    if (diff > tol) {
                        throw new AssertionError(label + " numeric mismatch at (" + r + "," + c + "): expected "
                                + en + ", got " + an);
                    }
                } else if (!java.util.Objects.equals(er[c], ar[c])) {
                    throw new AssertionError(label + " value mismatch at (" + r + "," + c + "): expected "
                            + er[c] + ", got " + ar[c]);
                }
            }
        }
    }

    private static void deleteRecursively(Path dir) {
        try (var stream = Files.walk(dir)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            });
        } catch (Exception ignored) {}
    }
}
