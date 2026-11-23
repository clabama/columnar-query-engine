package dev.wixforth.columnar.cli;

import dev.wixforth.columnar.benchmark.BenchmarkMain;
import dev.wixforth.columnar.benchmark.TpchDataGenerator;
import dev.wixforth.columnar.exec.QueryExecutor;
import dev.wixforth.columnar.exec.QueryResult;
import dev.wixforth.columnar.sql.LogicalPlan;
import dev.wixforth.columnar.sql.QueryPlanner;
import dev.wixforth.columnar.sql.SqlParser;
import dev.wixforth.columnar.storage.Database;
import dev.wixforth.columnar.storage.TableStorage;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Interactive SQL shell backed by a sample TPC-H micro-partitioned columnar database.
 * Supports arbitrary SQL queries as well as "EXPLAIN SELECT ..." inspection.
 */
public final class SqlShellMain {
    public static void main(String[] args) throws Exception {
        java.util.Locale.setDefault(java.util.Locale.ROOT);
        double sf = args.length > 0 ? Double.parseDouble(args[0]) : 0.1;
        Path workDir = Files.createTempDirectory("columnar-shell-");
        Database db = new Database(workDir);
        var gen = TpchDataGenerator.generate(db, null, null, sf, TableStorage.DEFAULT_PARTITION_ROWS, 42L);
        System.out.printf("Loaded TPC-H SF %.2f (%,d customers, %,d orders, %,d lineitems) in %,d ms.%n",
                sf, gen.customerRows(), gen.orderRows(), gen.lineitemRows(), gen.genMillis());
        System.out.println("Type SQL queries (end with ';'), prefix with EXPLAIN, or type 'demo' / 'exit'.");

        try (QueryExecutor exec = new QueryExecutor(db);
             BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            StringBuilder buf = new StringBuilder();
            while (true) {
                System.out.print(buf.isEmpty() ? "sql> " : "  -> ");
                String line = reader.readLine();
                if (line == null) break;
                String trimmed = line.trim();
                if (buf.isEmpty() && (trimmed.equalsIgnoreCase("exit") || trimmed.equalsIgnoreCase("quit"))) {
                    break;
                }
                if (buf.isEmpty() && trimmed.equalsIgnoreCase("demo")) {
                    runQuery(exec, db, BenchmarkMain.TPCH_Q6, false);
                    runQuery(exec, db, BenchmarkMain.TPCH_Q1, false);
                    runQuery(exec, db, BenchmarkMain.TPCH_Q3, false);
                    continue;
                }
                buf.append(line).append('\n');
                if (trimmed.endsWith(";")) {
                    String sql = buf.toString().trim();
                    buf.setLength(0);
                    boolean explain = sql.toUpperCase().startsWith("EXPLAIN ");
                    if (explain) sql = sql.substring(8).trim();
                    try {
                        runQuery(exec, db, sql, explain);
                    } catch (Exception e) {
                        System.out.println("Error: " + e.getMessage());
                    }
                }
            }
        }
    }

    private static void runQuery(QueryExecutor exec, Database db, String sql, boolean explainOnly) {
        LogicalPlan plan = QueryPlanner.plan(SqlParser.parse(sql), db);
        if (explainOnly) {
            System.out.println(plan.explain());
            return;
        }
        QueryResult res = exec.execute(plan);
        System.out.print(res.formatTable());
        System.out.printf("(%d rows in %.2f ms | pruned %d/%d partitions (%.1f%%) | read %.2f MB)%n%n",
                res.rowCount(),
                res.stats().elapsedMillis(),
                res.stats().partitionsPruned(),
                res.stats().partitionsTotal(),
                res.stats().partitionPrunedPercent(),
                res.stats().bytesRead() / 1e6);
    }
}
