package dev.wixforth.columnar.exec;

import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.DateUtil;
import dev.wixforth.columnar.sql.Expr;
import dev.wixforth.columnar.sql.LogicalPlan;
import dev.wixforth.columnar.sql.QueryPlanner;
import dev.wixforth.columnar.sql.SelectStatement;
import dev.wixforth.columnar.storage.Database;
import dev.wixforth.columnar.storage.PartitionMetadata;
import dev.wixforth.columnar.storage.ShardedLruCache;
import dev.wixforth.columnar.storage.TableStorage;
import dev.wixforth.columnar.vector.ColumnVector;
import dev.wixforth.columnar.vector.DoubleColumnVector;
import dev.wixforth.columnar.vector.IntColumnVector;
import dev.wixforth.columnar.vector.LongColumnVector;
import dev.wixforth.columnar.vector.StringColumnVector;
import dev.wixforth.columnar.vector.VectorBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Parallel vectorized query executor using dynamic morsel-driven partition scheduling
 * over a fixed worker thread pool, lock-free hash-join probing, and contention-free
 * thread-local hash aggregation with associative parallel merge.
 */
public final class QueryExecutor implements AutoCloseable {
    private final Database db;
    private final int numThreads;
    private final ExecutorService pool;
    private final boolean useCache;

    public QueryExecutor(Database db, int numThreads, boolean useCache) {
        this.db = db;
        this.numThreads = Math.max(1, numThreads);
        this.useCache = useCache;
        this.pool = Executors.newFixedThreadPool(this.numThreads, r -> {
            Thread t = new Thread(r, "columnar-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public QueryExecutor(Database db) {
        this(db, Runtime.getRuntime().availableProcessors(), true);
    }

    public int numThreads() {
        return numThreads;
    }

    public QueryResult execute(String sql) {
        SelectStatement stmt = dev.wixforth.columnar.sql.SqlParser.parse(sql);
        LogicalPlan plan = QueryPlanner.plan(stmt, db);
        return execute(plan);
    }

    public QueryResult execute(LogicalPlan plan) {
        long startNanos = System.nanoTime();
        AtomicLong bytesRead = new AtomicLong();
        AtomicLong cacheHits = new AtomicLong();
        AtomicLong cacheMisses = new AtomicLong();

        int totalParts = 0;
        int prunedParts = 0;
        int scannedParts = 0;
        long totalTableBytes = 0;
        for (LogicalPlan.TableScanPlan scan : plan.aliasToScan.values()) {
            totalParts += scan.totalPartitions();
            prunedParts += scan.prunedPartitions();
            scannedParts += scan.survivingPartitions().size();
            totalTableBytes += scan.table().totalBytes();
        }

        // Pre-resolve output schema and aggregate specs
        List<String> outNames = new ArrayList<>();
        List<ColumnType> outTypes = new ArrayList<>();
        List<AggSpec> aggSpecs = new ArrayList<>();
        List<SelectBinding> selectBindings = new ArrayList<>();

        for (int i = 0; i < plan.selectItems.size(); i++) {
            SelectStatement.SelectItem item = plan.selectItems.get(i);
            Expr expr = item.expr();
            if (expr instanceof Expr.AggregateCall agg) {
                int aggIdx = aggSpecs.size();
                aggSpecs.add(new AggSpec(agg.func(), agg.argument()));
                String name = item.alias() != null ? item.alias() : agg.func().name().toLowerCase(Locale.ROOT) + "_" + i;
                ColumnType type = agg.func() == Expr.AggFunc.COUNT ? ColumnType.INT64 : ColumnType.DOUBLE;
                outNames.add(name);
                outTypes.add(type);
                selectBindings.add(new SelectBinding(true, aggIdx, -1, null, expr));
            } else if (expr instanceof Expr.ColumnRef ref) {
                LogicalPlan.ResolvedColumn rc = QueryPlanner.resolveColumn(ref, plan.aliasToScan);
                int gbIdx = findGroupByIndex(plan.groupByColumns, rc);
                String name = item.alias() != null ? item.alias() : rc.columnName();
                outNames.add(name);
                outTypes.add(rc.type());
                selectBindings.add(new SelectBinding(false, -1, gbIdx, rc, expr));
            } else {
                String name = item.alias() != null ? item.alias() : "expr_" + i;
                outNames.add(name);
                outTypes.add(ColumnType.DOUBLE);
                selectBindings.add(new SelectBinding(false, -1, -1, null, expr));
            }
        }

        List<Object[]> resultRows;
        try {
            if (plan.joinSteps.isEmpty()) {
                resultRows = executeSingleTable(
                        plan, selectBindings, aggSpecs, bytesRead, cacheHits, cacheMisses);
            } else {
                resultRows = executeJoined(
                        plan, selectBindings, aggSpecs, bytesRead, cacheHits, cacheMisses);
            }
        } catch (Exception e) {
            throw new RuntimeException("Query execution failed", e);
        }

        // Apply ORDER BY
        if (!plan.orderBy.isEmpty() && resultRows.size() > 1) {
            int[] sortCols = new int[plan.orderBy.size()];
            boolean[] sortDesc = new boolean[plan.orderBy.size()];
            for (int i = 0; i < plan.orderBy.size(); i++) {
                sortCols[i] = resolveOrderByIndex(plan.orderBy.get(i).columnOrAlias(), outNames, plan.selectItems);
                sortDesc[i] = plan.orderBy.get(i).descending();
            }
            resultRows.sort(buildRowComparator(sortCols, sortDesc));
        }

        // Apply LIMIT
        if (plan.limit != null && resultRows.size() > plan.limit) {
            resultRows = new ArrayList<>(resultRows.subList(0, plan.limit));
        }

        long elapsed = System.nanoTime() - startNanos;
        ExecutionStats stats = new ExecutionStats(
                elapsed,
                totalParts,
                prunedParts,
                scannedParts,
                bytesRead.get(),
                totalTableBytes,
                cacheHits.get(),
                cacheMisses.get(),
                numThreads);

        return new QueryResult(outNames, outTypes, resultRows, stats);
    }

    // ==================== SINGLE-TABLE MORSEL-PARALLEL EXECUTION ====================

    private List<Object[]> executeSingleTable(
            LogicalPlan plan,
            List<SelectBinding> selectBindings,
            List<AggSpec> aggSpecs,
            AtomicLong bytesRead,
            AtomicLong cacheHits,
            AtomicLong cacheMisses) throws InterruptedException, ExecutionException {
        LogicalPlan.TableScanPlan scan = plan.rootScan;
        List<PartitionMetadata> parts = scan.survivingPartitions();
        if (parts.isEmpty()) {
            if (plan.isAggregation && plan.groupByColumns.isEmpty()) {
                List<Object[]> emptyOut = new ArrayList<>(1);
                emptyOut.add(finalizeGlobalEmpty(selectBindings, aggSpecs));
                return emptyOut;
            }
            return new ArrayList<>();
        }

        int workers = Math.min(numThreads, parts.size());
        AtomicInteger nextMorsel = new AtomicInteger(0);
        Expr.AggFunc[] aggFuncs = aggSpecs.stream().map(AggSpec::func).toArray(Expr.AggFunc[]::new);
        int[] gbProjIndices = plan.groupByColumns.stream().mapToInt(LogicalPlan.ResolvedColumn::projectedIndex).toArray();

        if (plan.isAggregation && plan.groupByColumns.isEmpty()) {
            // Fast-path: global aggregation without GROUP BY (e.g., TPC-H Q6)
            List<Callable<AggregateState>> tasks = new ArrayList<>(workers);
            for (int w = 0; w < workers; w++) {
                tasks.add(() -> {
                    AggregateState local = new AggregateState(aggSpecs.size());
                    int idx;
                    while ((idx = nextMorsel.getAndIncrement()) < parts.size()) {
                        ColumnVector[] cols = loadProjectedColumns(scan, parts.get(idx), bytesRead, cacheHits, cacheMisses);
                        int partRows = parts.get(idx).rowCount;
                        for (int off = 0; off < partRows; off += VectorBatch.DEFAULT_BATCH_SIZE) {
                            int len = Math.min(VectorBatch.DEFAULT_BATCH_SIZE, partRows - off);
                            VectorBatch batch = sliceBatch(cols, off, len);
                            VectorExprEvaluator.filterBatch(batch, scan.pushedPredicates(), scan.colNameToProjectedIndex());
                            if (batch.isEmpty()) continue;

                            int n = batch.selectedCount;
                            for (int a = 0; a < aggSpecs.size(); a++) {
                                AggSpec spec = aggSpecs.get(a);
                                if (spec.func == Expr.AggFunc.COUNT) {
                                    local.counts[a] += n;
                                } else {
                                    double[] vals = VectorExprEvaluator.evalNumericBatch(
                                            batch, spec.arg, scan.colNameToProjectedIndex());
                                    switch (spec.func) {
                                        case SUM -> {
                                            double s = 0.0;
                                            for (int i = 0; i < n; i++) s += vals[i];
                                            local.sums[a] += s;
                                        }
                                        case AVG -> {
                                            double s = 0.0;
                                            for (int i = 0; i < n; i++) s += vals[i];
                                            local.sums[a] += s;
                                            local.counts[a] += n;
                                        }
                                        case MIN -> {
                                            double m = local.mins[a];
                                            for (int i = 0; i < n; i++) if (vals[i] < m) m = vals[i];
                                            local.mins[a] = m;
                                        }
                                        case MAX -> {
                                            double m = local.maxs[a];
                                            for (int i = 0; i < n; i++) if (vals[i] > m) m = vals[i];
                                            local.maxs[a] = m;
                                        }
                                        default -> {}
                                    }
                                }
                            }
                        }
                    }
                    return local;
                });
            }

            AggregateState merged = new AggregateState(aggSpecs.size());
            for (Future<AggregateState> f : pool.invokeAll(tasks)) {
                merged.mergeFrom(f.get(), aggFuncs);
            }
            Object[] row = new Object[selectBindings.size()];
            for (int i = 0; i < selectBindings.size(); i++) {
                SelectBinding b = selectBindings.get(i);
                row[i] = merged.finalizeValue(b.aggIdx, aggFuncs[b.aggIdx]);
            }
            List<Object[]> out = new ArrayList<>(1);
            out.add(row);
            return out;
        }

        if (plan.isAggregation) {
            // Fast-path: parallel thread-local HashAggregate (e.g., TPC-H Q1)
            List<Callable<Map<AggregateState.GroupKey, AggregateState>>> tasks = new ArrayList<>(workers);
            for (int w = 0; w < workers; w++) {
                tasks.add(() -> {
                    Map<AggregateState.GroupKey, AggregateState> localMap = new HashMap<>(64);
                    int idx;
                    while ((idx = nextMorsel.getAndIncrement()) < parts.size()) {
                        ColumnVector[] cols = loadProjectedColumns(scan, parts.get(idx), bytesRead, cacheHits, cacheMisses);
                        int partRows = parts.get(idx).rowCount;
                        for (int off = 0; off < partRows; off += VectorBatch.DEFAULT_BATCH_SIZE) {
                            int len = Math.min(VectorBatch.DEFAULT_BATCH_SIZE, partRows - off);
                            VectorBatch batch = sliceBatch(cols, off, len);
                            VectorExprEvaluator.filterBatch(batch, scan.pushedPredicates(), scan.colNameToProjectedIndex());
                            if (batch.isEmpty()) continue;

                            int n = batch.selectedCount;
                            // Evaluate numeric aggregate expressions in tight vectorized loops once per batch
                            double[][] evaluatedAggs = new double[aggSpecs.size()][];
                            for (int a = 0; a < aggSpecs.size(); a++) {
                                if (aggSpecs.get(a).func != Expr.AggFunc.COUNT) {
                                    evaluatedAggs[a] = VectorExprEvaluator.evalNumericBatch(
                                            batch, aggSpecs.get(a).arg, scan.colNameToProjectedIndex());
                                }
                            }

                            // Special fast-path when all GROUP BY columns are dictionary-encoded STRINGs (e.g., TPC-H Q1)
                            if (allStringGroupBy(batch.columns, gbProjIndices) && gbProjIndices.length == 2) {
                                StringColumnVector g0 = (StringColumnVector) batch.columns[gbProjIndices[0]];
                                StringColumnVector g1 = (StringColumnVector) batch.columns[gbProjIndices[1]];
                                int d0 = g0.dictionary.length;
                                int d1 = g1.dictionary.length;
                                if ((long) d0 * d1 <= 4096) {
                                    AggregateState[] directTable = new AggregateState[d0 * d1];
                                    boolean hasSel = batch.hasSelection;
                                    int[] sel = batch.selection;
                                    for (int i = 0; i < n; i++) {
                                        int r = hasSel ? sel[i] : i;
                                        int slot = g0.getCode(r) * d1 + g1.getCode(r);
                                        AggregateState st = directTable[slot];
                                        if (st == null) {
                                            st = new AggregateState(aggSpecs.size());
                                            directTable[slot] = st;
                                        }
                                        for (int a = 0; a < aggSpecs.size(); a++) {
                                            st.accumulate(a, aggFuncs[a], evaluatedAggs[a] == null ? 1.0 : evaluatedAggs[a][i]);
                                        }
                                    }
                                    for (int c0 = 0; c0 < d0; c0++) {
                                        for (int c1 = 0; c1 < d1; c1++) {
                                            AggregateState st = directTable[c0 * d1 + c1];
                                            if (st != null) {
                                                AggregateState.GroupKey gk = new AggregateState.GroupKey(
                                                        new Object[]{g0.dictionary[c0], g1.dictionary[c1]});
                                                AggregateState existing = localMap.get(gk);
                                                if (existing == null) {
                                                    localMap.put(gk, st);
                                                } else {
                                                    existing.mergeFrom(st, aggFuncs);
                                                }
                                            }
                                        }
                                    }
                                    continue;
                                }
                            }

                            boolean hasSel = batch.hasSelection;
                            int[] sel = batch.selection;
                            for (int i = 0; i < n; i++) {
                                int r = hasSel ? sel[i] : i;
                                Object[] keyVals = new Object[gbProjIndices.length];
                                for (int g = 0; g < gbProjIndices.length; g++) {
                                    keyVals[g] = batch.columns[gbProjIndices[g]].getBoxed(r);
                                }
                                AggregateState.GroupKey gk = new AggregateState.GroupKey(keyVals);
                                AggregateState st = localMap.get(gk);
                                if (st == null) {
                                    st = new AggregateState(aggSpecs.size());
                                    localMap.put(gk, st);
                                }
                                for (int a = 0; a < aggSpecs.size(); a++) {
                                    st.accumulate(a, aggFuncs[a], evaluatedAggs[a] == null ? 1.0 : evaluatedAggs[a][i]);
                                }
                            }
                        }
                    }
                    return localMap;
                });
            }

            Map<AggregateState.GroupKey, AggregateState> mergedMap = new HashMap<>();
            for (Future<Map<AggregateState.GroupKey, AggregateState>> f : pool.invokeAll(tasks)) {
                for (var e : f.get().entrySet()) {
                    AggregateState existing = mergedMap.get(e.getKey());
                    if (existing == null) {
                        mergedMap.put(e.getKey(), e.getValue());
                    } else {
                        existing.mergeFrom(e.getValue(), aggFuncs);
                    }
                }
            }
            return materializeGroupedResult(mergedMap, selectBindings, aggFuncs);
        }

        // Non-aggregated SELECT projection
        List<Callable<List<Object[]>>> tasks = new ArrayList<>(workers);
        for (int w = 0; w < workers; w++) {
            tasks.add(() -> {
                List<Object[]> localRows = new ArrayList<>();
                int idx;
                while ((idx = nextMorsel.getAndIncrement()) < parts.size()) {
                    ColumnVector[] cols = loadProjectedColumns(scan, parts.get(idx), bytesRead, cacheHits, cacheMisses);
                    int partRows = parts.get(idx).rowCount;
                    for (int off = 0; off < partRows; off += VectorBatch.DEFAULT_BATCH_SIZE) {
                        int len = Math.min(VectorBatch.DEFAULT_BATCH_SIZE, partRows - off);
                        VectorBatch batch = sliceBatch(cols, off, len);
                        VectorExprEvaluator.filterBatch(batch, scan.pushedPredicates(), scan.colNameToProjectedIndex());
                        if (batch.isEmpty()) continue;

                        int n = batch.selectedCount;
                        double[][] computed = new double[selectBindings.size()][];
                        for (int b = 0; b < selectBindings.size(); b++) {
                            SelectBinding sb = selectBindings.get(b);
                            if (sb.resolvedCol == null) {
                                computed[b] = VectorExprEvaluator.evalNumericBatch(
                                        batch, sb.expr, scan.colNameToProjectedIndex());
                            }
                        }
                        boolean hasSel = batch.hasSelection;
                        int[] sel = batch.selection;
                        for (int i = 0; i < n; i++) {
                            int r = hasSel ? sel[i] : i;
                            Object[] row = new Object[selectBindings.size()];
                            for (int b = 0; b < selectBindings.size(); b++) {
                                SelectBinding sb = selectBindings.get(b);
                                if (sb.resolvedCol != null) {
                                    row[b] = batch.columns[sb.resolvedCol.projectedIndex()].getBoxed(r);
                                } else {
                                    row[b] = computed[b][i];
                                }
                            }
                            localRows.add(row);
                        }
                    }
                }
                return localRows;
            });
        }
        List<Object[]> allRows = new ArrayList<>();
        for (Future<List<Object[]>> f : pool.invokeAll(tasks)) {
            allRows.addAll(f.get());
        }
        return allRows;
    }

    // ==================== MULTI-TABLE LEFT-DEEP HASH JOIN EXECUTION ====================

    private List<Object[]> executeJoined(
            LogicalPlan plan,
            List<SelectBinding> selectBindings,
            List<AggSpec> aggSpecs,
            AtomicLong bytesRead,
            AtomicLong cacheHits,
            AtomicLong cacheMisses) throws IOException, InterruptedException, ExecutionException {

        // Step 1: Materialize rootScan into initial BuildHashTable
        BuildHashTable currentBuild = materializeScanToBuildTable(plan.rootScan, bytesRead, cacheHits, cacheMisses);

        // Step 2: Execute intermediate join steps (if 3+ tables, e.g., customer JOIN orders JOIN lineitem)
        for (int s = 0; s < plan.joinSteps.size() - 1; s++) {
            LogicalPlan.JoinStep step = plan.joinSteps.get(s);
            currentBuild.buildIndex(step.buildKey());
            currentBuild = probeAndMaterializeNextBuild(currentBuild, step, bytesRead, cacheHits, cacheMisses);
        }

        // Step 3: Index final build hash table and run parallel morsel probe over the final probe table
        LogicalPlan.JoinStep finalStep = plan.joinSteps.get(plan.joinSteps.size() - 1);
        if (currentBuild.rowCount() == 0 || finalStep.probeScan().survivingPartitions().isEmpty()) {
            if (plan.isAggregation && plan.groupByColumns.isEmpty()) {
                List<Object[]> emptyOut = new ArrayList<>(1);
                emptyOut.add(finalizeGlobalEmpty(selectBindings, aggSpecs));
                return emptyOut;
            }
            return new ArrayList<>();
        }
        currentBuild.buildIndex(finalStep.buildKey());

        final BuildHashTable buildTable = currentBuild;
        final LogicalPlan.TableScanPlan probeScan = finalStep.probeScan();
        final String probeAlias = probeScan.alias();
        final int probeKeyProjIdx = finalStep.probeKey().projectedIndex();
        final ColumnType probeKeyType = finalStep.probeKey().type();
        final List<PartitionMetadata> probeParts = probeScan.survivingPartitions();
        final int workers = Math.min(numThreads, probeParts.size());
        final AtomicInteger nextMorsel = new AtomicInteger(0);
        final Expr.AggFunc[] aggFuncs = aggSpecs.stream().map(AggSpec::func).toArray(Expr.AggFunc[]::new);

        // Pre-check whether each aggregate argument references ONLY the probe table (enables batch vector eval!)
        final boolean[] aggIsProbeOnly = new boolean[aggSpecs.size()];
        for (int a = 0; a < aggSpecs.size(); a++) {
            aggIsProbeOnly[a] = aggSpecs.get(a).func != Expr.AggFunc.COUNT
                    && exprOnlyUsesAlias(aggSpecs.get(a).arg, probeAlias, plan.aliasToScan);
        }

        // Pre-resolve GROUP BY slot locations (either build slot or probe projected index)
        final boolean[] gbFromBuild = new boolean[plan.groupByColumns.size()];
        final int[] gbSlotOrProj = new int[plan.groupByColumns.size()];
        for (int g = 0; g < plan.groupByColumns.size(); g++) {
            LogicalPlan.ResolvedColumn rc = plan.groupByColumns.get(g);
            if (rc.tableAlias().equals(probeAlias)) {
                gbFromBuild[g] = false;
                gbSlotOrProj[g] = rc.projectedIndex();
            } else {
                gbFromBuild[g] = true;
                gbSlotOrProj[g] = buildTable.slotOf(rc);
            }
        }

        // Pre-resolve build slots for quick scalar lookup
        final Map<String, Integer> buildSlots = new HashMap<>();
        for (int i = 0; i < buildTable.schemaCols.size(); i++) {
            LogicalPlan.ResolvedColumn rc = buildTable.schemaCols.get(i);
            buildSlots.put(rc.tableAlias() + "." + rc.columnName(), i);
        }

        if (plan.isAggregation && plan.groupByColumns.isEmpty()) {
            List<Callable<AggregateState>> tasks = new ArrayList<>(workers);
            for (int w = 0; w < workers; w++) {
                tasks.add(() -> {
                    AggregateState local = new AggregateState(aggSpecs.size());
                    int idx;
                    while ((idx = nextMorsel.getAndIncrement()) < probeParts.size()) {
                        ColumnVector[] cols = loadProjectedColumns(probeScan, probeParts.get(idx), bytesRead, cacheHits, cacheMisses);
                        int partRows = probeParts.get(idx).rowCount;
                        for (int off = 0; off < partRows; off += VectorBatch.DEFAULT_BATCH_SIZE) {
                            int len = Math.min(VectorBatch.DEFAULT_BATCH_SIZE, partRows - off);
                            VectorBatch batch = sliceBatch(cols, off, len);
                            VectorExprEvaluator.filterBatch(batch, probeScan.pushedPredicates(), probeScan.colNameToProjectedIndex());
                            if (batch.isEmpty()) continue;

                            double[][] probeAggVals = new double[aggSpecs.size()][];
                            for (int a = 0; a < aggSpecs.size(); a++) {
                                if (aggIsProbeOnly[a]) {
                                    probeAggVals[a] = VectorExprEvaluator.evalNumericBatch(
                                            batch, aggSpecs.get(a).arg, probeScan.colNameToProjectedIndex());
                                }
                            }
                            ColumnVector keyVec = batch.columns[probeKeyProjIdx];
                            int n = batch.selectedCount;
                            boolean hasSel = batch.hasSelection;
                            int[] sel = batch.selection;
                            JoinedRowAccessor accessor = new JoinedRowAccessor(buildTable, buildSlots, probeAlias, batch);

                            for (int i = 0; i < n; i++) {
                                int pr = hasSel ? sel[i] : i;
                                long key = extractProbeKey(keyVec, probeKeyType, pr);
                                int br = buildTable.probeFirst(key);
                                while (br != -1) {
                                    accessor.setRow(br, pr);
                                    if (passesResidual(plan.residualPredicates, plan.aliasToScan, accessor)) {
                                        for (int a = 0; a < aggSpecs.size(); a++) {
                                            double v = aggFuncs[a] == Expr.AggFunc.COUNT ? 1.0
                                                    : (aggIsProbeOnly[a] ? probeAggVals[a][i]
                                                    : VectorExprEvaluator.evalNumericScalar(aggSpecs.get(a).arg, plan.aliasToScan, accessor));
                                            local.accumulate(a, aggFuncs[a], v);
                                        }
                                    }
                                    br = buildTable.nextMatch(br);
                                }
                            }
                        }
                    }
                    return local;
                });
            }
            AggregateState merged = new AggregateState(aggSpecs.size());
            for (Future<AggregateState> f : pool.invokeAll(tasks)) {
                merged.mergeFrom(f.get(), aggFuncs);
            }
            Object[] row = new Object[selectBindings.size()];
            for (int i = 0; i < selectBindings.size(); i++) {
                SelectBinding b = selectBindings.get(i);
                row[i] = merged.finalizeValue(b.aggIdx, aggFuncs[b.aggIdx]);
            }
            List<Object[]> out = new ArrayList<>(1);
            out.add(row);
            return out;
        }

        if (plan.isAggregation) {
            // Check if ALL groupBy columns come from buildTable -> fast path keying directly by buildRowId!
            boolean allGbFromBuild = true;
            for (boolean b : gbFromBuild) {
                if (!b) {
                    allGbFromBuild = false;
                    break;
                }
            }

            List<Callable<Map<AggregateState.GroupKey, AggregateState>>> tasks = new ArrayList<>(workers);
            for (int w = 0; w < workers; w++) {
                final boolean fastBuildKey = allGbFromBuild;
                tasks.add(() -> {
                    Map<AggregateState.GroupKey, AggregateState> localMap = new HashMap<>(256);
                    // When all GROUP BY columns are from build side (e.g., TPC-H Q3), accumulate in flat array indexed by buildRowId!
                    AggregateState[] byBuildRow = fastBuildKey ? new AggregateState[buildTable.rowCount()] : null;

                    int idx;
                    while ((idx = nextMorsel.getAndIncrement()) < probeParts.size()) {
                        ColumnVector[] cols = loadProjectedColumns(probeScan, probeParts.get(idx), bytesRead, cacheHits, cacheMisses);
                        int partRows = probeParts.get(idx).rowCount;
                        for (int off = 0; off < partRows; off += VectorBatch.DEFAULT_BATCH_SIZE) {
                            int len = Math.min(VectorBatch.DEFAULT_BATCH_SIZE, partRows - off);
                            VectorBatch batch = sliceBatch(cols, off, len);
                            VectorExprEvaluator.filterBatch(batch, probeScan.pushedPredicates(), probeScan.colNameToProjectedIndex());
                            if (batch.isEmpty()) continue;

                            double[][] probeAggVals = new double[aggSpecs.size()][];
                            for (int a = 0; a < aggSpecs.size(); a++) {
                                if (aggIsProbeOnly[a]) {
                                    probeAggVals[a] = VectorExprEvaluator.evalNumericBatch(
                                            batch, aggSpecs.get(a).arg, probeScan.colNameToProjectedIndex());
                                }
                            }
                            ColumnVector keyVec = batch.columns[probeKeyProjIdx];
                            int n = batch.selectedCount;
                            boolean hasSel = batch.hasSelection;
                            int[] sel = batch.selection;
                            JoinedRowAccessor accessor = new JoinedRowAccessor(buildTable, buildSlots, probeAlias, batch);

                            for (int i = 0; i < n; i++) {
                                int pr = hasSel ? sel[i] : i;
                                long key = extractProbeKey(keyVec, probeKeyType, pr);
                                int br = buildTable.probeFirst(key);
                                while (br != -1) {
                                    if (plan.residualPredicates.isEmpty() || passesResidual(plan.residualPredicates, plan.aliasToScan, accessor.setRow(br, pr))) {
                                        AggregateState st;
                                        if (fastBuildKey) {
                                            st = byBuildRow[br];
                                            if (st == null) {
                                                st = new AggregateState(aggSpecs.size());
                                                byBuildRow[br] = st;
                                            }
                                        } else {
                                            Object[] gkVals = new Object[gbFromBuild.length];
                                            for (int g = 0; g < gbFromBuild.length; g++) {
                                                gkVals[g] = gbFromBuild[g]
                                                        ? buildTable.getBoxed(gbSlotOrProj[g], br)
                                                        : batch.columns[gbSlotOrProj[g]].getBoxed(pr);
                                            }
                                            AggregateState.GroupKey gk = new AggregateState.GroupKey(gkVals);
                                            st = localMap.get(gk);
                                            if (st == null) {
                                                st = new AggregateState(aggSpecs.size());
                                                localMap.put(gk, st);
                                            }
                                        }
                                        for (int a = 0; a < aggSpecs.size(); a++) {
                                            double v;
                                            if (aggFuncs[a] == Expr.AggFunc.COUNT) {
                                                v = 1.0;
                                            } else if (aggIsProbeOnly[a]) {
                                                v = probeAggVals[a][i];
                                            } else {
                                                accessor.setRow(br, pr);
                                                v = VectorExprEvaluator.evalNumericScalar(aggSpecs.get(a).arg, plan.aliasToScan, accessor);
                                            }
                                            st.accumulate(a, aggFuncs[a], v);
                                        }
                                    }
                                    br = buildTable.nextMatch(br);
                                }
                            }
                        }
                    }

                    if (fastBuildKey) {
                        for (int br = 0; br < byBuildRow.length; br++) {
                            AggregateState st = byBuildRow[br];
                            if (st != null) {
                                Object[] gkVals = new Object[gbFromBuild.length];
                                for (int g = 0; g < gbFromBuild.length; g++) {
                                    gkVals[g] = buildTable.getBoxed(gbSlotOrProj[g], br);
                                }
                                AggregateState.GroupKey gk = new AggregateState.GroupKey(gkVals);
                                AggregateState existing = localMap.get(gk);
                                if (existing == null) {
                                    localMap.put(gk, st);
                                } else {
                                    existing.mergeFrom(st, aggFuncs);
                                }
                            }
                        }
                    }
                    return localMap;
                });
            }

            Map<AggregateState.GroupKey, AggregateState> mergedMap = new HashMap<>();
            for (Future<Map<AggregateState.GroupKey, AggregateState>> f : pool.invokeAll(tasks)) {
                for (var e : f.get().entrySet()) {
                    AggregateState existing = mergedMap.get(e.getKey());
                    if (existing == null) {
                        mergedMap.put(e.getKey(), e.getValue());
                    } else {
                        existing.mergeFrom(e.getValue(), aggFuncs);
                    }
                }
            }
            return materializeGroupedResult(mergedMap, selectBindings, aggFuncs);
        }

        // Non-aggregated joined SELECT
        List<Callable<List<Object[]>>> tasks = new ArrayList<>(workers);
        for (int w = 0; w < workers; w++) {
            tasks.add(() -> {
                List<Object[]> localRows = new ArrayList<>();
                int idx;
                while ((idx = nextMorsel.getAndIncrement()) < probeParts.size()) {
                    ColumnVector[] cols = loadProjectedColumns(probeScan, probeParts.get(idx), bytesRead, cacheHits, cacheMisses);
                    int partRows = probeParts.get(idx).rowCount;
                    for (int off = 0; off < partRows; off += VectorBatch.DEFAULT_BATCH_SIZE) {
                        int len = Math.min(VectorBatch.DEFAULT_BATCH_SIZE, partRows - off);
                        VectorBatch batch = sliceBatch(cols, off, len);
                        VectorExprEvaluator.filterBatch(batch, probeScan.pushedPredicates(), probeScan.colNameToProjectedIndex());
                        if (batch.isEmpty()) continue;

                        ColumnVector keyVec = batch.columns[probeKeyProjIdx];
                        int n = batch.selectedCount;
                        boolean hasSel = batch.hasSelection;
                        int[] sel = batch.selection;
                        JoinedRowAccessor accessor = new JoinedRowAccessor(buildTable, buildSlots, probeAlias, batch);

                        for (int i = 0; i < n; i++) {
                            int pr = hasSel ? sel[i] : i;
                            long key = extractProbeKey(keyVec, probeKeyType, pr);
                            int br = buildTable.probeFirst(key);
                            while (br != -1) {
                                accessor.setRow(br, pr);
                                if (passesResidual(plan.residualPredicates, plan.aliasToScan, accessor)) {
                                    Object[] row = new Object[selectBindings.size()];
                                    for (int b = 0; b < selectBindings.size(); b++) {
                                        SelectBinding sb = selectBindings.get(b);
                                        if (sb.resolvedCol != null) {
                                            row[b] = accessor.getBoxed(sb.resolvedCol);
                                        } else {
                                            row[b] = VectorExprEvaluator.evalNumericScalar(sb.expr, plan.aliasToScan, accessor);
                                        }
                                    }
                                    localRows.add(row);
                                }
                                br = buildTable.nextMatch(br);
                            }
                        }
                    }
                }
                return localRows;
            });
        }
        List<Object[]> allRows = new ArrayList<>();
        for (Future<List<Object[]>> f : pool.invokeAll(tasks)) {
            allRows.addAll(f.get());
        }
        return allRows;
    }

    private BuildHashTable materializeScanToBuildTable(
            LogicalPlan.TableScanPlan scan,
            AtomicLong bytesRead,
            AtomicLong cacheHits,
            AtomicLong cacheMisses) throws IOException {
        List<LogicalPlan.ResolvedColumn> cols = buildResolvedColumns(scan);
        int estRows = (int) Math.min(Integer.MAX_VALUE / 4, Math.max(64, scan.survivingRows() / 4));
        BuildHashTable table = new BuildHashTable(cols, estRows);

        for (PartitionMetadata part : scan.survivingPartitions()) {
            ColumnVector[] vecs = loadProjectedColumns(scan, part, bytesRead, cacheHits, cacheMisses);
            for (int off = 0; off < part.rowCount; off += VectorBatch.DEFAULT_BATCH_SIZE) {
                int len = Math.min(VectorBatch.DEFAULT_BATCH_SIZE, part.rowCount - off);
                VectorBatch batch = sliceBatch(vecs, off, len);
                VectorExprEvaluator.filterBatch(batch, scan.pushedPredicates(), scan.colNameToProjectedIndex());
                if (batch.isEmpty()) continue;

                int n = batch.selectedCount;
                boolean hasSel = batch.hasSelection;
                int[] sel = batch.selection;
                for (int i = 0; i < n; i++) {
                    int r = hasSel ? sel[i] : i;
                    int dst = table.appendRow();
                    for (int c = 0; c < cols.size(); c++) {
                        copyCellToBuild(batch.columns[c], r, table, c, dst);
                    }
                }
            }
        }
        return table;
    }

    private BuildHashTable probeAndMaterializeNextBuild(
            BuildHashTable prevBuild,
            LogicalPlan.JoinStep step,
            AtomicLong bytesRead,
            AtomicLong cacheHits,
            AtomicLong cacheMisses) throws IOException {
        LogicalPlan.TableScanPlan scan = step.probeScan();
        List<LogicalPlan.ResolvedColumn> probeCols = buildResolvedColumns(scan);
        List<LogicalPlan.ResolvedColumn> combinedCols = new ArrayList<>(prevBuild.schemaCols.size() + probeCols.size());
        combinedCols.addAll(prevBuild.schemaCols);
        combinedCols.addAll(probeCols);

        int estRows = (int) Math.min(Integer.MAX_VALUE / 4, Math.max(64, scan.survivingRows() / 4));
        BuildHashTable next = new BuildHashTable(combinedCols, estRows);
        int prevCount = prevBuild.schemaCols.size();
        int probeKeyProj = step.probeKey().projectedIndex();
        ColumnType probeKeyType = step.probeKey().type();

        for (PartitionMetadata part : scan.survivingPartitions()) {
            ColumnVector[] vecs = loadProjectedColumns(scan, part, bytesRead, cacheHits, cacheMisses);
            for (int off = 0; off < part.rowCount; off += VectorBatch.DEFAULT_BATCH_SIZE) {
                int len = Math.min(VectorBatch.DEFAULT_BATCH_SIZE, part.rowCount - off);
                VectorBatch batch = sliceBatch(vecs, off, len);
                VectorExprEvaluator.filterBatch(batch, scan.pushedPredicates(), scan.colNameToProjectedIndex());
                if (batch.isEmpty()) continue;

                ColumnVector keyVec = batch.columns[probeKeyProj];
                int n = batch.selectedCount;
                boolean hasSel = batch.hasSelection;
                int[] sel = batch.selection;
                for (int i = 0; i < n; i++) {
                    int pr = hasSel ? sel[i] : i;
                    long key = extractProbeKey(keyVec, probeKeyType, pr);
                    int br = prevBuild.probeFirst(key);
                    while (br != -1) {
                        int dst = next.appendRow();
                        for (int c = 0; c < prevCount; c++) {
                            copyBuildToBuild(prevBuild, c, br, next, c, dst);
                        }
                        for (int c = 0; c < probeCols.size(); c++) {
                            copyCellToBuild(batch.columns[c], pr, next, prevCount + c, dst);
                        }
                        br = prevBuild.nextMatch(br);
                    }
                }
            }
        }
        return next;
    }

    // ==================== HELPERS ====================

    private ColumnVector[] loadProjectedColumns(
            LogicalPlan.TableScanPlan scan,
            PartitionMetadata part,
            AtomicLong bytesRead,
            AtomicLong cacheHits,
            AtomicLong cacheMisses) throws IOException {
        TableStorage table = scan.table();
        int[] proj = scan.projectedColumnIndices();
        ColumnVector[] out = new ColumnVector[proj.length];
        ShardedLruCache cache = useCache ? db.cache() : null;
        for (int i = 0; i < proj.length; i++) {
            out[i] = table.loadColumn(part, proj[i], cache, bytesRead, cacheHits, cacheMisses);
        }
        return out;
    }

    private static VectorBatch sliceBatch(ColumnVector[] partCols, int offset, int length) {
        ColumnVector[] sliced = new ColumnVector[partCols.length];
        for (int i = 0; i < partCols.length; i++) {
            sliced[i] = partCols[i].slice(offset, length);
        }
        return new VectorBatch(sliced, length);
    }

    private static List<LogicalPlan.ResolvedColumn> buildResolvedColumns(LogicalPlan.TableScanPlan scan) {
        int[] proj = scan.projectedColumnIndices();
        List<LogicalPlan.ResolvedColumn> cols = new ArrayList<>(proj.length);
        for (int i = 0; i < proj.length; i++) {
            var def = scan.table().schema().column(proj[i]);
            cols.add(new LogicalPlan.ResolvedColumn(
                    scan.alias(), def.name().toLowerCase(Locale.ROOT), proj[i], i, def.type()));
        }
        return cols;
    }

    private static void copyCellToBuild(ColumnVector vec, int srcRow, BuildHashTable dst, int slot, int dstRow) {
        if (vec instanceof LongColumnVector lv) dst.setLong(slot, dstRow, lv.get(srcRow));
        else if (vec instanceof DoubleColumnVector dv) dst.setDouble(slot, dstRow, dv.get(srcRow));
        else if (vec instanceof IntColumnVector iv) dst.setDate(slot, dstRow, iv.get(srcRow));
        else if (vec instanceof StringColumnVector sv) dst.setString(slot, dstRow, sv.get(srcRow));
    }

    private static void copyBuildToBuild(
            BuildHashTable src, int srcSlot, int srcRow, BuildHashTable dst, int dstSlot, int dstRow) {
        switch (src.schemaCols.get(srcSlot).type()) {
            case INT64 -> dst.setLong(dstSlot, dstRow, src.getLong(srcSlot, srcRow));
            case DOUBLE -> dst.setDouble(dstSlot, dstRow, src.getDouble(srcSlot, srcRow));
            case DATE -> dst.setDate(dstSlot, dstRow, src.getDate(srcSlot, srcRow));
            case STRING -> dst.setString(dstSlot, dstRow, src.getString(srcSlot, srcRow));
        }
    }

    private static long extractProbeKey(ColumnVector vec, ColumnType type, int row) {
        return switch (type) {
            case INT64 -> ((LongColumnVector) vec).get(row);
            case DATE -> ((IntColumnVector) vec).get(row);
            case STRING -> ((StringColumnVector) vec).get(row).hashCode();
            case DOUBLE -> Double.doubleToLongBits(((DoubleColumnVector) vec).get(row));
        };
    }

    private static boolean allStringGroupBy(ColumnVector[] cols, int[] gbProj) {
        for (int idx : gbProj) {
            if (!(cols[idx] instanceof StringColumnVector)) return false;
        }
        return true;
    }

    private static boolean exprOnlyUsesAlias(
            Expr expr, String alias, Map<String, LogicalPlan.TableScanPlan> aliasToScan) {
        if (expr instanceof Expr.ColumnRef ref) {
            return QueryPlanner.resolveColumn(ref, aliasToScan).tableAlias().equals(alias);
        }
        if (expr instanceof Expr.BinaryOp b) {
            return exprOnlyUsesAlias(b.left(), alias, aliasToScan)
                    && exprOnlyUsesAlias(b.right(), alias, aliasToScan);
        }
        if (expr instanceof Expr.UnaryMinus u) {
            return exprOnlyUsesAlias(u.operand(), alias, aliasToScan);
        }
        return true;
    }

    private static boolean passesResidual(
            List<Expr> residualPredicates,
            Map<String, LogicalPlan.TableScanPlan> aliasToScan,
            VectorExprEvaluator.RowAccessor accessor) {
        for (Expr pred : residualPredicates) {
            if (!VectorExprEvaluator.evalPredicateScalar(pred, aliasToScan, accessor)) {
                return false;
            }
        }
        return true;
    }

    private static List<Object[]> materializeGroupedResult(
            Map<AggregateState.GroupKey, AggregateState> merged,
            List<SelectBinding> bindings,
            Expr.AggFunc[] aggFuncs) {
        List<Object[]> out = new ArrayList<>(merged.size());
        for (var e : merged.entrySet()) {
            Object[] keyVals = e.getKey().values;
            AggregateState st = e.getValue();
            Object[] row = new Object[bindings.size()];
            for (int i = 0; i < bindings.size(); i++) {
                SelectBinding b = bindings.get(i);
                if (b.isAgg) {
                    row[i] = st.finalizeValue(b.aggIdx, aggFuncs[b.aggIdx]);
                } else if (b.groupByIdx >= 0) {
                    row[i] = keyVals[b.groupByIdx];
                } else {
                    throw new IllegalStateException("Non-grouped column in aggregated SELECT: " + b.expr);
                }
            }
            out.add(row);
        }
        return out;
    }

    private static Object[] finalizeGlobalEmpty(List<SelectBinding> bindings, List<AggSpec> aggSpecs) {
        Object[] row = new Object[bindings.size()];
        for (int i = 0; i < bindings.size(); i++) {
            SelectBinding b = bindings.get(i);
            if (b.isAgg) {
                if (aggSpecs.get(b.aggIdx).func == Expr.AggFunc.COUNT) {
                    row[i] = 0L;
                } else {
                    row[i] = 0.0;
                }
            }
        }
        return row;
    }

    private static int findGroupByIndex(
            List<LogicalPlan.ResolvedColumn> groupByCols, LogicalPlan.ResolvedColumn rc) {
        for (int i = 0; i < groupByCols.size(); i++) {
            LogicalPlan.ResolvedColumn g = groupByCols.get(i);
            if (g.tableAlias().equals(rc.tableAlias()) && g.columnName().equals(rc.columnName())) {
                return i;
            }
        }
        return -1;
    }

    private static int resolveOrderByIndex(
            String colOrAlias,
            List<String> outNames,
            List<SelectStatement.SelectItem> items) {
        String target = colOrAlias.toLowerCase(Locale.ROOT);
        int dot = target.indexOf('.');
        String shortTarget = dot >= 0 ? target.substring(dot + 1) : target;
        for (int i = 0; i < outNames.size(); i++) {
            if (outNames.get(i).equalsIgnoreCase(target) || outNames.get(i).equalsIgnoreCase(shortTarget)) {
                return i;
            }
        }
        for (int i = 0; i < items.size(); i++) {
            if (items.get(i).expr() instanceof Expr.ColumnRef ref
                    && (ref.qualifiedName().equalsIgnoreCase(target) || ref.columnName().equalsIgnoreCase(shortTarget))) {
                return i;
            }
        }
        throw new IllegalArgumentException("ORDER BY column not found in SELECT list: " + colOrAlias);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparator<Object[]> buildRowComparator(int[] cols, boolean[] desc) {
        return (a, b) -> {
            for (int i = 0; i < cols.length; i++) {
                Object va = a[cols[i]];
                Object vb = b[cols[i]];
                int cmp;
                if (va == vb) cmp = 0;
                else if (va == null) cmp = -1;
                else if (vb == null) cmp = 1;
                else if (va instanceof Number na && vb instanceof Number nb) {
                    cmp = Double.compare(na.doubleValue(), nb.doubleValue());
                } else {
                    cmp = ((Comparable) va).compareTo(vb);
                }
                if (cmp != 0) {
                    return desc[i] ? -cmp : cmp;
                }
            }
            return 0;
        };
    }

    private record AggSpec(Expr.AggFunc func, Expr arg) {}

    private record SelectBinding(
            boolean isAgg,
            int aggIdx,
            int groupByIdx,
            LogicalPlan.ResolvedColumn resolvedCol,
            Expr expr) {}

    private static final class JoinedRowAccessor implements VectorExprEvaluator.RowAccessor {
        private final BuildHashTable build;
        private final Map<String, Integer> buildSlots;
        private final String probeAlias;
        private final VectorBatch probeBatch;
        private int buildRow;
        private int probeRow;

        JoinedRowAccessor(
                BuildHashTable build,
                Map<String, Integer> buildSlots,
                String probeAlias,
                VectorBatch probeBatch) {
            this.build = build;
            this.buildSlots = buildSlots;
            this.probeAlias = probeAlias;
            this.probeBatch = probeBatch;
        }

        JoinedRowAccessor setRow(int buildRow, int probeRow) {
            this.buildRow = buildRow;
            this.probeRow = probeRow;
            return this;
        }

        @Override
        public long getLong(LogicalPlan.ResolvedColumn col) {
            if (col.tableAlias().equals(probeAlias)) {
                return ((LongColumnVector) probeBatch.columns[col.projectedIndex()]).get(probeRow);
            }
            return build.getLong(buildSlots.get(col.tableAlias() + "." + col.columnName()), buildRow);
        }

        @Override
        public double getDouble(LogicalPlan.ResolvedColumn col) {
            if (col.tableAlias().equals(probeAlias)) {
                ColumnVector v = probeBatch.columns[col.projectedIndex()];
                if (v instanceof DoubleColumnVector dv) return dv.get(probeRow);
                if (v instanceof LongColumnVector lv) return lv.get(probeRow);
                if (v instanceof IntColumnVector iv) return iv.get(probeRow);
                throw new IllegalArgumentException("Non-numeric column: " + col);
            }
            return build.getDouble(buildSlots.get(col.tableAlias() + "." + col.columnName()), buildRow);
        }

        @Override
        public int getDate(LogicalPlan.ResolvedColumn col) {
            if (col.tableAlias().equals(probeAlias)) {
                return ((IntColumnVector) probeBatch.columns[col.projectedIndex()]).get(probeRow);
            }
            return build.getDate(buildSlots.get(col.tableAlias() + "." + col.columnName()), buildRow);
        }

        @Override
        public String getString(LogicalPlan.ResolvedColumn col) {
            if (col.tableAlias().equals(probeAlias)) {
                return ((StringColumnVector) probeBatch.columns[col.projectedIndex()]).get(probeRow);
            }
            return build.getString(buildSlots.get(col.tableAlias() + "." + col.columnName()), buildRow);
        }

        @Override
        public Object getBoxed(LogicalPlan.ResolvedColumn col) {
            if (col.tableAlias().equals(probeAlias)) {
                return probeBatch.columns[col.projectedIndex()].getBoxed(probeRow);
            }
            return build.getBoxed(buildSlots.get(col.tableAlias() + "." + col.columnName()), buildRow);
        }
    }

    @Override
    public void close() {
        pool.shutdownNow();
    }
}
