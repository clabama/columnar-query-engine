package dev.wixforth.columnar.sql;

import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.storage.PartitionMetadata;
import dev.wixforth.columnar.storage.TableStorage;

import java.util.List;
import java.util.Map;

/**
 * Physical/logical query plan produced by {@link QueryPlanner} after predicate pushdown,
 * zone-map micro-partition pruning, and column projection pruning.
 */
public final class LogicalPlan {

    /**
     * Resolved reference to a specific table alias and column index within that table's projected schema.
     */
    public record ResolvedColumn(
            String tableAlias,
            String columnName,
            int tableColIndex,
            int projectedIndex,
            ColumnType type) {}

    /**
     * Pruned scan plan for a single table.
     */
    public record TableScanPlan(
            String alias,
            TableStorage table,
            int[] projectedColumnIndices,
            Map<String, Integer> colNameToProjectedIndex,
            List<Expr> pushedPredicates,
            List<PartitionMetadata> survivingPartitions,
            int totalPartitions,
            int prunedPartitions) {

        public long survivingRows() {
            long total = 0;
            for (PartitionMetadata p : survivingPartitions) {
                total += p.rowCount;
            }
            return total;
        }
    }

    /**
     * One step in a left-deep hash join pipeline:
     * Build a hash table on the current accumulated build relation keyed by {@code buildKey},
     * then probe it using {@code probeScan} on {@code probeKey}.
     */
    public record JoinStep(
            TableScanPlan probeScan,
            ResolvedColumn buildKey,
            ResolvedColumn probeKey) {}

    public final TableScanPlan rootScan;
    public final List<JoinStep> joinSteps;
    public final List<Expr> residualPredicates;
    public final List<SelectStatement.SelectItem> selectItems;
    public final List<ResolvedColumn> groupByColumns;
    public final List<SelectStatement.OrderByItem> orderBy;
    public final Integer limit;
    public final boolean isAggregation;
    public final Map<String, TableScanPlan> aliasToScan;

    public LogicalPlan(
            TableScanPlan rootScan,
            List<JoinStep> joinSteps,
            List<Expr> residualPredicates,
            List<SelectStatement.SelectItem> selectItems,
            List<ResolvedColumn> groupByColumns,
            List<SelectStatement.OrderByItem> orderBy,
            Integer limit,
            boolean isAggregation,
            Map<String, TableScanPlan> aliasToScan) {
        this.rootScan = rootScan;
        this.joinSteps = joinSteps;
        this.residualPredicates = residualPredicates;
        this.selectItems = selectItems;
        this.groupByColumns = groupByColumns;
        this.orderBy = orderBy;
        this.limit = limit;
        this.isAggregation = isAggregation;
        this.aliasToScan = aliasToScan;
    }

    /**
     * Renders a human-readable EXPLAIN plan showing partition pruning, projected columns, and operators.
     */
    public String explain() {
        StringBuilder sb = new StringBuilder();
        if (limit != null) {
            sb.append("Limit (n=").append(limit).append(")\n");
        }
        if (!orderBy.isEmpty()) {
            sb.append("  Sort (").append(orderBy).append(")\n");
        }
        if (isAggregation) {
            sb.append("  ParallelHashAggregate (groupBy=").append(groupByColumns.size()).append(" cols)\n");
        } else {
            sb.append("  Project (").append(selectItems.size()).append(" exprs)\n");
        }
        if (!residualPredicates.isEmpty()) {
            sb.append("  ResidualFilter (").append(residualPredicates.size()).append(" preds)\n");
        }
        for (int i = joinSteps.size() - 1; i >= 0; i--) {
            JoinStep step = joinSteps.get(i);
            sb.append("  VectorizedHashJoin (")
                    .append(step.buildKey().tableAlias()).append(".").append(step.buildKey().columnName())
                    .append(" = ")
                    .append(step.probeKey().tableAlias()).append(".").append(step.probeKey().columnName())
                    .append(")\n");
            sb.append("    Probe: ").append(formatScan(step.probeScan())).append("\n");
        }
        sb.append("    ").append(joinSteps.isEmpty() ? "Scan: " : "BuildRoot: ")
                .append(formatScan(rootScan)).append("\n");
        return sb.toString();
    }

    private static String formatScan(TableScanPlan scan) {
        double prunedPct = scan.totalPartitions() == 0 ? 0.0
                : (100.0 * scan.prunedPartitions() / scan.totalPartitions());
        StringBuilder cols = new StringBuilder("[");
        for (int i = 0; i < scan.projectedColumnIndices().length; i++) {
            if (i > 0) cols.append(", ");
            cols.append(scan.table().schema().column(scan.projectedColumnIndices()[i]).name());
        }
        cols.append("]");
        return String.format(
                java.util.Locale.ROOT,
                "TableScan(%s AS %s, projected=%d/%d %s, partitions=%d/%d pruned (%.1f%%), pushedPreds=%d)",
                scan.table().name(),
                scan.alias(),
                scan.projectedColumnIndices().length,
                scan.table().schema().columnCount(),
                cols,
                scan.prunedPartitions(),
                scan.totalPartitions(),
                prunedPct,
                scan.pushedPredicates().size());
    }
}
