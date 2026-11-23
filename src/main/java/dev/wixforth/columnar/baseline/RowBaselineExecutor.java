package dev.wixforth.columnar.baseline;

import dev.wixforth.columnar.exec.AggregateState;
import dev.wixforth.columnar.exec.ExecutionStats;
import dev.wixforth.columnar.exec.QueryResult;
import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.DateUtil;
import dev.wixforth.columnar.sql.Expr;
import dev.wixforth.columnar.sql.SelectStatement;
import dev.wixforth.columnar.sql.SqlParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Single-threaded row-oriented Volcano-style SQL query executor.
 * Scans every row and every column sequentially from row-major binary files without
 * micro-partition pruning, column projection pruning, vectorization, or parallelism.
 */
public final class RowBaselineExecutor {
    private final Map<String, RowStoreTable> tables = new HashMap<>();

    public void registerTable(RowStoreTable table) {
        tables.put(table.name().toLowerCase(Locale.ROOT), table);
    }

    public QueryResult execute(String sql) {
        long startNanos = System.nanoTime();
        SelectStatement stmt = SqlParser.parse(sql);

        Map<String, RowStoreTable> aliasToTable = new LinkedHashMap<>();
        List<String> aliases = new ArrayList<>();
        String rootAlias = stmt.fromTable().effectiveAlias().toLowerCase(Locale.ROOT);
        aliasToTable.put(rootAlias, getTable(stmt.fromTable().tableName()));
        aliases.add(rootAlias);
        for (SelectStatement.JoinClause jc : stmt.joins()) {
            String alias = jc.rightTable().effectiveAlias().toLowerCase(Locale.ROOT);
            aliasToTable.put(alias, getTable(jc.rightTable().tableName()));
            aliases.add(alias);
        }

        // Build unified row offset map: alias -> base column offset in combined row
        Map<String, Integer> aliasOffsets = new HashMap<>();
        int totalCols = 0;
        long totalBytesRead = 0;
        for (String alias : aliases) {
            aliasOffsets.put(alias, totalCols);
            RowStoreTable t = aliasToTable.get(alias);
            totalCols += t.schema().columnCount();
            totalBytesRead += t.fileBytes();
        }

        // Split single-table predicates for fair row-level filter pushdown (no zone maps)
        Map<String, List<Expr>> tablePreds = new HashMap<>();
        for (String a : aliases) tablePreds.put(a, new ArrayList<>());
        List<Expr> residualPreds = new ArrayList<>();
        if (stmt.whereClause() != null) {
            List<Expr> conjuncts = new ArrayList<>();
            splitAnd(stmt.whereClause(), conjuncts);
            for (Expr p : conjuncts) {
                var touched = new java.util.HashSet<String>();
                collectAliases(p, aliasToTable, touched);
                if (touched.size() == 1) {
                    tablePreds.get(touched.iterator().next()).add(p);
                } else {
                    residualPreds.add(p);
                }
            }
        }

        // Prepare output schema & aggregates
        boolean isAgg = !stmt.groupBy().isEmpty();
        List<String> outNames = new ArrayList<>();
        List<ColumnType> outTypes = new ArrayList<>();
        List<Expr.AggregateCall> aggCalls = new ArrayList<>();
        List<Integer> itemToAggIdx = new ArrayList<>();

        for (int i = 0; i < stmt.selectItems().size(); i++) {
            SelectStatement.SelectItem item = stmt.selectItems().get(i);
            if (item.expr() instanceof Expr.AggregateCall agg) {
                isAgg = true;
                itemToAggIdx.add(aggCalls.size());
                aggCalls.add(agg);
                outNames.add(item.alias() != null ? item.alias() : agg.func().name().toLowerCase(Locale.ROOT) + "_" + i);
                outTypes.add(agg.func() == Expr.AggFunc.COUNT ? ColumnType.INT64 : ColumnType.DOUBLE);
            } else {
                itemToAggIdx.add(-1);
                if (item.expr() instanceof Expr.ColumnRef ref) {
                    ResolvedSlot rs = resolve(ref, aliasToTable, aliasOffsets);
                    outNames.add(item.alias() != null ? item.alias() : ref.columnName().toLowerCase(Locale.ROOT));
                    outTypes.add(rs.type);
                } else {
                    outNames.add(item.alias() != null ? item.alias() : "expr_" + i);
                    outTypes.add(ColumnType.DOUBLE);
                }
            }
        }

        Expr.AggFunc[] aggFuncs = aggCalls.stream().map(Expr.AggregateCall::func).toArray(Expr.AggFunc[]::new);
        List<ResolvedSlot> gbSlots = new ArrayList<>();
        for (Expr.ColumnRef gb : stmt.groupBy()) {
            gbSlots.add(resolve(gb, aliasToTable, aliasOffsets));
        }

        // Row consumer (global agg, grouped agg, or raw projection)
        AggregateState globalAgg = (isAgg && gbSlots.isEmpty()) ? new AggregateState(aggCalls.size()) : null;
        Map<AggregateState.GroupKey, AggregateState> groupedAgg = (isAgg && !gbSlots.isEmpty()) ? new HashMap<>() : null;
        List<Object[]> rawRows = !isAgg ? new ArrayList<>() : null;

        final boolean finalIsAgg = isAgg;
        RowConsumer consumer = combinedRow -> {
            for (Expr rp : residualPreds) {
                if (!evalPred(rp, combinedRow, aliasToTable, aliasOffsets)) return;
            }
            if (finalIsAgg) {
                AggregateState st;
                if (globalAgg != null) {
                    st = globalAgg;
                } else {
                    Object[] gkVals = new Object[gbSlots.size()];
                    for (int g = 0; g < gbSlots.size(); g++) {
                        gkVals[g] = formatBoxed(combinedRow[gbSlots.get(g).slot], gbSlots.get(g).type);
                    }
                    AggregateState.GroupKey gk = new AggregateState.GroupKey(gkVals);
                    st = groupedAgg.computeIfAbsent(gk, k -> new AggregateState(aggCalls.size()));
                }
                for (int a = 0; a < aggCalls.size(); a++) {
                    Expr.AggregateCall call = aggCalls.get(a);
                    double v = call.func() == Expr.AggFunc.COUNT ? 1.0
                            : evalNum(call.argument(), combinedRow, aliasToTable, aliasOffsets);
                    st.accumulate(a, call.func(), v);
                }
            } else {
                Object[] outRow = new Object[stmt.selectItems().size()];
                for (int i = 0; i < stmt.selectItems().size(); i++) {
                    Expr expr = stmt.selectItems().get(i).expr();
                    if (expr instanceof Expr.ColumnRef ref) {
                        ResolvedSlot rs = resolve(ref, aliasToTable, aliasOffsets);
                        outRow[i] = formatBoxed(combinedRow[rs.slot], rs.type);
                    } else {
                        outRow[i] = evalNum(expr, combinedRow, aliasToTable, aliasOffsets);
                    }
                }
                rawRows.add(outRow);
            }
        };

        try {
            if (stmt.joins().isEmpty()) {
                RowStoreTable root = aliasToTable.get(rootAlias);
                List<Expr> preds = tablePreds.get(rootAlias);
                try (RowStoreTable.RowCursor cur = root.openCursor()) {
                    while (cur.hasNext()) {
                        Object[] row = cur.next();
                        if (matchesAll(preds, row, aliasToTable, aliasOffsets)) {
                            consumer.accept(row);
                        }
                    }
                }
            } else {
                // Sequential left-deep hash join over full row-major tuples
                RowStoreTable root = aliasToTable.get(rootAlias);
                int rootCols = root.schema().columnCount();
                List<Object[]> currentRows = new ArrayList<>();
                try (RowStoreTable.RowCursor cur = root.openCursor()) {
                    while (cur.hasNext()) {
                        Object[] row = cur.next();
                        if (matchesAll(tablePreds.get(rootAlias), row, aliasToTable, aliasOffsets)) {
                            Object[] copy = new Object[totalCols];
                            System.arraycopy(row, 0, copy, 0, rootCols);
                            currentRows.add(copy);
                        }
                    }
                }

                for (int j = 0; j < stmt.joins().size(); j++) {
                    SelectStatement.JoinClause jc = stmt.joins().get(j);
                    String rightAlias = jc.rightTable().effectiveAlias().toLowerCase(Locale.ROOT);
                    RowStoreTable rightTable = aliasToTable.get(rightAlias);
                    int rightOff = aliasOffsets.get(rightAlias);
                    int rightCols = rightTable.schema().columnCount();

                    ResolvedSlot k1 = resolve(jc.leftKey(), aliasToTable, aliasOffsets);
                    ResolvedSlot k2 = resolve(jc.rightKey(), aliasToTable, aliasOffsets);
                    ResolvedSlot buildKey = k1.alias.equals(rightAlias) ? k2 : k1;
                    ResolvedSlot probeKey = k1.alias.equals(rightAlias) ? k1 : k2;

                    Map<Object, List<Object[]>> hashTable = new HashMap<>(currentRows.size() * 2);
                    for (Object[] r : currentRows) {
                        hashTable.computeIfAbsent(r[buildKey.slot], k -> new ArrayList<>(2)).add(r);
                    }
                    currentRows = null; // allow GC

                    boolean isLastJoin = (j == stmt.joins().size() - 1);
                    List<Object[]> nextRows = isLastJoin ? null : new ArrayList<>();
                    List<Expr> rPreds = tablePreds.get(rightAlias);
                    Object[] probeBuf = new Object[totalCols];

                    try (RowStoreTable.RowCursor cur = rightTable.openCursor()) {
                        while (cur.hasNext()) {
                            Object[] rRow = cur.next();
                            System.arraycopy(rRow, 0, probeBuf, rightOff, rightCols);
                            if (!matchesAll(rPreds, probeBuf, aliasToTable, aliasOffsets)) continue;

                            List<Object[]> matches = hashTable.get(probeBuf[probeKey.slot]);
                            if (matches == null) continue;
                            for (Object[] leftRow : matches) {
                                if (isLastJoin) {
                                    System.arraycopy(rRow, 0, leftRow, rightOff, rightCols);
                                    consumer.accept(leftRow);
                                } else {
                                    Object[] merged = leftRow.clone();
                                    System.arraycopy(rRow, 0, merged, rightOff, rightCols);
                                    nextRows.add(merged);
                                }
                            }
                        }
                    }
                    currentRows = nextRows;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Baseline execution failed", e);
        }

        List<Object[]> resultRows;
        if (isAgg) {
            if (globalAgg != null) {
                Object[] row = new Object[stmt.selectItems().size()];
                for (int i = 0; i < stmt.selectItems().size(); i++) {
                    int ai = itemToAggIdx.get(i);
                    row[i] = globalAgg.finalizeValue(ai, aggFuncs[ai]);
                }
                resultRows = new ArrayList<>(1);
                resultRows.add(row);
            } else {
                resultRows = new ArrayList<>(groupedAgg.size());
                for (var e : groupedAgg.entrySet()) {
                    Object[] row = new Object[stmt.selectItems().size()];
                    for (int i = 0; i < stmt.selectItems().size(); i++) {
                        int ai = itemToAggIdx.get(i);
                        if (ai >= 0) {
                            row[i] = e.getValue().finalizeValue(ai, aggFuncs[ai]);
                        } else {
                            Expr.ColumnRef ref = (Expr.ColumnRef) stmt.selectItems().get(i).expr();
                            ResolvedSlot rs = resolve(ref, aliasToTable, aliasOffsets);
                            int gIdx = findGbSlot(gbSlots, rs);
                            row[i] = e.getKey().values[gIdx];
                        }
                    }
                    resultRows.add(row);
                }
            }
        } else {
            resultRows = rawRows;
        }

        if (!stmt.orderBy().isEmpty() && resultRows.size() > 1) {
            int[] cols = new int[stmt.orderBy().size()];
            boolean[] desc = new boolean[stmt.orderBy().size()];
            for (int i = 0; i < stmt.orderBy().size(); i++) {
                cols[i] = resolveOrderCol(stmt.orderBy().get(i).columnOrAlias(), outNames, stmt.selectItems());
                desc[i] = stmt.orderBy().get(i).descending();
            }
            resultRows.sort(rowComparator(cols, desc));
        }

        if (stmt.limit() != null && resultRows.size() > stmt.limit()) {
            resultRows = new ArrayList<>(resultRows.subList(0, stmt.limit()));
        }

        long elapsed = System.nanoTime() - startNanos;
        ExecutionStats stats = new ExecutionStats(
                elapsed, 1, 0, 1, totalBytesRead, totalBytesRead, 0, 0, 1);
        return new QueryResult(outNames, outTypes, resultRows, stats);
    }

    private interface RowConsumer {
        void accept(Object[] combinedRow);
    }

    private record ResolvedSlot(String alias, int slot, ColumnType type) {}

    private RowStoreTable getTable(String name) {
        RowStoreTable t = tables.get(name.toLowerCase(Locale.ROOT));
        if (t == null) throw new IllegalArgumentException("Unknown baseline table: " + name);
        return t;
    }

    private static ResolvedSlot resolve(
            Expr.ColumnRef ref, Map<String, RowStoreTable> aliasToTable, Map<String, Integer> offsets) {
        if (ref.tableAlias() != null) {
            String a = ref.tableAlias().toLowerCase(Locale.ROOT);
            RowStoreTable t = aliasToTable.get(a);
            int idx = t.schema().indexOf(ref.columnName());
            return new ResolvedSlot(a, offsets.get(a) + idx, t.schema().column(idx).type());
        }
        for (var e : aliasToTable.entrySet()) {
            if (e.getValue().schema().hasColumn(ref.columnName())) {
                int idx = e.getValue().schema().indexOf(ref.columnName());
                return new ResolvedSlot(e.getKey(), offsets.get(e.getKey()) + idx, e.getValue().schema().column(idx).type());
            }
        }
        throw new IllegalArgumentException("Unknown column in baseline: " + ref.qualifiedName());
    }

    private static boolean matchesAll(
            List<Expr> preds, Object[] row, Map<String, RowStoreTable> aliasToTable, Map<String, Integer> offsets) {
        for (Expr p : preds) {
            if (!evalPred(p, row, aliasToTable, offsets)) return false;
        }
        return true;
    }

    private static boolean evalPred(
            Expr expr, Object[] row, Map<String, RowStoreTable> aliasToTable, Map<String, Integer> offsets) {
        if (expr instanceof Expr.Between btw) {
            return evalPred(new Expr.BinaryOp(Expr.Op.GTE, btw.value(), btw.low()), row, aliasToTable, offsets)
                    && evalPred(new Expr.BinaryOp(Expr.Op.LTE, btw.value(), btw.high()), row, aliasToTable, offsets);
        }
        if (expr instanceof Expr.BinaryOp bin) {
            if (bin.op() == Expr.Op.AND) {
                return evalPred(bin.left(), row, aliasToTable, offsets)
                        && evalPred(bin.right(), row, aliasToTable, offsets);
            }
            if (bin.op() == Expr.Op.OR) {
                return evalPred(bin.left(), row, aliasToTable, offsets)
                        || evalPred(bin.right(), row, aliasToTable, offsets);
            }
            if (bin.left() instanceof Expr.ColumnRef ref && bin.right() instanceof Expr.StringLiteral sl) {
                ResolvedSlot rs = resolve(ref, aliasToTable, offsets);
                if (rs.type == ColumnType.STRING) {
                    return cmpInt(((String) row[rs.slot]).compareTo(sl.value()), 0, bin.op());
                } else if (rs.type == ColumnType.DATE) {
                    return cmpInt((Integer) row[rs.slot], DateUtil.parseEpochDay(sl.value()), bin.op());
                }
            }
            double l = evalNum(bin.left(), row, aliasToTable, offsets);
            double r = evalNum(bin.right(), row, aliasToTable, offsets);
            return cmpDouble(l, r, bin.op());
        }
        throw new IllegalArgumentException("Unsupported predicate: " + expr);
    }

    private static double evalNum(
            Expr expr, Object[] row, Map<String, RowStoreTable> aliasToTable, Map<String, Integer> offsets) {
        if (expr instanceof Expr.DoubleLiteral d) return d.value();
        if (expr instanceof Expr.LongLiteral l) return (double) l.value();
        if (expr instanceof Expr.UnaryMinus u) return -evalNum(u.operand(), row, aliasToTable, offsets);
        if (expr instanceof Expr.ColumnRef ref) {
            ResolvedSlot rs = resolve(ref, aliasToTable, offsets);
            return ((Number) row[rs.slot]).doubleValue();
        }
        if (expr instanceof Expr.BinaryOp bin) {
            double l = evalNum(bin.left(), row, aliasToTable, offsets);
            double r = evalNum(bin.right(), row, aliasToTable, offsets);
            return switch (bin.op()) {
                case ADD -> l + r;
                case SUB -> l - r;
                case MUL -> l * r;
                case DIV -> l / r;
                default -> throw new IllegalArgumentException("Bad op: " + bin.op());
            };
        }
        throw new IllegalArgumentException("Unsupported numeric expr: " + expr);
    }

    private static Object formatBoxed(Object raw, ColumnType type) {
        if (type == ColumnType.DATE && raw instanceof Integer d) {
            return DateUtil.formatEpochDay(d);
        }
        return raw;
    }

    private static int findGbSlot(List<ResolvedSlot> gbSlots, ResolvedSlot target) {
        for (int i = 0; i < gbSlots.size(); i++) {
            if (gbSlots.get(i).slot == target.slot) return i;
        }
        throw new IllegalStateException("Column not in GROUP BY: " + target);
    }

    private static int resolveOrderCol(String name, List<String> outNames, List<SelectStatement.SelectItem> items) {
        String shortName = name.contains(".") ? name.substring(name.indexOf('.') + 1) : name;
        for (int i = 0; i < outNames.size(); i++) {
            if (outNames.get(i).equalsIgnoreCase(name) || outNames.get(i).equalsIgnoreCase(shortName)) return i;
        }
        for (int i = 0; i < items.size(); i++) {
            if (items.get(i).expr() instanceof Expr.ColumnRef ref
                    && (ref.qualifiedName().equalsIgnoreCase(name) || ref.columnName().equalsIgnoreCase(shortName))) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown ORDER BY column: " + name);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparator<Object[]> rowComparator(int[] cols, boolean[] desc) {
        return (a, b) -> {
            for (int i = 0; i < cols.length; i++) {
                Object va = a[cols[i]];
                Object vb = b[cols[i]];
                int c = (va instanceof Number na && vb instanceof Number nb)
                        ? Double.compare(na.doubleValue(), nb.doubleValue())
                        : ((Comparable) va).compareTo(vb);
                if (c != 0) return desc[i] ? -c : c;
            }
            return 0;
        };
    }

    private static boolean cmpInt(int a, int b, Expr.Op op) {
        return switch (op) {
            case EQ -> a == b;
            case NEQ -> a != b;
            case LT -> a < b;
            case LTE -> a <= b;
            case GT -> a > b;
            case GTE -> a >= b;
            default -> false;
        };
    }

    private static boolean cmpDouble(double a, double b, Expr.Op op) {
        return switch (op) {
            case EQ -> a == b;
            case NEQ -> a != b;
            case LT -> a < b;
            case LTE -> a <= b;
            case GT -> a > b;
            case GTE -> a >= b;
            default -> false;
        };
    }

    private static void splitAnd(Expr e, List<Expr> out) {
        if (e instanceof Expr.BinaryOp b && b.op() == Expr.Op.AND) {
            splitAnd(b.left(), out);
            splitAnd(b.right(), out);
        } else {
            out.add(e);
        }
    }

    private static void collectAliases(Expr e, Map<String, RowStoreTable> tables, java.util.Set<String> out) {
        if (e instanceof Expr.ColumnRef ref) {
            if (ref.tableAlias() != null) {
                out.add(ref.tableAlias().toLowerCase(Locale.ROOT));
            } else {
                for (var entry : tables.entrySet()) {
                    if (entry.getValue().schema().hasColumn(ref.columnName())) {
                        out.add(entry.getKey());
                        break;
                    }
                }
            }
        } else if (e instanceof Expr.BinaryOp b) {
            collectAliases(b.left(), tables, out);
            collectAliases(b.right(), tables, out);
        } else if (e instanceof Expr.Between btw) {
            collectAliases(btw.value(), tables, out);
            collectAliases(btw.low(), tables, out);
            collectAliases(btw.high(), tables, out);
        } else if (e instanceof Expr.UnaryMinus u) {
            collectAliases(u.operand(), tables, out);
        }
    }
}
