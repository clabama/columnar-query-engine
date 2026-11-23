package dev.wixforth.columnar.sql;

import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.DateUtil;
import dev.wixforth.columnar.storage.ColumnStats;
import dev.wixforth.columnar.storage.Database;
import dev.wixforth.columnar.storage.PartitionMetadata;
import dev.wixforth.columnar.storage.TableStorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Rule-based logical & physical query planner implementing:
 *   1. Conjunctive predicate pushdown to individual table scans
 *   2. Micro-partition zone-map pruning (min/max range checks before disk I/O)
 *   3. Column projection pruning (reading only referenced columns)
 *   4. Cost-aware build/probe orientation for 2-way joins
 */
public final class QueryPlanner {
    private QueryPlanner() {}

    public static LogicalPlan plan(SelectStatement stmt, Database db) {
        // 1. Register all table aliases
        Map<String, TableStorage> aliasToTable = new LinkedHashMap<>();
        List<String> aliasOrder = new ArrayList<>();

        String rootAlias = stmt.fromTable().effectiveAlias().toLowerCase(Locale.ROOT);
        aliasToTable.put(rootAlias, db.table(stmt.fromTable().tableName()));
        aliasOrder.add(rootAlias);

        for (SelectStatement.JoinClause jc : stmt.joins()) {
            String alias = jc.rightTable().effectiveAlias().toLowerCase(Locale.ROOT);
            if (aliasToTable.containsKey(alias)) {
                throw new IllegalArgumentException("Duplicate table alias: " + alias);
            }
            aliasToTable.put(alias, db.table(jc.rightTable().tableName()));
            aliasOrder.add(alias);
        }

        // 2. Collect referenced columns per table alias (Column Projection Pruning)
        Map<String, Set<Integer>> requiredCols = new LinkedHashMap<>();
        for (String alias : aliasOrder) {
            requiredCols.put(alias, new TreeSet<>());
        }

        boolean hasAgg = !stmt.groupBy().isEmpty();
        for (SelectStatement.SelectItem item : stmt.selectItems()) {
            if (containsAggregate(item.expr())) {
                hasAgg = true;
            }
            collectColumns(item.expr(), aliasToTable, requiredCols);
        }
        if (stmt.whereClause() != null) {
            collectColumns(stmt.whereClause(), aliasToTable, requiredCols);
        }
        for (SelectStatement.JoinClause jc : stmt.joins()) {
            collectColumnRef(jc.leftKey(), aliasToTable, requiredCols);
            collectColumnRef(jc.rightKey(), aliasToTable, requiredCols);
        }
        for (Expr.ColumnRef gb : stmt.groupBy()) {
            collectColumnRef(gb, aliasToTable, requiredCols);
        }
        // Check if ORDER BY references a column not already in SELECT
        Set<String> selectAliases = new HashSet<>();
        for (SelectStatement.SelectItem item : stmt.selectItems()) {
            if (item.alias() != null) {
                selectAliases.add(item.alias().toLowerCase(Locale.ROOT));
            }
        }
        for (SelectStatement.OrderByItem ob : stmt.orderBy()) {
            if (!selectAliases.contains(ob.columnOrAlias().toLowerCase(Locale.ROOT))) {
                Expr.ColumnRef ref = parseQualified(ob.columnOrAlias());
                collectColumnRef(ref, aliasToTable, requiredCols);
            }
        }
        // Ensure at least 1 column is projected if query is e.g. SELECT COUNT(*) FROM t
        for (String alias : aliasOrder) {
            if (requiredCols.get(alias).isEmpty()) {
                requiredCols.get(alias).add(0);
            }
        }

        // 3. Split WHERE into AND conjuncts and push single-table predicates down
        Map<String, List<Expr>> pushedPredicates = new LinkedHashMap<>();
        for (String alias : aliasOrder) {
            pushedPredicates.put(alias, new ArrayList<>());
        }
        List<Expr> residualPredicates = new ArrayList<>();
        if (stmt.whereClause() != null) {
            List<Expr> conjuncts = new ArrayList<>();
            splitConjuncts(stmt.whereClause(), conjuncts);
            for (Expr pred : conjuncts) {
                Set<String> touchedAliases = new LinkedHashSet<>();
                collectAliases(pred, aliasToTable, touchedAliases);
                if (touchedAliases.size() == 1) {
                    String target = touchedAliases.iterator().next();
                    pushedPredicates.get(target).add(pred);
                } else {
                    residualPredicates.add(pred);
                }
            }
        }

        // 4. Build TableScanPlan for each table with Zone-Map Micro-Partition Pruning
        Map<String, LogicalPlan.TableScanPlan> aliasToScan = new LinkedHashMap<>();
        for (String alias : aliasOrder) {
            TableStorage table = aliasToTable.get(alias);
            Set<Integer> colSet = requiredCols.get(alias);
            int[] projIndices = colSet.stream().mapToInt(Integer::intValue).toArray();
            Map<String, Integer> nameToProj = new HashMap<>();
            for (int i = 0; i < projIndices.length; i++) {
                String cname = table.schema().column(projIndices[i]).name().toLowerCase(Locale.ROOT);
                nameToProj.put(cname, i);
            }

            List<Expr> preds = pushedPredicates.get(alias);
            List<PartitionMetadata> surviving = new ArrayList<>();
            for (PartitionMetadata part : table.partitions()) {
                if (partitionMayMatch(part, table, preds)) {
                    surviving.add(part);
                }
            }
            int totalParts = table.partitionCount();
            int prunedParts = totalParts - surviving.size();

            aliasToScan.put(alias, new LogicalPlan.TableScanPlan(
                    alias, table, projIndices, nameToProj, preds, surviving, totalParts, prunedParts));
        }

        // 5. Build join pipeline (swap build/probe for 2-way joins so smaller surviving table is build side)
        LogicalPlan.TableScanPlan rootScan = aliasToScan.get(rootAlias);
        List<LogicalPlan.JoinStep> joinSteps = new ArrayList<>();
        Set<String> availableAliases = new HashSet<>();
        availableAliases.add(rootAlias);

        if (stmt.joins().size() == 1) {
            SelectStatement.JoinClause jc = stmt.joins().get(0);
            String rightAlias = jc.rightTable().effectiveAlias().toLowerCase(Locale.ROOT);
            LogicalPlan.TableScanPlan rightScan = aliasToScan.get(rightAlias);

            LogicalPlan.ResolvedColumn k1 = resolveColumn(jc.leftKey(), aliasToScan);
            LogicalPlan.ResolvedColumn k2 = resolveColumn(jc.rightKey(), aliasToScan);

            LogicalPlan.ResolvedColumn rootKey = k1.tableAlias().equals(rootAlias) ? k1 : k2;
            LogicalPlan.ResolvedColumn rightKey = k1.tableAlias().equals(rightAlias) ? k1 : k2;

            // Put smaller surviving table on build side, larger on parallel probe side
            if (rootScan.survivingRows() > rightScan.survivingRows()) {
                rootScan = rightScan;
                joinSteps.add(new LogicalPlan.JoinStep(aliasToScan.get(rootAlias), rightKey, rootKey));
            } else {
                joinSteps.add(new LogicalPlan.JoinStep(rightScan, rootKey, rightKey));
            }
        } else {
            for (SelectStatement.JoinClause jc : stmt.joins()) {
                String rightAlias = jc.rightTable().effectiveAlias().toLowerCase(Locale.ROOT);
                LogicalPlan.TableScanPlan rightScan = aliasToScan.get(rightAlias);

                LogicalPlan.ResolvedColumn k1 = resolveColumn(jc.leftKey(), aliasToScan);
                LogicalPlan.ResolvedColumn k2 = resolveColumn(jc.rightKey(), aliasToScan);

                LogicalPlan.ResolvedColumn buildKey;
                LogicalPlan.ResolvedColumn probeKey;
                if (availableAliases.contains(k1.tableAlias()) && k2.tableAlias().equals(rightAlias)) {
                    buildKey = k1;
                    probeKey = k2;
                } else if (availableAliases.contains(k2.tableAlias()) && k1.tableAlias().equals(rightAlias)) {
                    buildKey = k2;
                    probeKey = k1;
                } else {
                    throw new IllegalArgumentException(
                            "Join condition must link existing tables to " + rightAlias);
                }

                joinSteps.add(new LogicalPlan.JoinStep(rightScan, buildKey, probeKey));
                availableAliases.add(rightAlias);
            }
        }

        // 6. Resolve GROUP BY columns
        List<LogicalPlan.ResolvedColumn> groupByCols = new ArrayList<>();
        for (Expr.ColumnRef ref : stmt.groupBy()) {
            groupByCols.add(resolveColumn(ref, aliasToScan));
        }

        return new LogicalPlan(
                rootScan,
                joinSteps,
                residualPredicates,
                stmt.selectItems(),
                groupByCols,
                stmt.orderBy(),
                stmt.limit(),
                hasAgg,
                aliasToScan);
    }

    /**
     * Evaluates pushed conjunctive predicates against a partition's min/max zone maps.
     * Returns false ONLY when the partition is provably disjoint from the predicate.
     */
    public static boolean partitionMayMatch(
            PartitionMetadata part,
            TableStorage table,
            List<Expr> pushedPredicates) {
        for (Expr pred : pushedPredicates) {
            if (!predicateMayMatchPartition(pred, part, table)) {
                return false;
            }
        }
        return true;
    }

    private static boolean predicateMayMatchPartition(Expr pred, PartitionMetadata part, TableStorage table) {
        if (pred instanceof Expr.Between btw) {
            Expr ge = new Expr.BinaryOp(Expr.Op.GTE, btw.value(), btw.low());
            Expr le = new Expr.BinaryOp(Expr.Op.LTE, btw.value(), btw.high());
            return predicateMayMatchPartition(ge, part, table)
                    && predicateMayMatchPartition(le, part, table);
        }
        if (pred instanceof Expr.BinaryOp bin) {
            if (bin.op() == Expr.Op.AND) {
                return predicateMayMatchPartition(bin.left(), part, table)
                        && predicateMayMatchPartition(bin.right(), part, table);
            }
            if (bin.op() == Expr.Op.OR) {
                return predicateMayMatchPartition(bin.left(), part, table)
                        || predicateMayMatchPartition(bin.right(), part, table);
            }

            // Normalize ColumnRef <op> Literal
            Expr.ColumnRef colRef = null;
            Expr lit = null;
            Expr.Op op = bin.op();
            if (bin.left() instanceof Expr.ColumnRef c && isLiteral(bin.right())) {
                colRef = c;
                lit = bin.right();
            } else if (isLiteral(bin.left()) && bin.right() instanceof Expr.ColumnRef c) {
                colRef = c;
                lit = bin.left();
                op = flipComparison(op);
            }

            if (colRef != null && lit != null && op != null) {
                int colIdx = table.schema().indexOf(colRef.columnName());
                ColumnStats st = part.stats(colIdx);
                return checkZoneMap(st, op, lit);
            }
        }
        return true; // Conservative default for complex expressions
    }

    private static boolean checkZoneMap(ColumnStats st, Expr.Op op, Expr lit) {
        return switch (st.type) {
            case INT64 -> {
                long v = asLong(lit);
                yield switch (op) {
                    case EQ -> v >= st.minLong && v <= st.maxLong;
                    case NEQ -> !(st.minLong == v && st.maxLong == v);
                    case LT -> st.minLong < v;
                    case LTE -> st.minLong <= v;
                    case GT -> st.maxLong > v;
                    case GTE -> st.maxLong >= v;
                    default -> true;
                };
            }
            case DOUBLE -> {
                double v = asDouble(lit);
                yield switch (op) {
                    case EQ -> v >= st.minDouble && v <= st.maxDouble;
                    case NEQ -> !(st.minDouble == v && st.maxDouble == v);
                    case LT -> st.minDouble < v;
                    case LTE -> st.minDouble <= v;
                    case GT -> st.maxDouble > v;
                    case GTE -> st.maxDouble >= v;
                    default -> true;
                };
            }
            case DATE -> {
                int v = asEpochDay(lit);
                yield switch (op) {
                    case EQ -> v >= st.minInt && v <= st.maxInt;
                    case NEQ -> !(st.minInt == v && st.maxInt == v);
                    case LT -> st.minInt < v;
                    case LTE -> st.minInt <= v;
                    case GT -> st.maxInt > v;
                    case GTE -> st.maxInt >= v;
                    default -> true;
                };
            }
            case STRING -> {
                if (!(lit instanceof Expr.StringLiteral sl)) yield true;
                String v = sl.value();
                yield switch (op) {
                    case EQ -> v.compareTo(st.minString) >= 0 && v.compareTo(st.maxString) <= 0;
                    case NEQ -> !(st.minString.equals(v) && st.maxString.equals(v));
                    case LT -> st.minString.compareTo(v) < 0;
                    case LTE -> st.minString.compareTo(v) <= 0;
                    case GT -> st.maxString.compareTo(v) > 0;
                    case GTE -> st.maxString.compareTo(v) >= 0;
                    default -> true;
                };
            }
        };
    }

    private static boolean isLiteral(Expr e) {
        return e instanceof Expr.LongLiteral
                || e instanceof Expr.DoubleLiteral
                || e instanceof Expr.StringLiteral
                || (e instanceof Expr.UnaryMinus u
                        && (u.operand() instanceof Expr.LongLiteral || u.operand() instanceof Expr.DoubleLiteral));
    }

    private static long asLong(Expr lit) {
        if (lit instanceof Expr.LongLiteral l) return l.value();
        if (lit instanceof Expr.DoubleLiteral d) return (long) d.value();
        if (lit instanceof Expr.UnaryMinus u) return -asLong(u.operand());
        throw new IllegalArgumentException("Expected numeric literal, got: " + lit);
    }

    private static double asDouble(Expr lit) {
        if (lit instanceof Expr.DoubleLiteral d) return d.value();
        if (lit instanceof Expr.LongLiteral l) return (double) l.value();
        if (lit instanceof Expr.UnaryMinus u) return -asDouble(u.operand());
        throw new IllegalArgumentException("Expected numeric literal, got: " + lit);
    }

    private static int asEpochDay(Expr lit) {
        if (lit instanceof Expr.StringLiteral s) return DateUtil.parseEpochDay(s.value());
        if (lit instanceof Expr.LongLiteral l) return (int) l.value();
        throw new IllegalArgumentException("Expected ISO date string literal, got: " + lit);
    }

    private static Expr.Op flipComparison(Expr.Op op) {
        return switch (op) {
            case EQ -> Expr.Op.EQ;
            case NEQ -> Expr.Op.NEQ;
            case LT -> Expr.Op.GT;
            case LTE -> Expr.Op.GTE;
            case GT -> Expr.Op.LT;
            case GTE -> Expr.Op.LTE;
            default -> null;
        };
    }

    public static LogicalPlan.ResolvedColumn resolveColumn(
            Expr.ColumnRef ref,
            Map<String, LogicalPlan.TableScanPlan> aliasToScan) {
        String alias = resolveAlias(ref, aliasToScan);
        LogicalPlan.TableScanPlan scan = aliasToScan.get(alias);
        int tableColIdx = scan.table().schema().indexOf(ref.columnName());
        Integer projIdx = scan.colNameToProjectedIndex().get(ref.columnName().toLowerCase(Locale.ROOT));
        if (projIdx == null) {
            throw new IllegalStateException("Column not projected: " + ref.qualifiedName());
        }
        ColumnType type = scan.table().schema().column(tableColIdx).type();
        return new LogicalPlan.ResolvedColumn(alias, ref.columnName().toLowerCase(Locale.ROOT), tableColIdx, projIdx, type);
    }

    private static String resolveAlias(Expr.ColumnRef ref, Map<String, LogicalPlan.TableScanPlan> aliasToScan) {
        if (ref.tableAlias() != null) {
            String key = ref.tableAlias().toLowerCase(Locale.ROOT);
            if (!aliasToScan.containsKey(key)) {
                throw new IllegalArgumentException("Unknown table alias: " + ref.tableAlias());
            }
            return key;
        }
        String matched = null;
        for (var entry : aliasToScan.entrySet()) {
            if (entry.getValue().table().schema().hasColumn(ref.columnName())) {
                if (matched != null) {
                    throw new IllegalArgumentException("Ambiguous column reference: " + ref.columnName());
                }
                matched = entry.getKey();
            }
        }
        if (matched == null) {
            throw new IllegalArgumentException("Unknown column: " + ref.columnName());
        }
        return matched;
    }

    private static String resolveAliasFromTables(Expr.ColumnRef ref, Map<String, TableStorage> aliasToTable) {
        if (ref.tableAlias() != null) {
            String key = ref.tableAlias().toLowerCase(Locale.ROOT);
            if (!aliasToTable.containsKey(key)) {
                throw new IllegalArgumentException("Unknown table alias: " + ref.tableAlias());
            }
            return key;
        }
        String matched = null;
        for (var entry : aliasToTable.entrySet()) {
            if (entry.getValue().schema().hasColumn(ref.columnName())) {
                if (matched != null) {
                    throw new IllegalArgumentException("Ambiguous column reference: " + ref.columnName());
                }
                matched = entry.getKey();
            }
        }
        if (matched == null) {
            throw new IllegalArgumentException("Unknown column: " + ref.columnName());
        }
        return matched;
    }

    private static void collectColumns(
            Expr expr,
            Map<String, TableStorage> aliasToTable,
            Map<String, Set<Integer>> requiredCols) {
        if (expr instanceof Expr.ColumnRef ref) {
            collectColumnRef(ref, aliasToTable, requiredCols);
        } else if (expr instanceof Expr.BinaryOp bin) {
            collectColumns(bin.left(), aliasToTable, requiredCols);
            collectColumns(bin.right(), aliasToTable, requiredCols);
        } else if (expr instanceof Expr.UnaryMinus u) {
            collectColumns(u.operand(), aliasToTable, requiredCols);
        } else if (expr instanceof Expr.Between btw) {
            collectColumns(btw.value(), aliasToTable, requiredCols);
            collectColumns(btw.low(), aliasToTable, requiredCols);
            collectColumns(btw.high(), aliasToTable, requiredCols);
        } else if (expr instanceof Expr.AggregateCall agg) {
            collectColumns(agg.argument(), aliasToTable, requiredCols);
        }
    }

    private static void collectColumnRef(
            Expr.ColumnRef ref,
            Map<String, TableStorage> aliasToTable,
            Map<String, Set<Integer>> requiredCols) {
        String alias = resolveAliasFromTables(ref, aliasToTable);
        int idx = aliasToTable.get(alias).schema().indexOf(ref.columnName());
        requiredCols.get(alias).add(idx);
    }

    private static void collectAliases(
            Expr expr,
            Map<String, TableStorage> aliasToTable,
            Set<String> out) {
        if (expr instanceof Expr.ColumnRef ref) {
            out.add(resolveAliasFromTables(ref, aliasToTable));
        } else if (expr instanceof Expr.BinaryOp bin) {
            collectAliases(bin.left(), aliasToTable, out);
            collectAliases(bin.right(), aliasToTable, out);
        } else if (expr instanceof Expr.UnaryMinus u) {
            collectAliases(u.operand(), aliasToTable, out);
        } else if (expr instanceof Expr.Between btw) {
            collectAliases(btw.value(), aliasToTable, out);
            collectAliases(btw.low(), aliasToTable, out);
            collectAliases(btw.high(), aliasToTable, out);
        } else if (expr instanceof Expr.AggregateCall agg) {
            collectAliases(agg.argument(), aliasToTable, out);
        }
    }

    private static void splitConjuncts(Expr expr, List<Expr> out) {
        if (expr instanceof Expr.BinaryOp bin && bin.op() == Expr.Op.AND) {
            splitConjuncts(bin.left(), out);
            splitConjuncts(bin.right(), out);
        } else if (expr instanceof Expr.Between btw) {
            out.add(new Expr.BinaryOp(Expr.Op.GTE, btw.value(), btw.low()));
            out.add(new Expr.BinaryOp(Expr.Op.LTE, btw.value(), btw.high()));
        } else {
            out.add(expr);
        }
    }

    private static boolean containsAggregate(Expr expr) {
        if (expr instanceof Expr.AggregateCall) return true;
        if (expr instanceof Expr.BinaryOp b) return containsAggregate(b.left()) || containsAggregate(b.right());
        if (expr instanceof Expr.UnaryMinus u) return containsAggregate(u.operand());
        return false;
    }

    private static Expr.ColumnRef parseQualified(String name) {
        int dot = name.indexOf('.');
        if (dot >= 0) {
            return new Expr.ColumnRef(name.substring(0, dot), name.substring(dot + 1));
        }
        return new Expr.ColumnRef(null, name);
    }
}
