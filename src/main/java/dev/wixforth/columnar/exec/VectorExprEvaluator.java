package dev.wixforth.columnar.exec;

import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.DateUtil;
import dev.wixforth.columnar.sql.Expr;
import dev.wixforth.columnar.sql.LogicalPlan;
import dev.wixforth.columnar.vector.ColumnVector;
import dev.wixforth.columnar.vector.DoubleColumnVector;
import dev.wixforth.columnar.vector.IntColumnVector;
import dev.wixforth.columnar.vector.LongColumnVector;
import dev.wixforth.columnar.vector.StringColumnVector;
import dev.wixforth.columnar.vector.VectorBatch;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Vectorized expression and predicate evaluator operating directly on primitive arrays
 * and selection vectors (no per-row boxing).
 */
public final class VectorExprEvaluator {
    private VectorExprEvaluator() {}

    /**
     * Applies a list of conjunctive predicates to a single-table VectorBatch in place,
     * updating its selection vector.
     */
    public static void filterBatch(
            VectorBatch batch,
            List<Expr> predicates,
            Map<String, Integer> colNameToProjIdx) {
        if (predicates.isEmpty() || batch.isEmpty()) {
            return;
        }
        int[] sel = new int[batch.rowCount];
        for (Expr pred : predicates) {
            int outCount = evaluatePredicate(batch, pred, colNameToProjIdx, sel);
            int[] nextSel = new int[outCount];
            System.arraycopy(sel, 0, nextSel, 0, outCount);
            batch.applySelection(nextSel, outCount);
            if (outCount == 0) {
                return;
            }
        }
    }

    private static int evaluatePredicate(
            VectorBatch batch,
            Expr pred,
            Map<String, Integer> colMap,
            int[] outSel) {
        if (pred instanceof Expr.Between btw) {
            Expr ge = new Expr.BinaryOp(Expr.Op.GTE, btw.value(), btw.low());
            Expr le = new Expr.BinaryOp(Expr.Op.LTE, btw.value(), btw.high());
            int[] tmp = new int[batch.selectedCount];
            int midCount = evaluatePredicate(batch, ge, colMap, tmp);
            VectorBatch sub = new VectorBatch(batch.columns, batch.rowCount);
            sub.applySelection(tmp, midCount);
            return evaluatePredicate(sub, le, colMap, outSel);
        }

        if (pred instanceof Expr.BinaryOp bin) {
            if (bin.op() == Expr.Op.AND) {
                int[] tmp = new int[batch.selectedCount];
                int midCount = evaluatePredicate(batch, bin.left(), colMap, tmp);
                if (midCount == 0) return 0;
                VectorBatch sub = new VectorBatch(batch.columns, batch.rowCount);
                sub.applySelection(tmp, midCount);
                return evaluatePredicate(sub, bin.right(), colMap, outSel);
            }
            if (bin.op() == Expr.Op.OR) {
                boolean[] mask = new boolean[batch.rowCount];
                int[] leftSel = new int[batch.selectedCount];
                int lc = evaluatePredicate(batch, bin.left(), colMap, leftSel);
                for (int i = 0; i < lc; i++) mask[leftSel[i]] = true;

                int[] rightSel = new int[batch.selectedCount];
                int rc = evaluatePredicate(batch, bin.right(), colMap, rightSel);
                for (int i = 0; i < rc; i++) mask[rightSel[i]] = true;

                int count = 0;
                if (batch.hasSelection) {
                    for (int i = 0; i < batch.selectedCount; i++) {
                        int r = batch.selection[i];
                        if (mask[r]) outSel[count++] = r;
                    }
                } else {
                    for (int r = 0; r < batch.rowCount; r++) {
                        if (mask[r]) outSel[count++] = r;
                    }
                }
                return count;
            }

            // Fast path: ColumnRef <op> Literal
            Expr.ColumnRef colRef = null;
            Expr lit = null;
            Expr.Op op = bin.op();
            if (bin.left() instanceof Expr.ColumnRef c && isLiteral(bin.right())) {
                colRef = c;
                lit = bin.right();
            } else if (isLiteral(bin.left()) && bin.right() instanceof Expr.ColumnRef c) {
                colRef = c;
                lit = bin.left();
                op = flip(op);
            }

            if (colRef != null && lit != null && op != null) {
                int projIdx = colMap.get(colRef.columnName().toLowerCase(Locale.ROOT));
                ColumnVector vec = batch.columns[projIdx];
                return evalColVsLiteral(batch, vec, op, lit, outSel);
            }

            // General numeric expression comparison
            double[] lhs = evalNumericBatch(batch, bin.left(), colMap);
            double[] rhs = evalNumericBatch(batch, bin.right(), colMap);
            int count = 0;
            int n = batch.selectedCount;
            if (batch.hasSelection) {
                int[] inSel = batch.selection;
                for (int i = 0; i < n; i++) {
                    if (compareDouble(lhs[i], rhs[i], op)) {
                        outSel[count++] = inSel[i];
                    }
                }
            } else {
                for (int i = 0; i < n; i++) {
                    if (compareDouble(lhs[i], rhs[i], op)) {
                        outSel[count++] = i;
                    }
                }
            }
            return count;
        }
        throw new IllegalArgumentException("Unsupported predicate expression: " + pred);
    }

    private static int evalColVsLiteral(
            VectorBatch batch,
            ColumnVector vec,
            Expr.Op op,
            Expr lit,
            int[] outSel) {
        int count = 0;
        boolean hasSel = batch.hasSelection;
        int[] inSel = batch.selection;
        int n = batch.selectedCount;

        if (vec instanceof LongColumnVector lv) {
            long[] data = lv.data;
            int off = lv.offset;
            long rhs = asLong(lit);
            if (!hasSel) {
                switch (op) {
                    case EQ -> { for (int i = 0; i < n; i++) if (data[off + i] == rhs) outSel[count++] = i; }
                    case NEQ -> { for (int i = 0; i < n; i++) if (data[off + i] != rhs) outSel[count++] = i; }
                    case LT -> { for (int i = 0; i < n; i++) if (data[off + i] < rhs) outSel[count++] = i; }
                    case LTE -> { for (int i = 0; i < n; i++) if (data[off + i] <= rhs) outSel[count++] = i; }
                    case GT -> { for (int i = 0; i < n; i++) if (data[off + i] > rhs) outSel[count++] = i; }
                    case GTE -> { for (int i = 0; i < n; i++) if (data[off + i] >= rhs) outSel[count++] = i; }
                    default -> throw new IllegalArgumentException("Invalid op: " + op);
                }
            } else {
                for (int i = 0; i < n; i++) {
                    int r = inSel[i];
                    long v = data[off + r];
                    boolean ok = switch (op) {
                        case EQ -> v == rhs;
                        case NEQ -> v != rhs;
                        case LT -> v < rhs;
                        case LTE -> v <= rhs;
                        case GT -> v > rhs;
                        case GTE -> v >= rhs;
                        default -> false;
                    };
                    if (ok) outSel[count++] = r;
                }
            }
            return count;
        }

        if (vec instanceof DoubleColumnVector dv) {
            double[] data = dv.data;
            int off = dv.offset;
            double rhs = asDouble(lit);
            if (!hasSel) {
                switch (op) {
                    case EQ -> { for (int i = 0; i < n; i++) if (data[off + i] == rhs) outSel[count++] = i; }
                    case NEQ -> { for (int i = 0; i < n; i++) if (data[off + i] != rhs) outSel[count++] = i; }
                    case LT -> { for (int i = 0; i < n; i++) if (data[off + i] < rhs) outSel[count++] = i; }
                    case LTE -> { for (int i = 0; i < n; i++) if (data[off + i] <= rhs) outSel[count++] = i; }
                    case GT -> { for (int i = 0; i < n; i++) if (data[off + i] > rhs) outSel[count++] = i; }
                    case GTE -> { for (int i = 0; i < n; i++) if (data[off + i] >= rhs) outSel[count++] = i; }
                    default -> throw new IllegalArgumentException("Invalid op: " + op);
                }
            } else {
                for (int i = 0; i < n; i++) {
                    int r = inSel[i];
                    if (compareDouble(data[off + r], rhs, op)) {
                        outSel[count++] = r;
                    }
                }
            }
            return count;
        }

        if (vec instanceof IntColumnVector iv) {
            int[] data = iv.data;
            int off = iv.offset;
            int rhs = asEpochDay(lit);
            if (!hasSel) {
                switch (op) {
                    case EQ -> { for (int i = 0; i < n; i++) if (data[off + i] == rhs) outSel[count++] = i; }
                    case NEQ -> { for (int i = 0; i < n; i++) if (data[off + i] != rhs) outSel[count++] = i; }
                    case LT -> { for (int i = 0; i < n; i++) if (data[off + i] < rhs) outSel[count++] = i; }
                    case LTE -> { for (int i = 0; i < n; i++) if (data[off + i] <= rhs) outSel[count++] = i; }
                    case GT -> { for (int i = 0; i < n; i++) if (data[off + i] > rhs) outSel[count++] = i; }
                    case GTE -> { for (int i = 0; i < n; i++) if (data[off + i] >= rhs) outSel[count++] = i; }
                    default -> throw new IllegalArgumentException("Invalid op: " + op);
                }
            } else {
                for (int i = 0; i < n; i++) {
                    int r = inSel[i];
                    int v = data[off + r];
                    boolean ok = switch (op) {
                        case EQ -> v == rhs;
                        case NEQ -> v != rhs;
                        case LT -> v < rhs;
                        case LTE -> v <= rhs;
                        case GT -> v > rhs;
                        case GTE -> v >= rhs;
                        default -> false;
                    };
                    if (ok) outSel[count++] = r;
                }
            }
            return count;
        }

        if (vec instanceof StringColumnVector sv) {
            String rhs = ((Expr.StringLiteral) lit).value();
            // Fast dictionary-code equality check!
            if (op == Expr.Op.EQ || op == Expr.Op.NEQ) {
                int targetCode = sv.findCode(rhs);
                int[] codes = sv.codes;
                int off = sv.offset;
                if (op == Expr.Op.EQ) {
                    if (targetCode < 0) return 0; // Value absent in entire partition!
                    if (!hasSel) {
                        for (int i = 0; i < n; i++) if (codes[off + i] == targetCode) outSel[count++] = i;
                    } else {
                        for (int i = 0; i < n; i++) {
                            int r = inSel[i];
                            if (codes[off + r] == targetCode) outSel[count++] = r;
                        }
                    }
                } else {
                    if (!hasSel) {
                        for (int i = 0; i < n; i++) if (codes[off + i] != targetCode) outSel[count++] = i;
                    } else {
                        for (int i = 0; i < n; i++) {
                            int r = inSel[i];
                            if (codes[off + r] != targetCode) outSel[count++] = r;
                        }
                    }
                }
                return count;
            }
            // Dictionary-level pre-evaluation for range comparisons
            boolean[] dictMatch = new boolean[sv.dictionary.length];
            for (int d = 0; d < sv.dictionary.length; d++) {
                int cmp = sv.dictionary[d].compareTo(rhs);
                dictMatch[d] = switch (op) {
                    case LT -> cmp < 0;
                    case LTE -> cmp <= 0;
                    case GT -> cmp > 0;
                    case GTE -> cmp >= 0;
                    default -> false;
                };
            }
            int[] codes = sv.codes;
            int off = sv.offset;
            if (!hasSel) {
                for (int i = 0; i < n; i++) if (dictMatch[codes[off + i]]) outSel[count++] = i;
            } else {
                for (int i = 0; i < n; i++) {
                    int r = inSel[i];
                    if (dictMatch[codes[off + r]]) outSel[count++] = r;
                }
            }
            return count;
        }
        throw new IllegalStateException("Unsupported vector type: " + vec.getClass());
    }

    /**
     * Evaluates an arithmetic expression into a compact double[batch.selectedCount] array.
     */
    public static double[] evalNumericBatch(
            VectorBatch batch,
            Expr expr,
            Map<String, Integer> colMap) {
        int n = batch.selectedCount;
        double[] out = new double[n];
        evalNumericInto(batch, expr, colMap, out);
        return out;
    }

    private static void evalNumericInto(
            VectorBatch batch,
            Expr expr,
            Map<String, Integer> colMap,
            double[] out) {
        int n = batch.selectedCount;
        if (expr instanceof Expr.DoubleLiteral dl) {
            java.util.Arrays.fill(out, 0, n, dl.value());
            return;
        }
        if (expr instanceof Expr.LongLiteral ll) {
            java.util.Arrays.fill(out, 0, n, (double) ll.value());
            return;
        }
        if (expr instanceof Expr.UnaryMinus u) {
            evalNumericInto(batch, u.operand(), colMap, out);
            for (int i = 0; i < n; i++) out[i] = -out[i];
            return;
        }
        if (expr instanceof Expr.ColumnRef ref) {
            int projIdx = colMap.get(ref.columnName().toLowerCase(Locale.ROOT));
            ColumnVector vec = batch.columns[projIdx];
            boolean hasSel = batch.hasSelection;
            int[] sel = batch.selection;
            if (vec instanceof DoubleColumnVector dv) {
                double[] d = dv.data;
                int off = dv.offset;
                if (!hasSel) {
                    System.arraycopy(d, off, out, 0, n);
                } else {
                    for (int i = 0; i < n; i++) out[i] = d[off + sel[i]];
                }
            } else if (vec instanceof LongColumnVector lv) {
                long[] d = lv.data;
                int off = lv.offset;
                if (!hasSel) {
                    for (int i = 0; i < n; i++) out[i] = d[off + i];
                } else {
                    for (int i = 0; i < n; i++) out[i] = d[off + sel[i]];
                }
            } else if (vec instanceof IntColumnVector iv) {
                int[] d = iv.data;
                int off = iv.offset;
                if (!hasSel) {
                    for (int i = 0; i < n; i++) out[i] = d[off + i];
                } else {
                    for (int i = 0; i < n; i++) out[i] = d[off + sel[i]];
                }
            } else {
                throw new IllegalArgumentException("Non-numeric column in arithmetic expression: " + ref.columnName());
            }
            return;
        }
        if (expr instanceof Expr.BinaryOp bin) {
            double[] left = new double[n];
            double[] right = new double[n];
            evalNumericInto(batch, bin.left(), colMap, left);
            evalNumericInto(batch, bin.right(), colMap, right);
            switch (bin.op()) {
                case ADD -> { for (int i = 0; i < n; i++) out[i] = left[i] + right[i]; }
                case SUB -> { for (int i = 0; i < n; i++) out[i] = left[i] - right[i]; }
                case MUL -> { for (int i = 0; i < n; i++) out[i] = left[i] * right[i]; }
                case DIV -> { for (int i = 0; i < n; i++) out[i] = left[i] / right[i]; }
                default -> throw new IllegalArgumentException("Unsupported arithmetic op: " + bin.op());
            }
            return;
        }
        throw new IllegalArgumentException("Unsupported numeric expression: " + expr);
    }

    /**
     * Evaluates a numeric expression for a single joined row using column readers.
     */
    public static double evalNumericScalar(
            Expr expr,
            Map<String, LogicalPlan.TableScanPlan> aliasToScan,
            RowAccessor accessor) {
        if (expr instanceof Expr.DoubleLiteral dl) return dl.value();
        if (expr instanceof Expr.LongLiteral ll) return (double) ll.value();
        if (expr instanceof Expr.UnaryMinus u) return -evalNumericScalar(u.operand(), aliasToScan, accessor);
        if (expr instanceof Expr.ColumnRef ref) {
            LogicalPlan.ResolvedColumn rc = dev.wixforth.columnar.sql.QueryPlanner.resolveColumn(ref, aliasToScan);
            return accessor.getDouble(rc);
        }
        if (expr instanceof Expr.BinaryOp bin) {
            double l = evalNumericScalar(bin.left(), aliasToScan, accessor);
            double r = evalNumericScalar(bin.right(), aliasToScan, accessor);
            return switch (bin.op()) {
                case ADD -> l + r;
                case SUB -> l - r;
                case MUL -> l * r;
                case DIV -> l / r;
                default -> throw new IllegalArgumentException("Unsupported arithmetic op: " + bin.op());
            };
        }
        throw new IllegalArgumentException("Unsupported scalar expression: " + expr);
    }

    public static boolean evalPredicateScalar(
            Expr expr,
            Map<String, LogicalPlan.TableScanPlan> aliasToScan,
            RowAccessor accessor) {
        if (expr instanceof Expr.Between btw) {
            return evalPredicateScalar(new Expr.BinaryOp(Expr.Op.GTE, btw.value(), btw.low()), aliasToScan, accessor)
                    && evalPredicateScalar(new Expr.BinaryOp(Expr.Op.LTE, btw.value(), btw.high()), aliasToScan, accessor);
        }
        if (expr instanceof Expr.BinaryOp bin) {
            if (bin.op() == Expr.Op.AND) {
                return evalPredicateScalar(bin.left(), aliasToScan, accessor)
                        && evalPredicateScalar(bin.right(), aliasToScan, accessor);
            }
            if (bin.op() == Expr.Op.OR) {
                return evalPredicateScalar(bin.left(), aliasToScan, accessor)
                        || evalPredicateScalar(bin.right(), aliasToScan, accessor);
            }
            // String or Date comparison if ColumnRef vs StringLiteral
            if (bin.left() instanceof Expr.ColumnRef ref && bin.right() instanceof Expr.StringLiteral sl) {
                LogicalPlan.ResolvedColumn rc = dev.wixforth.columnar.sql.QueryPlanner.resolveColumn(ref, aliasToScan);
                if (rc.type() == ColumnType.STRING) {
                    int cmp = accessor.getString(rc).compareTo(sl.value());
                    return compareInt(cmp, 0, bin.op());
                } else if (rc.type() == ColumnType.DATE) {
                    int lhs = accessor.getDate(rc);
                    int rhs = DateUtil.parseEpochDay(sl.value());
                    return compareInt(lhs, rhs, bin.op());
                }
            }
            double l = evalNumericScalar(bin.left(), aliasToScan, accessor);
            double r = evalNumericScalar(bin.right(), aliasToScan, accessor);
            return compareDouble(l, r, bin.op());
        }
        throw new IllegalArgumentException("Unsupported scalar predicate: " + expr);
    }

    public interface RowAccessor {
        long getLong(LogicalPlan.ResolvedColumn col);
        double getDouble(LogicalPlan.ResolvedColumn col);
        int getDate(LogicalPlan.ResolvedColumn col);
        String getString(LogicalPlan.ResolvedColumn col);
        Object getBoxed(LogicalPlan.ResolvedColumn col);
    }

    private static boolean compareDouble(double a, double b, Expr.Op op) {
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

    private static boolean compareInt(int a, int b, Expr.Op op) {
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
        throw new IllegalArgumentException("Expected long literal: " + lit);
    }

    private static double asDouble(Expr lit) {
        if (lit instanceof Expr.DoubleLiteral d) return d.value();
        if (lit instanceof Expr.LongLiteral l) return (double) l.value();
        if (lit instanceof Expr.UnaryMinus u) return -asDouble(u.operand());
        throw new IllegalArgumentException("Expected double literal: " + lit);
    }

    private static int asEpochDay(Expr lit) {
        if (lit instanceof Expr.StringLiteral s) return DateUtil.parseEpochDay(s.value());
        if (lit instanceof Expr.LongLiteral l) return (int) l.value();
        throw new IllegalArgumentException("Expected date literal: " + lit);
    }

    private static Expr.Op flip(Expr.Op op) {
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
}
