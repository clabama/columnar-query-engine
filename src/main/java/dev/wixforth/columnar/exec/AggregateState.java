package dev.wixforth.columnar.exec;

import dev.wixforth.columnar.sql.Expr;

import java.util.Arrays;

/**
 * Associative per-group aggregate accumulator state supporting lock-free thread-local
 * accumulation followed by exact parallel merge (including AVG via sum + count).
 */
public final class AggregateState {
    public final double[] sums;
    public final long[] counts;
    public final double[] mins;
    public final double[] maxs;

    public AggregateState(int aggCount) {
        this.sums = new double[aggCount];
        this.counts = new long[aggCount];
        this.mins = new double[aggCount];
        this.maxs = new double[aggCount];
        Arrays.fill(this.mins, Double.POSITIVE_INFINITY);
        Arrays.fill(this.maxs, Double.NEGATIVE_INFINITY);
    }

    public void accumulate(int idx, Expr.AggFunc func, double val) {
        switch (func) {
            case SUM -> sums[idx] += val;
            case COUNT -> counts[idx]++;
            case AVG -> {
                sums[idx] += val;
                counts[idx]++;
            }
            case MIN -> {
                if (val < mins[idx]) mins[idx] = val;
            }
            case MAX -> {
                if (val > maxs[idx]) maxs[idx] = val;
            }
        }
    }

    public void mergeFrom(AggregateState other, Expr.AggFunc[] funcs) {
        for (int i = 0; i < funcs.length; i++) {
            switch (funcs[i]) {
                case SUM -> sums[i] += other.sums[i];
                case COUNT -> counts[i] += other.counts[i];
                case AVG -> {
                    sums[i] += other.sums[i];
                    counts[i] += other.counts[i];
                }
                case MIN -> {
                    if (other.mins[i] < mins[i]) mins[i] = other.mins[i];
                }
                case MAX -> {
                    if (other.maxs[i] > maxs[i]) maxs[i] = other.maxs[i];
                }
            }
        }
    }

    public Object finalizeValue(int idx, Expr.AggFunc func) {
        return switch (func) {
            case SUM -> Double.valueOf(sums[idx]);
            case COUNT -> Long.valueOf(counts[idx]);
            case AVG -> Double.valueOf(counts[idx] == 0 ? 0.0 : sums[idx] / counts[idx]);
            case MIN -> mins[idx] == Double.POSITIVE_INFINITY ? null : Double.valueOf(mins[idx]);
            case MAX -> maxs[idx] == Double.NEGATIVE_INFINITY ? null : Double.valueOf(maxs[idx]);
        };
    }

    /**
     * Compact value-typed group key for HashAggregate maps.
     */
    public static final class GroupKey {
        public final Object[] values;
        private final int hash;

        public GroupKey(Object[] values) {
            this.values = values;
            this.hash = Arrays.hashCode(values);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof GroupKey other)) return false;
            return hash == other.hash && Arrays.equals(values, other.values);
        }
    }
}
