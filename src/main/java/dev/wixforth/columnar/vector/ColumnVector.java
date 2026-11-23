package dev.wixforth.columnar.vector;

import dev.wixforth.columnar.schema.ColumnType;

/**
 * Sealed hierarchy of primitive-backed column vectors used throughout vectorized execution.
 * Avoids per-row object boxing and supports zero-copy slicing into cache-sized batches.
 */
public sealed interface ColumnVector
        permits LongColumnVector, DoubleColumnVector, IntColumnVector, StringColumnVector {

    ColumnType type();

    int size();

    /** Estimated heap footprint in bytes (used for bounded LRU cache eviction). */
    long estimatedBytes();

    /** Returns a zero-copy view over [offset, offset + length). */
    ColumnVector slice(int offset, int length);

    /** Materializes a single boxed value (only used for final result formatting / display). */
    Object getBoxed(int row);
}
