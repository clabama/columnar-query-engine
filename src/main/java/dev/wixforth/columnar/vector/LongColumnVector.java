package dev.wixforth.columnar.vector;

import dev.wixforth.columnar.schema.ColumnType;

/**
 * Primitive long[] column vector supporting zero-copy sub-range views.
 */
public final class LongColumnVector implements ColumnVector {
    public final long[] data;
    public final int offset;
    public final int size;

    public LongColumnVector(long[] data, int size) {
        this(data, 0, size);
    }

    public LongColumnVector(long[] data, int offset, int size) {
        this.data = data;
        this.offset = offset;
        this.size = size;
    }

    public long get(int row) {
        return data[offset + row];
    }

    @Override
    public ColumnType type() {
        return ColumnType.INT64;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long estimatedBytes() {
        return 32L + (long) data.length * Long.BYTES;
    }

    @Override
    public LongColumnVector slice(int sliceOffset, int sliceLength) {
        return new LongColumnVector(data, this.offset + sliceOffset, sliceLength);
    }

    @Override
    public Object getBoxed(int row) {
        return data[offset + row];
    }
}
