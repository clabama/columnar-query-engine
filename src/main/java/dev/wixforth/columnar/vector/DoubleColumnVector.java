package dev.wixforth.columnar.vector;

import dev.wixforth.columnar.schema.ColumnType;

/**
 * Primitive double[] column vector supporting zero-copy sub-range views.
 */
public final class DoubleColumnVector implements ColumnVector {
    public final double[] data;
    public final int offset;
    public final int size;

    public DoubleColumnVector(double[] data, int size) {
        this(data, 0, size);
    }

    public DoubleColumnVector(double[] data, int offset, int size) {
        this.data = data;
        this.offset = offset;
        this.size = size;
    }

    public double get(int row) {
        return data[offset + row];
    }

    @Override
    public ColumnType type() {
        return ColumnType.DOUBLE;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long estimatedBytes() {
        return 32L + (long) data.length * Double.BYTES;
    }

    @Override
    public DoubleColumnVector slice(int sliceOffset, int sliceLength) {
        return new DoubleColumnVector(data, this.offset + sliceOffset, sliceLength);
    }

    @Override
    public Object getBoxed(int row) {
        return data[offset + row];
    }
}
