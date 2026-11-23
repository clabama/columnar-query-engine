package dev.wixforth.columnar.vector;

import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.DateUtil;

/**
 * Primitive int[] column vector storing DATE values as epoch days.
 */
public final class IntColumnVector implements ColumnVector {
    public final int[] data;
    public final int offset;
    public final int size;

    public IntColumnVector(int[] data, int size) {
        this(data, 0, size);
    }

    public IntColumnVector(int[] data, int offset, int size) {
        this.data = data;
        this.offset = offset;
        this.size = size;
    }

    public int get(int row) {
        return data[offset + row];
    }

    @Override
    public ColumnType type() {
        return ColumnType.DATE;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long estimatedBytes() {
        return 32L + (long) data.length * Integer.BYTES;
    }

    @Override
    public IntColumnVector slice(int sliceOffset, int sliceLength) {
        return new IntColumnVector(data, this.offset + sliceOffset, sliceLength);
    }

    @Override
    public Object getBoxed(int row) {
        return DateUtil.formatEpochDay(data[offset + row]);
    }
}
