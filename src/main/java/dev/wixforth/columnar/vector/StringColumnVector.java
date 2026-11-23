package dev.wixforth.columnar.vector;

import dev.wixforth.columnar.schema.ColumnType;

/**
 * Dictionary-encoded string column vector.
 * Stores unique values in {@code dictionary} and per-row integer codes in {@code codes},
 * enabling fast integer equality checks during filtering and grouping.
 */
public final class StringColumnVector implements ColumnVector {
    public final String[] dictionary;
    public final int[] codes;
    public final int offset;
    public final int size;

    public StringColumnVector(String[] dictionary, int[] codes, int size) {
        this(dictionary, codes, 0, size);
    }

    public StringColumnVector(String[] dictionary, int[] codes, int offset, int size) {
        this.dictionary = dictionary;
        this.codes = codes;
        this.offset = offset;
        this.size = size;
    }

    public int getCode(int row) {
        return codes[offset + row];
    }

    public String get(int row) {
        return dictionary[codes[offset + row]];
    }

    /** Returns the dictionary code for target string, or -1 if absent in this partition. */
    public int findCode(String target) {
        for (int i = 0; i < dictionary.length; i++) {
            if (dictionary[i].equals(target)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public ColumnType type() {
        return ColumnType.STRING;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long estimatedBytes() {
        long dictBytes = 32L;
        for (String s : dictionary) {
            dictBytes += 40L + (long) s.length() * 2L;
        }
        return dictBytes + 32L + (long) codes.length * Integer.BYTES;
    }

    @Override
    public StringColumnVector slice(int sliceOffset, int sliceLength) {
        return new StringColumnVector(dictionary, codes, this.offset + sliceOffset, sliceLength);
    }

    @Override
    public Object getBoxed(int row) {
        return get(row);
    }
}
