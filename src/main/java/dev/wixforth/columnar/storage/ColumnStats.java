package dev.wixforth.columnar.storage;

import dev.wixforth.columnar.schema.ColumnType;

/**
 * Per-column zone map (min/max metadata) and physical byte range inside a micro-partition.
 * Enables micro-partition pruning before opening files and column projection pruning during reads.
 */
public final class ColumnStats {
    public final ColumnType type;
    public final EncodingType encoding;
    public final long byteOffset;
    public final int byteLength;

    public final long minLong;
    public final long maxLong;
    public final double minDouble;
    public final double maxDouble;
    public final int minInt;
    public final int maxInt;
    public final String minString;
    public final String maxString;

    private ColumnStats(
            ColumnType type,
            EncodingType encoding,
            long byteOffset,
            int byteLength,
            long minLong,
            long maxLong,
            double minDouble,
            double maxDouble,
            int minInt,
            int maxInt,
            String minString,
            String maxString) {
        this.type = type;
        this.encoding = encoding;
        this.byteOffset = byteOffset;
        this.byteLength = byteLength;
        this.minLong = minLong;
        this.maxLong = maxLong;
        this.minDouble = minDouble;
        this.maxDouble = maxDouble;
        this.minInt = minInt;
        this.maxInt = maxInt;
        this.minString = minString;
        this.maxString = maxString;
    }

    public static ColumnStats ofLong(EncodingType enc, long offset, int length, long min, long max) {
        return new ColumnStats(ColumnType.INT64, enc, offset, length, min, max, 0, 0, 0, 0, null, null);
    }

    public static ColumnStats ofDouble(EncodingType enc, long offset, int length, double min, double max) {
        return new ColumnStats(ColumnType.DOUBLE, enc, offset, length, 0, 0, min, max, 0, 0, null, null);
    }

    public static ColumnStats ofDate(EncodingType enc, long offset, int length, int min, int max) {
        return new ColumnStats(ColumnType.DATE, enc, offset, length, 0, 0, 0, 0, min, max, null, null);
    }

    public static ColumnStats ofString(EncodingType enc, long offset, int length, String min, String max) {
        return new ColumnStats(ColumnType.STRING, enc, offset, length, 0, 0, 0, 0, 0, 0, min, max);
    }
}
