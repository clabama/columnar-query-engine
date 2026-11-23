package dev.wixforth.columnar.storage;

import java.nio.file.Path;

/**
 * Immutable catalog metadata for a single micro-partition file on disk.
 */
public final class PartitionMetadata {
    public final int partitionId;
    public final Path filePath;
    public final int rowCount;
    public final long totalFileBytes;
    private final ColumnStats[] columnStats;

    public PartitionMetadata(
            int partitionId,
            Path filePath,
            int rowCount,
            long totalFileBytes,
            ColumnStats[] columnStats) {
        this.partitionId = partitionId;
        this.filePath = filePath;
        this.rowCount = rowCount;
        this.totalFileBytes = totalFileBytes;
        this.columnStats = columnStats;
    }

    public ColumnStats stats(int colIdx) {
        return columnStats[colIdx];
    }

    public int columnCount() {
        return columnStats.length;
    }
}
