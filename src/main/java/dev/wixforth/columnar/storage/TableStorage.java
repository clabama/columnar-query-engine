package dev.wixforth.columnar.storage;

import dev.wixforth.columnar.schema.Schema;
import dev.wixforth.columnar.vector.ColumnVector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages immutable columnar micro-partitions belonging to a single logical table.
 */
public final class TableStorage {
    public static final int DEFAULT_PARTITION_ROWS = 65_536;

    private final String name;
    private final Schema schema;
    private final Path tableDir;
    private final List<PartitionMetadata> partitions = new ArrayList<>();
    private long totalRows = 0;
    private long totalBytes = 0;

    public TableStorage(String name, Schema schema, Path tableDir) throws IOException {
        this.name = name;
        this.schema = schema;
        this.tableDir = tableDir;
        Files.createDirectories(tableDir);
    }

    public String name() {
        return name;
    }

    public Schema schema() {
        return schema;
    }

    public List<PartitionMetadata> partitions() {
        return Collections.unmodifiableList(partitions);
    }

    public int partitionCount() {
        return partitions.size();
    }

    public long totalRows() {
        return totalRows;
    }

    public long totalBytes() {
        return totalBytes;
    }

    /**
     * Writes an immutable micro-partition to disk and registers its zone maps in memory.
     */
    public synchronized PartitionMetadata appendPartition(ColumnVector[] columns, int rowCount) throws IOException {
        if (columns.length != schema.columnCount()) {
            throw new IllegalArgumentException("Column count mismatch for table " + name);
        }
        int id = partitions.size();
        Path file = tableDir.resolve(String.format("part-%05d.mpart", id));
        PartitionMetadata meta = MicroPartitionCodec.writePartition(id, file, schema, columns, rowCount);
        partitions.add(meta);
        totalRows += rowCount;
        totalBytes += meta.totalFileBytes;
        return meta;
    }

    /**
     * Loads a single projected column from a partition, checking the sharded LRU cache first.
     */
    public ColumnVector loadColumn(
            PartitionMetadata meta,
            int colIdx,
            ShardedLruCache cache,
            AtomicLong bytesReadCounter,
            AtomicLong cacheHitsCounter,
            AtomicLong cacheMissesCounter) throws IOException {
        if (cache != null) {
            ShardedLruCache.CacheKey key = new ShardedLruCache.CacheKey(name, meta.partitionId, colIdx);
            ColumnVector cached = cache.get(key);
            if (cached != null) {
                if (cacheHitsCounter != null) cacheHitsCounter.incrementAndGet();
                return cached;
            }
            if (cacheMissesCounter != null) cacheMissesCounter.incrementAndGet();
            ColumnVector loaded = MicroPartitionCodec.readColumn(meta, colIdx);
            if (bytesReadCounter != null) bytesReadCounter.addAndGet(meta.stats(colIdx).byteLength);
            cache.put(key, loaded);
            return loaded;
        }
        if (cacheMissesCounter != null) cacheMissesCounter.incrementAndGet();
        ColumnVector loaded = MicroPartitionCodec.readColumn(meta, colIdx);
        if (bytesReadCounter != null) bytesReadCounter.addAndGet(meta.stats(colIdx).byteLength);
        return loaded;
    }
}
