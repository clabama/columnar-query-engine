package dev.wixforth.columnar.storage;

import dev.wixforth.columnar.vector.ColumnVector;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe, memory-bounded sharded LRU cache for decoded micro-partition column chunks.
 * Sharding across independent lock stripes eliminates global lock contention during parallel scans.
 */
public final class ShardedLruCache {
    public record CacheKey(String tableName, int partitionId, int columnIndex) {}

    private static final class Shard {
        final ReentrantLock lock = new ReentrantLock();
        final LinkedHashMap<CacheKey, Entry> map = new LinkedHashMap<>(64, 0.75f, true);
        final long maxBytes;
        long currentBytes = 0;

        Shard(long maxBytes) {
            this.maxBytes = Math.max(1024L, maxBytes);
        }
    }

    private record Entry(ColumnVector vector, long bytes) {}

    private final Shard[] shards;
    private final int shardMask;
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    public ShardedLruCache(long maxTotalBytes, int numShardsPowerOfTwo) {
        if (Integer.bitCount(numShardsPowerOfTwo) != 1) {
            throw new IllegalArgumentException("Shard count must be a power of two");
        }
        this.shards = new Shard[numShardsPowerOfTwo];
        this.shardMask = numShardsPowerOfTwo - 1;
        long perShard = Math.max(4096L, maxTotalBytes / numShardsPowerOfTwo);
        for (int i = 0; i < numShardsPowerOfTwo; i++) {
            this.shards[i] = new Shard(perShard);
        }
    }

    public ShardedLruCache(long maxTotalBytes) {
        this(maxTotalBytes, 16);
    }

    private Shard shardFor(CacheKey key) {
        int h = key.hashCode();
        h ^= (h >>> 16);
        return shards[h & shardMask];
    }

    public ColumnVector get(CacheKey key) {
        Shard shard = shardFor(key);
        shard.lock.lock();
        try {
            Entry e = shard.map.get(key);
            if (e != null) {
                hits.incrementAndGet();
                return e.vector;
            }
            misses.incrementAndGet();
            return null;
        } finally {
            shard.lock.unlock();
        }
    }

    public void put(CacheKey key, ColumnVector vector) {
        long size = vector.estimatedBytes();
        Shard shard = shardFor(key);
        if (size > shard.maxBytes) {
            return; // Do not cache single chunks larger than a shard's budget
        }
        shard.lock.lock();
        try {
            Entry prev = shard.map.put(key, new Entry(vector, size));
            if (prev != null) {
                shard.currentBytes -= prev.bytes;
            }
            shard.currentBytes += size;

            Iterator<Map.Entry<CacheKey, Entry>> it = shard.map.entrySet().iterator();
            while (shard.currentBytes > shard.maxBytes && it.hasNext()) {
                Map.Entry<CacheKey, Entry> eldest = it.next();
                shard.currentBytes -= eldest.getValue().bytes;
                it.remove();
                evictions.incrementAndGet();
            }
        } finally {
            shard.lock.unlock();
        }
    }

    public void clear() {
        for (Shard shard : shards) {
            shard.lock.lock();
            try {
                shard.map.clear();
                shard.currentBytes = 0;
            } finally {
                shard.lock.unlock();
            }
        }
        hits.set(0);
        misses.set(0);
        evictions.set(0);
    }

    public long hits() {
        return hits.get();
    }

    public long misses() {
        return misses.get();
    }

    public long evictions() {
        return evictions.get();
    }

    public long totalCachedBytes() {
        long total = 0;
        for (Shard shard : shards) {
            shard.lock.lock();
            try {
                total += shard.currentBytes;
            } finally {
                shard.lock.unlock();
            }
        }
        return total;
    }
}
