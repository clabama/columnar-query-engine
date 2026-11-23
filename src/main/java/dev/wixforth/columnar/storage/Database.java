package dev.wixforth.columnar.storage;

import dev.wixforth.columnar.schema.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process database catalog holding table micro-partition stores and the shared sharded LRU cache.
 */
public final class Database {
    private final Path dataDir;
    private final Map<String, TableStorage> tables = new ConcurrentHashMap<>();
    private final ShardedLruCache cache;

    public Database(Path dataDir, long cacheBudgetBytes) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
        this.cache = new ShardedLruCache(cacheBudgetBytes, 16);
    }

    public Database(Path dataDir) throws IOException {
        this(dataDir, 256L * 1024 * 1024); // 256 MB default chunk cache
    }

    public TableStorage createTable(String name, Schema schema) throws IOException {
        String key = name.toLowerCase(Locale.ROOT);
        TableStorage table = new TableStorage(key, schema, dataDir.resolve(key));
        tables.put(key, table);
        return table;
    }

    public TableStorage table(String name) {
        TableStorage t = tables.get(name.toLowerCase(Locale.ROOT));
        if (t == null) {
            throw new IllegalArgumentException("Unknown table: " + name);
        }
        return t;
    }

    public Set<String> tableNames() {
        return tables.keySet();
    }

    public ShardedLruCache cache() {
        return cache;
    }

    public Path dataDir() {
        return dataDir;
    }
}
