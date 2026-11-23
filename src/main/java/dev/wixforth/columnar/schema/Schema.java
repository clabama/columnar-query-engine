package dev.wixforth.columnar.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Immutable relational table schema with O(1) case-insensitive column lookup.
 */
public final class Schema {
    private final List<ColumnDef> columns;
    private final Map<String, Integer> nameToIndex;

    public Schema(List<ColumnDef> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Schema must contain at least one column");
        }
        this.columns = List.copyOf(columns);
        Map<String, Integer> index = new HashMap<>(columns.size() * 2);
        for (int i = 0; i < this.columns.size(); i++) {
            String key = this.columns.get(i).name().toLowerCase(Locale.ROOT);
            if (index.put(key, i) != null) {
                throw new IllegalArgumentException("Duplicate column name: " + this.columns.get(i).name());
            }
        }
        this.nameToIndex = Collections.unmodifiableMap(index);
    }

    public List<ColumnDef> columns() {
        return columns;
    }

    public int columnCount() {
        return columns.size();
    }

    public ColumnDef column(int idx) {
        return columns.get(idx);
    }

    public int indexOf(String name) {
        Integer idx = nameToIndex.get(name.toLowerCase(Locale.ROOT));
        if (idx == null) {
            throw new IllegalArgumentException("Unknown column: " + name);
        }
        return idx;
    }

    public boolean hasColumn(String name) {
        return nameToIndex.containsKey(name.toLowerCase(Locale.ROOT));
    }

    public ColumnType typeOf(String name) {
        return columns.get(indexOf(name)).type();
    }

    @Override
    public String toString() {
        return "Schema" + columns;
    }
}
