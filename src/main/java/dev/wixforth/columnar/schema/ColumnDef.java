package dev.wixforth.columnar.schema;

import java.util.Objects;

/**
 * Immutable column definition within a relational table schema.
 */
public record ColumnDef(String name, ColumnType type) {
    public ColumnDef {
        Objects.requireNonNull(name, "Column name must not be null");
        Objects.requireNonNull(type, "Column type must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Column name must not be blank");
        }
    }
}
