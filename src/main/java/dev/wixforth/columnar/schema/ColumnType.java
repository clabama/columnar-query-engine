package dev.wixforth.columnar.schema;

/**
 * Supported primitive and dictionary-encoded column data types.
 */
public enum ColumnType {
    INT64,
    DOUBLE,
    STRING,
    /** Calendar date stored internally as epoch-day int32 for compact vectorized comparisons. */
    DATE
}
