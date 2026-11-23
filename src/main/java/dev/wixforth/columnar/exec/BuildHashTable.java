package dev.wixforth.columnar.exec;

import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.DateUtil;
import dev.wixforth.columnar.sql.LogicalPlan;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compact primitive-backed columnar build-side hash table for parallel lock-free hash joins.
 *
 * Stores materialized build-side columns in growable primitive arrays and indexes rows by
 * {@code long} join keys using open addressing with linear probing + intrusive next-row chains
 * for duplicate keys (zero per-row Object/List allocations during build or probe).
 */
public final class BuildHashTable {
    public final List<LogicalPlan.ResolvedColumn> schemaCols;
    private final Map<String, Integer> colSlotMap; // "alias.col" -> slot index
    private final Object[] columnArrays;           // long[], double[], int[], or String[]
    private int rowCount = 0;
    private int capacity;

    // Open-addressing hash index (built once after all build rows are appended -> read-only during parallel probe)
    private long[] bucketKeys;
    private int[] bucketHead;
    private int[] nextRow;
    private int mask;
    private boolean indexed = false;

    public BuildHashTable(List<LogicalPlan.ResolvedColumn> schemaCols, int initialCapacity) {
        this.schemaCols = schemaCols;
        this.capacity = Math.max(64, initialCapacity);
        this.colSlotMap = new HashMap<>(schemaCols.size() * 2);
        this.columnArrays = new Object[schemaCols.size()];
        for (int i = 0; i < schemaCols.size(); i++) {
            LogicalPlan.ResolvedColumn rc = schemaCols.get(i);
            colSlotMap.put(rc.tableAlias() + "." + rc.columnName(), i);
            columnArrays[i] = switch (rc.type()) {
                case INT64 -> new long[this.capacity];
                case DOUBLE -> new double[this.capacity];
                case DATE -> new int[this.capacity];
                case STRING -> new String[this.capacity];
            };
        }
    }

    public int slotOf(LogicalPlan.ResolvedColumn rc) {
        Integer slot = colSlotMap.get(rc.tableAlias() + "." + rc.columnName());
        if (slot == null) {
            throw new IllegalStateException("Column not in BuildHashTable: " + rc);
        }
        return slot;
    }

    public boolean containsAlias(String alias) {
        for (LogicalPlan.ResolvedColumn rc : schemaCols) {
            if (rc.tableAlias().equals(alias)) return true;
        }
        return false;
    }

    public int rowCount() {
        return rowCount;
    }

    public int appendRow() {
        if (rowCount == capacity) {
            grow();
        }
        return rowCount++;
    }

    public void setLong(int slot, int row, long v) {
        ((long[]) columnArrays[slot])[row] = v;
    }

    public void setDouble(int slot, int row, double v) {
        ((double[]) columnArrays[slot])[row] = v;
    }

    public void setDate(int slot, int row, int v) {
        ((int[]) columnArrays[slot])[row] = v;
    }

    public void setString(int slot, int row, String v) {
        ((String[]) columnArrays[slot])[row] = v;
    }

    public long getLong(int slot, int row) {
        return ((long[]) columnArrays[slot])[row];
    }

    public double getDouble(int slot, int row) {
        ColumnType t = schemaCols.get(slot).type();
        return switch (t) {
            case DOUBLE -> ((double[]) columnArrays[slot])[row];
            case INT64 -> (double) ((long[]) columnArrays[slot])[row];
            case DATE -> (double) ((int[]) columnArrays[slot])[row];
            default -> throw new IllegalArgumentException("Non-numeric build column: " + schemaCols.get(slot));
        };
    }

    public int getDate(int slot, int row) {
        return ((int[]) columnArrays[slot])[row];
    }

    public String getString(int slot, int row) {
        return ((String[]) columnArrays[slot])[row];
    }

    public Object getBoxed(int slot, int row) {
        return switch (schemaCols.get(slot).type()) {
            case INT64 -> ((long[]) columnArrays[slot])[row];
            case DOUBLE -> ((double[]) columnArrays[slot])[row];
            case DATE -> DateUtil.formatEpochDay(((int[]) columnArrays[slot])[row]);
            case STRING -> ((String[]) columnArrays[slot])[row];
        };
    }

    /**
     * Freezes the build relation and builds a flat open-addressing hash index over {@code keyCol}.
     * Thread-safe for concurrent lock-free probing once complete.
     */
    public void buildIndex(LogicalPlan.ResolvedColumn keyCol) {
        int keySlot = slotOf(keyCol);
        ColumnType keyType = keyCol.type();

        int tableSize = 1;
        int target = Math.max(16, rowCount * 2);
        while (tableSize < target) {
            tableSize <<= 1;
        }
        this.mask = tableSize - 1;
        this.bucketKeys = new long[tableSize];
        this.bucketHead = new int[tableSize];
        Arrays.fill(this.bucketHead, -1);
        this.nextRow = new int[rowCount];
        Arrays.fill(this.nextRow, -1);

        for (int r = 0; r < rowCount; r++) {
            long key = extractKey(keySlot, keyType, r);
            int slot = hash(key) & mask;
            while (true) {
                int head = bucketHead[slot];
                if (head == -1) {
                    bucketKeys[slot] = key;
                    bucketHead[slot] = r;
                    break;
                }
                if (bucketKeys[slot] == key) {
                    nextRow[r] = head;
                    bucketHead[slot] = r;
                    break;
                }
                slot = (slot + 1) & mask;
            }
        }
        this.indexed = true;
    }

    /**
     * Lock-free probe returning the head build-row index matching {@code key}, or -1 if absent.
     * Follow duplicate matches via {@link #nextMatch(int)}.
     */
    public int probeFirst(long key) {
        if (!indexed || rowCount == 0) return -1;
        int slot = hash(key) & mask;
        while (true) {
            int head = bucketHead[slot];
            if (head == -1) return -1;
            if (bucketKeys[slot] == key) return head;
            slot = (slot + 1) & mask;
        }
    }

    public int nextMatch(int buildRow) {
        return nextRow[buildRow];
    }

    private long extractKey(int slot, ColumnType type, int row) {
        return switch (type) {
            case INT64 -> ((long[]) columnArrays[slot])[row];
            case DATE -> ((int[]) columnArrays[slot])[row];
            case STRING -> ((String[]) columnArrays[slot])[row].hashCode();
            case DOUBLE -> Double.doubleToLongBits(((double[]) columnArrays[slot])[row]);
        };
    }

    private static int hash(long key) {
        long h = key * 0x9E3779B97F4A7C15L;
        return (int) (h ^ (h >>> 32));
    }

    private void grow() {
        int newCap = capacity * 2;
        for (int i = 0; i < schemaCols.size(); i++) {
            columnArrays[i] = switch (schemaCols.get(i).type()) {
                case INT64 -> Arrays.copyOf((long[]) columnArrays[i], newCap);
                case DOUBLE -> Arrays.copyOf((double[]) columnArrays[i], newCap);
                case DATE -> Arrays.copyOf((int[]) columnArrays[i], newCap);
                case STRING -> Arrays.copyOf((String[]) columnArrays[i], newCap);
            };
        }
        capacity = newCap;
    }
}
