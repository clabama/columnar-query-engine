package dev.wixforth.columnar.vector;

/**
 * Fixed-size columnar batch (typically 4,096 rows) with an optional selection vector
 * for late materialization during vectorized filter and join execution.
 */
public final class VectorBatch {
    public static final int DEFAULT_BATCH_SIZE = 4096;

    public final ColumnVector[] columns;
    public final int rowCount;
    public int[] selection;
    public int selectedCount;
    public boolean hasSelection;

    public VectorBatch(ColumnVector[] columns, int rowCount) {
        this.columns = columns;
        this.rowCount = rowCount;
        this.selectedCount = rowCount;
        this.hasSelection = false;
    }

    public void applySelection(int[] sel, int count) {
        this.selection = sel;
        this.selectedCount = count;
        this.hasSelection = true;
    }

    public boolean isEmpty() {
        return selectedCount == 0;
    }
}
