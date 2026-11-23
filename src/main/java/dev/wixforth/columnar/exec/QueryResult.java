package dev.wixforth.columnar.exec;

import dev.wixforth.columnar.schema.ColumnType;

import java.util.List;

/**
 * Materialized SQL query result set paired with execution telemetry.
 */
public record QueryResult(
        List<String> columnNames,
        List<ColumnType> columnTypes,
        List<Object[]> rows,
        ExecutionStats stats) {

    public int rowCount() {
        return rows.size();
    }

    public String formatTable() {
        int cols = columnNames.size();
        int[] widths = new int[cols];
        for (int c = 0; c < cols; c++) {
            widths[c] = columnNames.get(c).length();
        }
        String[][] formatted = new String[rows.size()][cols];
        for (int r = 0; r < rows.size(); r++) {
            Object[] row = rows.get(r);
            for (int c = 0; c < cols; c++) {
                String s = formatCell(row[c]);
                formatted[r][c] = s;
                if (s.length() > widths[c]) {
                    widths[c] = s.length();
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        appendRow(sb, columnNames.toArray(new String[0]), widths);
        for (int c = 0; c < cols; c++) {
            if (c > 0) sb.append("-+-");
            sb.append("-".repeat(widths[c]));
        }
        sb.append('\n');
        for (String[] row : formatted) {
            appendRow(sb, row, widths);
        }
        return sb.toString();
    }

    private static void appendRow(StringBuilder sb, String[] cells, int[] widths) {
        for (int c = 0; c < cells.length; c++) {
            if (c > 0) sb.append(" | ");
            sb.append(String.format("%-" + widths[c] + "s", cells[c]));
        }
        sb.append('\n');
    }

    private static String formatCell(Object val) {
        if (val == null) return "NULL";
        if (val instanceof Double d) {
            return String.format(java.util.Locale.ROOT, "%.2f", d);
        }
        return val.toString();
    }
}
