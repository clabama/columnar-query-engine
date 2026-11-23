package dev.wixforth.columnar.sql;

import java.util.List;

/**
 * Parsed SQL SELECT statement AST.
 */
public record SelectStatement(
        List<SelectItem> selectItems,
        TableRef fromTable,
        List<JoinClause> joins,
        Expr whereClause,
        List<Expr.ColumnRef> groupBy,
        List<OrderByItem> orderBy,
        Integer limit) {

    public record SelectItem(Expr expr, String alias) {}

    public record TableRef(String tableName, String alias) {
        public String effectiveAlias() {
            return alias != null ? alias : tableName;
        }
    }

    public record JoinClause(TableRef rightTable, Expr.ColumnRef leftKey, Expr.ColumnRef rightKey) {}

    public record OrderByItem(String columnOrAlias, boolean descending) {}
}
