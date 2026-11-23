package dev.wixforth.columnar.sql;

/**
 * Sealed AST expression hierarchy for SQL predicates, arithmetic expressions, and aggregates.
 */
public sealed interface Expr permits
        Expr.ColumnRef,
        Expr.LongLiteral,
        Expr.DoubleLiteral,
        Expr.StringLiteral,
        Expr.BinaryOp,
        Expr.UnaryMinus,
        Expr.Between,
        Expr.AggregateCall,
        Expr.Star {

    enum Op {
        ADD, SUB, MUL, DIV,
        EQ, NEQ, LT, LTE, GT, GTE,
        AND, OR
    }

    enum AggFunc {
        SUM, COUNT, AVG, MIN, MAX
    }

    record ColumnRef(String tableAlias, String columnName) implements Expr {
        public String qualifiedName() {
            return tableAlias == null ? columnName : tableAlias + "." + columnName;
        }
    }

    record LongLiteral(long value) implements Expr {}

    record DoubleLiteral(double value) implements Expr {}

    record StringLiteral(String value) implements Expr {}

    record BinaryOp(Op op, Expr left, Expr right) implements Expr {}

    record UnaryMinus(Expr operand) implements Expr {}

    record Between(Expr value, Expr low, Expr high) implements Expr {}

    record AggregateCall(AggFunc func, Expr argument) implements Expr {}

    record Star() implements Expr {}
}
