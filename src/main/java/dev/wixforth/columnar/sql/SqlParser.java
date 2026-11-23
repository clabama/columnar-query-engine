package dev.wixforth.columnar.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Hand-written recursive-descent SQL parser supporting SELECT, FROM, INNER JOIN, WHERE,
 * GROUP BY, ORDER BY, LIMIT, arithmetic expressions, and standard SQL aggregates.
 */
public final class SqlParser {
    private final List<Token> tokens;
    private int pos = 0;

    private SqlParser(List<Token> tokens) {
        this.tokens = tokens;
    }

    public static SelectStatement parse(String sql) {
        List<Token> tokens = SqlLexer.tokenize(sql);
        SqlParser parser = new SqlParser(tokens);
        SelectStatement stmt = parser.parseSelect();
        if (parser.peek().type() == Token.Type.SEMICOLON) {
            parser.advance();
        }
        if (parser.peek().type() != Token.Type.EOF) {
            throw new IllegalArgumentException(
                    "Unexpected token '" + parser.peek().text() + "' at position " + parser.peek().position());
        }
        return stmt;
    }

    private SelectStatement parseSelect() {
        expect(Token.Type.SELECT);
        List<SelectStatement.SelectItem> items = new ArrayList<>();
        do {
            Expr expr = parseExpression();
            String alias = null;
            if (match(Token.Type.AS)) {
                alias = expect(Token.Type.IDENTIFIER).text();
            } else if (peek().type() == Token.Type.IDENTIFIER) {
                alias = advance().text();
            }
            items.add(new SelectStatement.SelectItem(expr, alias));
        } while (match(Token.Type.COMMA));

        expect(Token.Type.FROM);
        SelectStatement.TableRef fromTable = parseTableRef();

        List<SelectStatement.JoinClause> joins = new ArrayList<>();
        while (peek().type() == Token.Type.INNER || peek().type() == Token.Type.JOIN) {
            match(Token.Type.INNER);
            expect(Token.Type.JOIN);
            SelectStatement.TableRef right = parseTableRef();
            expect(Token.Type.ON);
            Expr.ColumnRef leftKey = parseColumnRef();
            expect(Token.Type.EQ);
            Expr.ColumnRef rightKey = parseColumnRef();
            joins.add(new SelectStatement.JoinClause(right, leftKey, rightKey));
        }

        Expr where = null;
        if (match(Token.Type.WHERE)) {
            where = parseExpression();
        }

        List<Expr.ColumnRef> groupBy = new ArrayList<>();
        if (match(Token.Type.GROUP)) {
            expect(Token.Type.BY);
            do {
                groupBy.add(parseColumnRef());
            } while (match(Token.Type.COMMA));
        }

        List<SelectStatement.OrderByItem> orderBy = new ArrayList<>();
        if (match(Token.Type.ORDER)) {
            expect(Token.Type.BY);
            do {
                Expr.ColumnRef ref = parseColumnRef();
                boolean desc = false;
                if (match(Token.Type.DESC)) {
                    desc = true;
                } else {
                    match(Token.Type.ASC);
                }
                orderBy.add(new SelectStatement.OrderByItem(ref.qualifiedName(), desc));
            } while (match(Token.Type.COMMA));
        }

        Integer limit = null;
        if (match(Token.Type.LIMIT)) {
            limit = Integer.parseInt(expect(Token.Type.INT_LITERAL).text());
        }

        return new SelectStatement(items, fromTable, joins, where, groupBy, orderBy, limit);
    }

    private SelectStatement.TableRef parseTableRef() {
        String table = expect(Token.Type.IDENTIFIER).text();
        String alias = null;
        if (match(Token.Type.AS)) {
            alias = expect(Token.Type.IDENTIFIER).text();
        } else if (peek().type() == Token.Type.IDENTIFIER) {
            alias = advance().text();
        }
        return new SelectStatement.TableRef(table, alias);
    }

    private Expr.ColumnRef parseColumnRef() {
        String first = expect(Token.Type.IDENTIFIER).text();
        if (match(Token.Type.DOT)) {
            String col = expect(Token.Type.IDENTIFIER).text();
            return new Expr.ColumnRef(first, col);
        }
        return new Expr.ColumnRef(null, first);
    }

    private Expr parseExpression() {
        return parseOr();
    }

    private Expr parseOr() {
        Expr left = parseAnd();
        while (match(Token.Type.OR)) {
            Expr right = parseAnd();
            left = new Expr.BinaryOp(Expr.Op.OR, left, right);
        }
        return left;
    }

    private Expr parseAnd() {
        Expr left = parseComparison();
        while (match(Token.Type.AND)) {
            Expr right = parseComparison();
            left = new Expr.BinaryOp(Expr.Op.AND, left, right);
        }
        return left;
    }

    private Expr parseComparison() {
        Expr left = parseAdditive();
        if (match(Token.Type.BETWEEN)) {
            Expr low = parseAdditive();
            expect(Token.Type.AND);
            Expr high = parseAdditive();
            return new Expr.Between(left, low, high);
        }
        Token.Type t = peek().type();
        Expr.Op op = switch (t) {
            case EQ -> Expr.Op.EQ;
            case NEQ -> Expr.Op.NEQ;
            case LT -> Expr.Op.LT;
            case LTE -> Expr.Op.LTE;
            case GT -> Expr.Op.GT;
            case GTE -> Expr.Op.GTE;
            default -> null;
        };
        if (op != null) {
            advance();
            Expr right = parseAdditive();
            return new Expr.BinaryOp(op, left, right);
        }
        return left;
    }

    private Expr parseAdditive() {
        Expr left = parseMultiplicative();
        while (peek().type() == Token.Type.PLUS || peek().type() == Token.Type.MINUS) {
            Expr.Op op = advance().type() == Token.Type.PLUS ? Expr.Op.ADD : Expr.Op.SUB;
            Expr right = parseMultiplicative();
            left = new Expr.BinaryOp(op, left, right);
        }
        return left;
    }

    private Expr parseMultiplicative() {
        Expr left = parseUnary();
        while (peek().type() == Token.Type.STAR || peek().type() == Token.Type.SLASH) {
            Expr.Op op = advance().type() == Token.Type.STAR ? Expr.Op.MUL : Expr.Op.DIV;
            Expr right = parseUnary();
            left = new Expr.BinaryOp(op, left, right);
        }
        return left;
    }

    private Expr parseUnary() {
        if (match(Token.Type.MINUS)) {
            return new Expr.UnaryMinus(parseUnary());
        }
        return parsePrimary();
    }

    private Expr parsePrimary() {
        Token tok = peek();
        switch (tok.type()) {
            case INT_LITERAL -> {
                advance();
                return new Expr.LongLiteral(Long.parseLong(tok.text()));
            }
            case DOUBLE_LITERAL -> {
                advance();
                return new Expr.DoubleLiteral(Double.parseDouble(tok.text()));
            }
            case STRING_LITERAL -> {
                advance();
                return new Expr.StringLiteral(tok.text());
            }
            case STAR -> {
                advance();
                return new Expr.Star();
            }
            case LPAREN -> {
                advance();
                Expr inner = parseExpression();
                expect(Token.Type.RPAREN);
                return inner;
            }
            case IDENTIFIER -> {
                advance();
                if (peek().type() == Token.Type.LPAREN) {
                    advance();
                    Expr.AggFunc func = parseAggFunc(tok.text());
                    Expr arg = parseExpression();
                    expect(Token.Type.RPAREN);
                    return new Expr.AggregateCall(func, arg);
                }
                if (match(Token.Type.DOT)) {
                    String col = expect(Token.Type.IDENTIFIER).text();
                    return new Expr.ColumnRef(tok.text(), col);
                }
                return new Expr.ColumnRef(null, tok.text());
            }
            default -> throw new IllegalArgumentException(
                    "Unexpected token '" + tok.text() + "' at position " + tok.position());
        }
    }

    private Expr.AggFunc parseAggFunc(String name) {
        return switch (name.toUpperCase(Locale.ROOT)) {
            case "SUM" -> Expr.AggFunc.SUM;
            case "COUNT" -> Expr.AggFunc.COUNT;
            case "AVG" -> Expr.AggFunc.AVG;
            case "MIN" -> Expr.AggFunc.MIN;
            case "MAX" -> Expr.AggFunc.MAX;
            default -> throw new IllegalArgumentException("Unsupported function: " + name);
        };
    }

    private Token peek() {
        return tokens.get(pos);
    }

    private Token advance() {
        return tokens.get(pos++);
    }

    private boolean match(Token.Type type) {
        if (peek().type() == type) {
            pos++;
            return true;
        }
        return false;
    }

    private Token expect(Token.Type type) {
        Token tok = peek();
        if (tok.type() != type) {
            throw new IllegalArgumentException(
                    "Expected " + type + " but found " + tok.type() + " ('" + tok.text() + "') at position " + tok.position());
        }
        pos++;
        return tok;
    }
}
