package dev.wixforth.columnar.sql;

/**
 * Lexical token produced by {@link SqlLexer}.
 */
public record Token(Type type, String text, int position) {
    public enum Type {
        // Keywords
        SELECT, FROM, WHERE, INNER, JOIN, ON, GROUP, ORDER, BY, ASC, DESC,
        LIMIT, AS, AND, OR, BETWEEN, NOT,
        // Punctuation & operators
        COMMA, DOT, LPAREN, RPAREN, STAR, PLUS, MINUS, SLASH,
        EQ, NEQ, LT, LTE, GT, GTE, SEMICOLON,
        // Literals & identifiers
        IDENTIFIER, INT_LITERAL, DOUBLE_LITERAL, STRING_LITERAL,
        EOF
    }
}
