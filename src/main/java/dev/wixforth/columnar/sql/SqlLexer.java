package dev.wixforth.columnar.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Hand-written single-pass SQL lexer supporting analytical query syntax.
 */
public final class SqlLexer {
    private static final Map<String, Token.Type> KEYWORDS = Map.ofEntries(
            Map.entry("SELECT", Token.Type.SELECT),
            Map.entry("FROM", Token.Type.FROM),
            Map.entry("WHERE", Token.Type.WHERE),
            Map.entry("INNER", Token.Type.INNER),
            Map.entry("JOIN", Token.Type.JOIN),
            Map.entry("ON", Token.Type.ON),
            Map.entry("GROUP", Token.Type.GROUP),
            Map.entry("ORDER", Token.Type.ORDER),
            Map.entry("BY", Token.Type.BY),
            Map.entry("ASC", Token.Type.ASC),
            Map.entry("DESC", Token.Type.DESC),
            Map.entry("LIMIT", Token.Type.LIMIT),
            Map.entry("AS", Token.Type.AS),
            Map.entry("AND", Token.Type.AND),
            Map.entry("OR", Token.Type.OR),
            Map.entry("BETWEEN", Token.Type.BETWEEN),
            Map.entry("NOT", Token.Type.NOT)
    );

    private SqlLexer() {}

    public static List<Token> tokenize(String sql) {
        List<Token> tokens = new ArrayList<>();
        int i = 0;
        int n = sql.length();

        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            // Line comment --
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                i += 2;
                while (i < n && sql.charAt(i) != '\n') i++;
                continue;
            }

            int start = i;
            switch (c) {
                case ',' -> { tokens.add(new Token(Token.Type.COMMA, ",", start)); i++; }
                case '.' -> { tokens.add(new Token(Token.Type.DOT, ".", start)); i++; }
                case '(' -> { tokens.add(new Token(Token.Type.LPAREN, "(", start)); i++; }
                case ')' -> { tokens.add(new Token(Token.Type.RPAREN, ")", start)); i++; }
                case '*' -> { tokens.add(new Token(Token.Type.STAR, "*", start)); i++; }
                case '+' -> { tokens.add(new Token(Token.Type.PLUS, "+", start)); i++; }
                case '-' -> { tokens.add(new Token(Token.Type.MINUS, "-", start)); i++; }
                case '/' -> { tokens.add(new Token(Token.Type.SLASH, "/", start)); i++; }
                case ';' -> { tokens.add(new Token(Token.Type.SEMICOLON, ";", start)); i++; }
                case '=' -> { tokens.add(new Token(Token.Type.EQ, "=", start)); i++; }
                case '!' -> {
                    if (i + 1 < n && sql.charAt(i + 1) == '=') {
                        tokens.add(new Token(Token.Type.NEQ, "!=", start));
                        i += 2;
                    } else {
                        throw new IllegalArgumentException("Unexpected character '!' at position " + start);
                    }
                }
                case '<' -> {
                    if (i + 1 < n && sql.charAt(i + 1) == '=') {
                        tokens.add(new Token(Token.Type.LTE, "<=", start));
                        i += 2;
                    } else if (i + 1 < n && sql.charAt(i + 1) == '>') {
                        tokens.add(new Token(Token.Type.NEQ, "<>", start));
                        i += 2;
                    } else {
                        tokens.add(new Token(Token.Type.LT, "<", start));
                        i++;
                    }
                }
                case '>' -> {
                    if (i + 1 < n && sql.charAt(i + 1) == '=') {
                        tokens.add(new Token(Token.Type.GTE, ">=", start));
                        i += 2;
                    } else {
                        tokens.add(new Token(Token.Type.GT, ">", start));
                        i++;
                    }
                }
                case '\'' -> {
                    i++; // opening quote
                    StringBuilder sb = new StringBuilder();
                    while (i < n) {
                        char ch = sql.charAt(i);
                        if (ch == '\'') {
                            if (i + 1 < n && sql.charAt(i + 1) == '\'') {
                                sb.append('\'');
                                i += 2;
                            } else {
                                i++;
                                break;
                            }
                        } else {
                            sb.append(ch);
                            i++;
                        }
                    }
                    tokens.add(new Token(Token.Type.STRING_LITERAL, sb.toString(), start));
                }
                default -> {
                    if (Character.isDigit(c)) {
                        boolean isDouble = false;
                        while (i < n && Character.isDigit(sql.charAt(i))) i++;
                        if (i < n && sql.charAt(i) == '.') {
                            isDouble = true;
                            i++;
                            while (i < n && Character.isDigit(sql.charAt(i))) i++;
                        }
                        String lit = sql.substring(start, i);
                        tokens.add(new Token(
                                isDouble ? Token.Type.DOUBLE_LITERAL : Token.Type.INT_LITERAL,
                                lit,
                                start));
                    } else if (Character.isLetter(c) || c == '_') {
                        while (i < n && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_')) i++;
                        String ident = sql.substring(start, i);
                        Token.Type kw = KEYWORDS.get(ident.toUpperCase(Locale.ROOT));
                        tokens.add(new Token(kw != null ? kw : Token.Type.IDENTIFIER, ident, start));
                    } else {
                        throw new IllegalArgumentException("Unexpected character '" + c + "' at position " + start);
                    }
                }
            }
        }

        tokens.add(new Token(Token.Type.EOF, "", n));
        return tokens;
    }
}
