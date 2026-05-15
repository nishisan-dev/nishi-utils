package dev.nishisan.utils.oss.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Avaliador estático para o subset de expressões aceito por
 * {@code derive.output.formula}.
 *
 * <p>Gramática suportada:</p>
 * <pre>
 * expr   := term (('+' | '-') term)*
 * term   := factor (('*' | '/') factor)*
 * factor := identifier | number | '(' expr ')' | ('+' | '-') factor
 * </pre>
 *
 * <p>Identifiers válidos são apenas {@code delta} e {@code deltaT}, fornecidos
 * via mapa de bindings na chamada {@link #evaluate(String, Map)}.</p>
 *
 * <p>Implementação por parser recursivo descendente; sem alocações entre
 * chamadas — instâncias são cheap throwaway. Compila e avalia a cada chamada,
 * o que é aceitável para a cadência típica do ngrrd (ingestão em escala de
 * segundos/minutos).</p>
 */
public final class FormulaEvaluator {

    private final List<Token> tokens;
    private int cursor;

    private FormulaEvaluator(List<Token> tokens) {
        this.tokens = tokens;
        this.cursor = 0;
    }

    /**
     * Avalia uma expressão.
     *
     * @param formula  expressão (ex.: {@code "delta * 8 / deltaT"})
     * @param bindings valores para os identifiers {@code delta} e {@code deltaT}
     * @return resultado numérico
     */
    public static double evaluate(String formula, Map<String, Double> bindings) {
        if (formula == null || formula.isBlank()) {
            throw new IllegalArgumentException("formula não pode ser vazia");
        }
        List<Token> tokens = tokenize(formula);
        FormulaEvaluator parser = new FormulaEvaluator(tokens);
        double result = parser.parseExpression(bindings);
        if (parser.cursor != tokens.size()) {
            throw new IllegalArgumentException("Tokens remanescentes após o parse de '" + formula + "'");
        }
        return result;
    }

    private double parseExpression(Map<String, Double> bindings) {
        double left = parseTerm(bindings);
        while (cursor < tokens.size()) {
            Token t = tokens.get(cursor);
            if (t.kind != TokenKind.PLUS && t.kind != TokenKind.MINUS) {
                break;
            }
            cursor++;
            double right = parseTerm(bindings);
            left = t.kind == TokenKind.PLUS ? left + right : left - right;
        }
        return left;
    }

    private double parseTerm(Map<String, Double> bindings) {
        double left = parseFactor(bindings);
        while (cursor < tokens.size()) {
            Token t = tokens.get(cursor);
            if (t.kind != TokenKind.STAR && t.kind != TokenKind.SLASH) {
                break;
            }
            cursor++;
            double right = parseFactor(bindings);
            left = t.kind == TokenKind.STAR ? left * right : left / right;
        }
        return left;
    }

    private double parseFactor(Map<String, Double> bindings) {
        if (cursor >= tokens.size()) {
            throw new IllegalArgumentException("Expressão incompleta");
        }
        Token t = tokens.get(cursor);
        switch (t.kind) {
            case PLUS -> {
                cursor++;
                return parseFactor(bindings);
            }
            case MINUS -> {
                cursor++;
                return -parseFactor(bindings);
            }
            case LPAREN -> {
                cursor++;
                double inner = parseExpression(bindings);
                expect(TokenKind.RPAREN);
                return inner;
            }
            case NUMBER -> {
                cursor++;
                return Double.parseDouble(t.text);
            }
            case IDENT -> {
                cursor++;
                Double bound = bindings.get(t.text);
                if (bound == null) {
                    throw new IllegalArgumentException("Identifier sem binding: " + t.text);
                }
                return bound;
            }
            default -> throw new IllegalArgumentException("Token inesperado: " + t.text);
        }
    }

    private void expect(TokenKind kind) {
        if (cursor >= tokens.size() || tokens.get(cursor).kind != kind) {
            throw new IllegalArgumentException("Esperado " + kind + " na posição " + cursor);
        }
        cursor++;
    }

    private static List<Token> tokenize(String formula) {
        List<Token> out = new ArrayList<>();
        int i = 0;
        while (i < formula.length()) {
            char c = formula.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '+') {
                out.add(new Token(TokenKind.PLUS, "+"));
                i++;
            } else if (c == '-') {
                out.add(new Token(TokenKind.MINUS, "-"));
                i++;
            } else if (c == '*') {
                out.add(new Token(TokenKind.STAR, "*"));
                i++;
            } else if (c == '/') {
                out.add(new Token(TokenKind.SLASH, "/"));
                i++;
            } else if (c == '(') {
                out.add(new Token(TokenKind.LPAREN, "("));
                i++;
            } else if (c == ')') {
                out.add(new Token(TokenKind.RPAREN, ")"));
                i++;
            } else if (Character.isDigit(c) || c == '.') {
                int start = i;
                while (i < formula.length()
                        && (Character.isDigit(formula.charAt(i)) || formula.charAt(i) == '.')) {
                    i++;
                }
                out.add(new Token(TokenKind.NUMBER, formula.substring(start, i)));
            } else if (Character.isLetter(c) || c == '_') {
                int start = i;
                while (i < formula.length()
                        && (Character.isLetterOrDigit(formula.charAt(i)) || formula.charAt(i) == '_')) {
                    i++;
                }
                out.add(new Token(TokenKind.IDENT, formula.substring(start, i)));
            } else {
                throw new IllegalArgumentException("Caractere inválido em formula na posição " + i + ": '" + c + "'");
            }
        }
        return out;
    }

    private enum TokenKind {
        PLUS, MINUS, STAR, SLASH, LPAREN, RPAREN, NUMBER, IDENT
    }

    private record Token(TokenKind kind, String text) {
    }
}
