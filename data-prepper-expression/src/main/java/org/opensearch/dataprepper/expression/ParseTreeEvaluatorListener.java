/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionListener;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser.AND;
import static org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser.EQUAL;
import static org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser.GT;
import static org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser.GTE;
import static org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser.LT;
import static org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser.LTE;
import static org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser.NOT;
import static org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser.NOT_EQUAL;
import static org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser.OR;

/*
Example:
    ParseTreeWalker walker = new ParseTreeWalker();
    ParseTreeEvaluatorListener listener = new ParseTreeEvaluatorListener(...);
    listener.initialize(...);
    walker.walk(listener, ...);
    final Object result = listener.getResult();
 */
@Named
public class ParseTreeEvaluatorListener implements DataPrepperExpressionListener {
    private static final Set<Integer> CONDITIONAL_OPERATOR_TYPES = Set.of(AND, OR);
    private static final Set<Integer> EQUALITY_OPERATOR_TYPES = Set.of(EQUAL, NOT_EQUAL);
    private static final Set<Integer> RELATIONAL_OPERATOR_TYPES = Set.of(LT, LTE, GT, GTE);

    private final Map<Integer, Operator<?>> strategy;
    private final CoercionService coercionService;
    private Stack<Integer> operatorSymbolStack = new Stack<>();
    private Stack<Object> argStack = new Stack<>();
    private Event context;

    @Inject
    ParseTreeEvaluatorListener(final List<Operator<?>> operators, final CoercionService coercionService) {
        this.coercionService = coercionService;
        strategy = new HashMap<>(operators.size());
        for (final Operator<?> operator : operators) {
            strategy.put(operator.getSymbol(), operator);
        }
    }

    public void initialize(final Event context) {
        this.context = context;
        operatorSymbolStack = new Stack<>();
        argStack = new Stack<>();
    }

    public Object getResult() {
        if (argStack.size() != 1 || context == null) {
            throw new IllegalStateException("The ParseTreeEvaluatorListener has not been initialized or walked through exactly once " +
                    "after initialization by a ParseTreeWalker.");
        }
        return argStack.peek();
    }

    @Override
    public void enterExpression(DataPrepperExpressionParser.ExpressionContext ctx) {

    }

    @Override
    public void exitExpression(DataPrepperExpressionParser.ExpressionContext ctx) {

    }

    @Override
    public void enterConditionalExpression(DataPrepperExpressionParser.ConditionalExpressionContext ctx) {

    }

    @Override
    public void exitConditionalExpression(DataPrepperExpressionParser.ConditionalExpressionContext ctx) {
        if (operatorSymbolStack.isEmpty() || !CONDITIONAL_OPERATOR_TYPES.contains(operatorSymbolStack.peek())) {
            return;
        }
        performSingleOperation(2);
    }

    @Override
    public void enterConditionalOperator(DataPrepperExpressionParser.ConditionalOperatorContext ctx) {

    }

    @Override
    public void exitConditionalOperator(DataPrepperExpressionParser.ConditionalOperatorContext ctx) {

    }

    @Override
    public void enterEqualityOperatorExpression(DataPrepperExpressionParser.EqualityOperatorExpressionContext ctx) {

    }

    @Override
    public void exitEqualityOperatorExpression(DataPrepperExpressionParser.EqualityOperatorExpressionContext ctx) {
        if (operatorSymbolStack.isEmpty() || !EQUALITY_OPERATOR_TYPES.contains(operatorSymbolStack.peek())) {
            return;
        }
        performSingleOperation(2);
    }

    @Override
    public void enterEqualityOperator(DataPrepperExpressionParser.EqualityOperatorContext ctx) {

    }

    @Override
    public void exitEqualityOperator(DataPrepperExpressionParser.EqualityOperatorContext ctx) {

    }

    @Override
    public void enterRegexOperatorExpression(DataPrepperExpressionParser.RegexOperatorExpressionContext ctx) {

    }

    @Override
    public void exitRegexOperatorExpression(DataPrepperExpressionParser.RegexOperatorExpressionContext ctx) {

    }

    @Override
    public void enterRegexEqualityOperator(DataPrepperExpressionParser.RegexEqualityOperatorContext ctx) {

    }

    @Override
    public void exitRegexEqualityOperator(DataPrepperExpressionParser.RegexEqualityOperatorContext ctx) {

    }

    @Override
    public void enterRelationalOperatorExpression(DataPrepperExpressionParser.RelationalOperatorExpressionContext ctx) {

    }

    @Override
    public void exitRelationalOperatorExpression(DataPrepperExpressionParser.RelationalOperatorExpressionContext ctx) {
        if (operatorSymbolStack.isEmpty() || !RELATIONAL_OPERATOR_TYPES.contains(operatorSymbolStack.peek())) {
            return;
        }
        performSingleOperation(2);
    }

    @Override
    public void enterRelationalOperator(DataPrepperExpressionParser.RelationalOperatorContext ctx) {

    }

    @Override
    public void exitRelationalOperator(DataPrepperExpressionParser.RelationalOperatorContext ctx) {

    }

    @Override
    public void enterSetOperatorExpression(DataPrepperExpressionParser.SetOperatorExpressionContext ctx) {

    }

    @Override
    public void exitSetOperatorExpression(DataPrepperExpressionParser.SetOperatorExpressionContext ctx) {

    }

    @Override
    public void enterSetOperator(DataPrepperExpressionParser.SetOperatorContext ctx) {

    }

    @Override
    public void exitSetOperator(DataPrepperExpressionParser.SetOperatorContext ctx) {

    }

    @Override
    public void enterUnaryOperatorExpression(DataPrepperExpressionParser.UnaryOperatorExpressionContext ctx) {

    }

    @Override
    public void exitUnaryOperatorExpression(DataPrepperExpressionParser.UnaryOperatorExpressionContext ctx) {

    }

    @Override
    public void enterParenthesesExpression(DataPrepperExpressionParser.ParenthesesExpressionContext ctx) {

    }

    @Override
    public void exitParenthesesExpression(DataPrepperExpressionParser.ParenthesesExpressionContext ctx) {

    }

    @Override
    public void enterRegexPattern(DataPrepperExpressionParser.RegexPatternContext ctx) {

    }

    @Override
    public void exitRegexPattern(DataPrepperExpressionParser.RegexPatternContext ctx) {

    }

    @Override
    public void enterSetInitializer(DataPrepperExpressionParser.SetInitializerContext ctx) {

    }

    @Override
    public void exitSetInitializer(DataPrepperExpressionParser.SetInitializerContext ctx) {

    }

    @Override
    public void enterUnaryNotOperatorExpression(DataPrepperExpressionParser.UnaryNotOperatorExpressionContext ctx) {

    }

    @Override
    public void exitUnaryNotOperatorExpression(DataPrepperExpressionParser.UnaryNotOperatorExpressionContext ctx) {
        if (operatorSymbolStack.isEmpty() || operatorSymbolStack.peek() != NOT) {
            return;
        }
        performSingleOperation(1);
    }

    @Override
    public void enterUnaryOperator(DataPrepperExpressionParser.UnaryOperatorContext ctx) {

    }

    @Override
    public void exitUnaryOperator(DataPrepperExpressionParser.UnaryOperatorContext ctx) {
        // TODO: verify unary operator on operatorSymbolStack top
    }

    @Override
    public void enterPrimary(DataPrepperExpressionParser.PrimaryContext ctx) {

    }

    @Override
    public void exitPrimary(DataPrepperExpressionParser.PrimaryContext ctx) {

    }

    @Override
    public void enterJsonPointer(DataPrepperExpressionParser.JsonPointerContext ctx) {

    }

    @Override
    public void exitJsonPointer(DataPrepperExpressionParser.JsonPointerContext ctx) {

    }

    @Override
    public void enterVariableIdentifier(DataPrepperExpressionParser.VariableIdentifierContext ctx) {

    }

    @Override
    public void exitVariableIdentifier(DataPrepperExpressionParser.VariableIdentifierContext ctx) {

    }

    @Override
    public void enterVariableName(DataPrepperExpressionParser.VariableNameContext ctx) {

    }

    @Override
    public void exitVariableName(DataPrepperExpressionParser.VariableNameContext ctx) {

    }

    @Override
    public void enterLiteral(DataPrepperExpressionParser.LiteralContext ctx) {

    }

    @Override
    public void exitLiteral(DataPrepperExpressionParser.LiteralContext ctx) {

    }

    @Override
    public void visitTerminal(TerminalNode node) {
        final int nodeType = node.getSymbol().getType();
        if (nodeType == DataPrepperExpressionParser.EOF) {
            return;
        }
        if (strategy.containsKey(nodeType) || nodeType == DataPrepperExpressionParser.LPAREN) {
            operatorSymbolStack.push(nodeType);
        } else if (nodeType == DataPrepperExpressionParser.RPAREN) {
            // pop LPAREN at operatorSymbolStack top
            operatorSymbolStack.pop();
        } else {
            try {
                final Object arg = coercionService.coerceTerminalNode(node, context);
                argStack.push(arg);
            } catch (final CoercionException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public void visitErrorNode(ErrorNode node) {

    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {

    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {

    }

    private void performSingleOperation(final int numOfArgs) {
        final Integer operatorType = operatorSymbolStack.pop();
        final Operator<?> operator = strategy.get(operatorType);
        final Object[] args = new Object[numOfArgs];
        for (int i = numOfArgs - 1; i >= 0; i--) {
            args[i] = argStack.pop();
        }
        final Object result = operator.evaluate(args);
        argStack.push(result);
    }
}
