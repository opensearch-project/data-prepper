/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionBaseListener;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionListener;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

/**
 * @since 1.3
 * This listener implements {@link DataPrepperExpressionListener} to provide callbacks to handle evaluation of
 * {@link org.antlr.v4.runtime.tree.ParseTree} representation of an expression while {@link org.antlr.v4.runtime.tree.ParseTreeWalker}
 * traverses through the {@link org.antlr.v4.runtime.tree.ParseTree}.
 *
 * Use case:
 * ParseTreeWalker walker = new ParseTreeWalker();
 * ParseTreeEvaluatorListener listener = new ParseTreeEvaluatorListener(...);
 * walker.walk(listener, ...);
 * final Object result = listener.getResult();
 */
class ParseTreeEvaluatorListener extends DataPrepperExpressionBaseListener {

    private final OperatorProvider operatorProvider;
    private final ParseTreeCoercionService coercionService;
    private final Stack<Integer> operatorSymbolStack;
    private final Stack<Object> operandStack;
    private final Event event;
    private boolean listStart;
    private Set<Object> setMembers;

    public ParseTreeEvaluatorListener(final OperatorProvider operatorProvider,
                                      final ParseTreeCoercionService coercionService,
                                      final Event event) {
        this.coercionService = coercionService;
        this.operatorProvider = operatorProvider;
        this.event = event;
        this.listStart = false;
        operatorSymbolStack = new Stack<>();
        operandStack = new Stack<>();
    }

    public Object getResult() {
        if (operandStack.size() != 1) {
            throw new IllegalStateException("The ParseTreeEvaluatorListener has not been walked through exactly once by " +
                    "a ParseTreeWalker.");
        }
        return operandStack.peek();
    }

    private void validateSetMembers(Set<Object> setMembers) {
        int numbers = 0;
        int strings = 0;
        int booleans = 0;
        for (Object member: setMembers) {
            if (member instanceof Number) {
                numbers++;
            } else if (member instanceof String) {
                strings++;
            } else if (member instanceof Boolean) {
                booleans++;
            }
        }
        if (numbers != setMembers.size() && strings != setMembers.size() && booleans != setMembers.size()) {
            throw new RuntimeException("All set members should be of same type");
        }
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        final int nodeType = node.getSymbol().getType();
        if (nodeType == DataPrepperExpressionParser.EOF) {
            return;
        }
        if (operatorProvider.containsOperator(nodeType) || nodeType == DataPrepperExpressionParser.LPAREN) {
            operatorSymbolStack.push(nodeType);
        } else if (nodeType == DataPrepperExpressionParser.LBRACE) {
            listStart = true;
            setMembers = new HashSet<>();
        } else if (nodeType == DataPrepperExpressionParser.RBRACE) {
            listStart = false;
            validateSetMembers(setMembers);
            operandStack.push(setMembers);
        } else if (nodeType == DataPrepperExpressionParser.RPAREN) {
            // pop LPAREN at operatorSymbolStack top
            operatorSymbolStack.pop();
        } else {
            final Object arg = coercionService.coercePrimaryTerminalNode(node, event);
            if (listStart) {
                if (!(arg instanceof Integer) || (((int)arg) != DataPrepperExpressionParser.COMMA && ((int)arg) != DataPrepperExpressionParser.SET_DELIMITER)) {
                    setMembers.add(arg);
                }
            } else {
                operandStack.push(arg);
            }
        }
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        throw new RuntimeException("Hit error node in the parse tree: " + node.getText());
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        if (!operatorSymbolStack.isEmpty()) {
            final int operatorSymbol = operatorSymbolStack.peek();
            if (operatorSymbol != DataPrepperExpressionParser.LPAREN) {
                final Operator<?> op = operatorProvider.getOperator(operatorSymbol);
                if (op.shouldEvaluate(ctx)) {
                    operatorSymbolStack.pop();
                    try {
                        performSingleOperation(op, ctx);
                    } catch (final Exception e) {
                        throw new ExpressionEvaluationException("Unable to evaluate the part of input statement: "
                                + getPartialStatementFromContext(ctx), e);
                    }
                }
            }
        }
    }

    private void performSingleOperation(final Operator<?> operator, final ParserRuleContext ctx) {
        final int numOfArgs = operator.getNumberOfOperands(ctx);
        final Object[] args = new Object[numOfArgs];
        for (int i = numOfArgs - 1; i >= 0; i--) {
            args[i] = operandStack.pop();
        }
        final Object result = operator.evaluate(args);
        operandStack.push(result);
    }

    private String getPartialStatementFromContext(final ParserRuleContext ctx) {
        final Token startToken = ctx.getStart();
        final Token stopToken = ctx.getStop();
        final String fullStatement = startToken.getInputStream().toString();
        return fullStatement.substring(startToken.getStartIndex(), stopToken.getStopIndex() + 1);
    }
}
