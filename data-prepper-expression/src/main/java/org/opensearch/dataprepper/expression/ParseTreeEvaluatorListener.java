/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

    /**
     * Track function context for composable function support
     */
    private final Stack<FunctionEvalContext> functionContextStack = new Stack<>();
    /**
     * Track whether we are directly inside a functionArg that contains only a jsonPointer
     */
    private int functionArgDepth = 0;

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

    private boolean isInsideFunction() {
        return !functionContextStack.isEmpty();
    }

    @Override
    public void enterFunction(DataPrepperExpressionParser.FunctionContext ctx) {
        final String functionName = ctx.FunctionName().getText();
        functionContextStack.push(new FunctionEvalContext(functionName, operandStack.size()));
    }

    @Override
    public void exitFunction(DataPrepperExpressionParser.FunctionContext ctx) {
        final FunctionEvalContext funcCtx = functionContextStack.pop();

        // Collect all arguments that were pushed onto the operand stack during function arg evaluation
        final List<Object> args = new ArrayList<>();
        while (operandStack.size() > funcCtx.stackSizeAtEntry) {
            args.add(0, operandStack.pop());
        }

        final Object result = coercionService.evaluateFunction(funcCtx.functionName, args, event);
        operandStack.push(result);
    }

    @Override
    public void enterFunctionArg(DataPrepperExpressionParser.FunctionArgContext ctx) {
        functionArgDepth++;
    }

    @Override
    public void exitFunctionArg(DataPrepperExpressionParser.FunctionArgContext ctx) {
        functionArgDepth--;
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        final int nodeType = node.getSymbol().getType();
        if (nodeType == DataPrepperExpressionParser.EOF) {
            return;
        }

        // Skip FunctionName, LPAREN, RPAREN, and COMMA tokens inside function rules
        // These are structural tokens handled by enter/exitFunction
        if (nodeType == DataPrepperExpressionParser.FunctionName) {
            return;
        }

        if (isInsideFunction()) {
            // Inside a function context, LPAREN/RPAREN/COMMA are structural - skip them
            if (nodeType == DataPrepperExpressionParser.LPAREN ||
                nodeType == DataPrepperExpressionParser.RPAREN ||
                nodeType == DataPrepperExpressionParser.COMMA) {
                return;
            }

            // Inside a function arg, bare JsonPointer (e.g. /field) should be converted to EventKey.
            // EscapedJsonPointer (e.g. "/key1") is a quoted value — treat it as a string literal,
            // not an EventKey. This ensures getMetadata("/key1") receives a String argument.
            if (functionArgDepth > 0) {
                if (nodeType == DataPrepperExpressionParser.JsonPointer) {
                    operandStack.push(coercionService.createEventKey(node.getText()));
                    return;
                }
                if (nodeType == DataPrepperExpressionParser.EscapedJsonPointer) {
                    final String nodeStringValue = node.getText();
                    // Strip surrounding quotes and push as String
                    operandStack.push(nodeStringValue.substring(1, nodeStringValue.length() - 1));
                    return;
                }
            }
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
                if (!(arg instanceof Integer) || (((int)arg) != DataPrepperExpressionParser.COMMA)) {
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
                        if (e instanceof IllegalArgumentException && op.isBooleanOperator()) {
                            operandStack.push(false);
                        } else {
                            throw new ExpressionEvaluationException("Unable to evaluate the part of input statement: "
                                + getPartialStatementFromContext(ctx), e);
                        }
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

    private static final class FunctionEvalContext {
        final String functionName;
        final int stackSizeAtEntry;

        FunctionEvalContext(final String functionName, final int stackSizeAtEntry) {
            this.functionName = functionName;
            this.stackSizeAtEntry = stackSizeAtEntry;
        }
    }
}
