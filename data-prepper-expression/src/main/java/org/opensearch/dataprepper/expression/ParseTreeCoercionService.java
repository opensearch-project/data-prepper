/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Pattern;

@Named
class ParseTreeCoercionService {
    private static final Pattern QUOTE_PATTERN = Pattern.compile("^\"{1,3}|\"{1,3}$");
    private static final Pattern ARGUMENT_SPLITTER = Pattern.compile("(?<!\\\\),");
    private static final int INITIAL_ARG_LIST_SIZE = 8;
    private static final String INVALID_FUNCTION_FORMAT_OPEN = "Invalid function format: missing opening parenthesis";
    private static final String INVALID_FUNCTION_FORMAT_CLOSE = "Invalid function format: missing closing parenthesis";
    private static final String INVALID_STRING_ARG = "Invalid string argument: check if any argument is missing a closing double quote or contains comma that's not escaped with `\\`.";
    private static final String UNSUPPORTED_ARG_TYPE = "Unsupported type passed as function argument";
    private final Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions;
    private final ExpressionFunctionProvider expressionFunctionProvider;
    private final Function<Object, Object> convertLiteralType;
    private final ConcurrentMap<String, FunctionMetadata> cachedFunctionStrings = new ConcurrentHashMap<>(16, 0.75f);
    private final EventKeyFactory eventKeyFactory;

    public Object coercePrimaryTerminalNode(final TerminalNode node, final Event event) {
        Objects.requireNonNull(node, "TerminalNode cannot be null");
        final int nodeType = node.getSymbol().getType();
        final String nodeStringValue = node.getText();
        switch (nodeType) {
            case DataPrepperExpressionParser.Function:
                FunctionMetadata functionMetadata = cachedFunctionStrings.computeIfAbsent(nodeStringValue, this::parseFunctionMetadata);
                return expressionFunctionProvider.provideFunction(functionMetadata.functionName, functionMetadata.argList, event, convertLiteralType);
            case DataPrepperExpressionParser.EscapedJsonPointer:
                return resolveJsonPointerValue(nodeStringValue.substring(1, nodeStringValue.length() - 1), event);
            case DataPrepperExpressionParser.JsonPointer:
                return resolveJsonPointerValue(nodeStringValue, event);
            case DataPrepperExpressionParser.String:
                return QUOTE_PATTERN.matcher(nodeStringValue).replaceAll("");
            case DataPrepperExpressionParser.Integer:
                try {
                    return Integer.valueOf(nodeStringValue);
                } catch (NumberFormatException e) {
                    return Long.valueOf(nodeStringValue);
                }
            case DataPrepperExpressionParser.Float:
                return Float.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Boolean:
                return Boolean.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.COMMA:
                return DataPrepperExpressionParser.COMMA;
            case DataPrepperExpressionParser.SET_DELIMITER:
                return DataPrepperExpressionParser.SET_DELIMITER;
            case DataPrepperExpressionParser.Null:
                return null;
            case DataPrepperExpressionParser.DataTypes:
                return nodeStringValue;

            default:
                throw new ExpressionCoercionException("Unsupported terminal node type symbol string: " +
                        DataPrepperExpressionParser.VOCABULARY.getDisplayName(nodeType));
        }
    }

    @Inject
    public ParseTreeCoercionService(
            final Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions,
            final ExpressionFunctionProvider expressionFunctionProvider,
            final EventKeyFactory eventKeyFactory) {
        Objects.requireNonNull(literalTypeConversions, "literalTypeConversions cannot be null");
        Objects.requireNonNull(expressionFunctionProvider, "expressionFunctionProvider cannot be null");
        this.literalTypeConversions = literalTypeConversions;
        convertLiteralType = (value) -> {
            if (literalTypeConversions.containsKey(value.getClass())) {
                return literalTypeConversions.get(value.getClass()).apply(value);
            } else {
                throw new ExpressionCoercionException("Unsupported type for value " + value);
            }
        };
        this.expressionFunctionProvider = expressionFunctionProvider;
        this.eventKeyFactory = eventKeyFactory;
    }

    public <T> T coerce(final Object obj, Class<T> clazz) throws ExpressionCoercionException {
        if (obj.getClass().isAssignableFrom(clazz)) {
            return (T) obj;
        }
        throw new ExpressionCoercionException(
                "Unable to cast " + obj.getClass().getName() + " into " + clazz.getName());
    }

    private FunctionMetadata parseFunctionMetadata(final String nodeStringValue) {
        final int funcNameIndex = nodeStringValue.indexOf("(");
        if (funcNameIndex == -1) {
            throw new ExpressionCoercionException(INVALID_FUNCTION_FORMAT_OPEN);
        }
        final String functionName = nodeStringValue.substring(0, funcNameIndex);
        final int argsEndIndex = nodeStringValue.indexOf(")", funcNameIndex);
        if (argsEndIndex == -1) {
            throw new ExpressionCoercionException(INVALID_FUNCTION_FORMAT_CLOSE);
        }

        List<Object> argList = new ArrayList<>(INITIAL_ARG_LIST_SIZE);
        if (argsEndIndex > funcNameIndex + 1) {
            final String argsStr = nodeStringValue.substring(funcNameIndex + 1, argsEndIndex);
            final String[] args = ARGUMENT_SPLITTER.split(argsStr);

            for (final String arg : args) {
                String trimmedArg = arg.trim();
                if (trimmedArg.isEmpty()) {
                    continue;
                }
                if (trimmedArg.charAt(0) == '/') {
                    argList.add(trimmedArg);
                } else if (trimmedArg.charAt(0) == '"') {
                    if (trimmedArg.length() < 2 || trimmedArg.charAt(trimmedArg.length() - 1) != '"') {
                        throw new ExpressionCoercionException(
                                INVALID_STRING_ARG);
                    }
                    argList.add(trimmedArg);
                } else {
                    try {
                        argList.add(Integer.parseInt(trimmedArg));
                        continue;
                    } catch (final Exception e) {

                    }
                    throw new ExpressionCoercionException(UNSUPPORTED_ARG_TYPE);
                }
            }
        }

        return new FunctionMetadata(functionName, argList);
    }

    private static final class FunctionMetadata {
        final String functionName;
        final List<Object> argList;

        private FunctionMetadata(final String functionName, final List<Object> argList) {
            this.functionName = functionName.intern();
            this.argList = argList;
        }
    }

    private Object resolveJsonPointerValue(final String jsonPointer, final Event event) {
        EventKey eventKey = this.eventKeyFactory.createEventKey(jsonPointer);
        final Object value = event.get(eventKey, Object.class);
        return value != null ? convertLiteralType.apply(value) : null;
    }
}