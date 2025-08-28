/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.model.event.Event;

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
    private final Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions;
    private final ExpressionFunctionProvider expressionFunctionProvider;
    private final Function<Object, Object> convertLiteralType;
    private final ConcurrentMap<String, FunctionMetadata> cachedFunctionStrings = new ConcurrentHashMap<>();

    public Object coercePrimaryTerminalNode(final TerminalNode node, final Event event) {
        Objects.requireNonNull(node, "TerminalNode cannot be null");
        final int nodeType = node.getSymbol().getType();
        final String nodeStringValue = node.getText();
        switch (nodeType) {
            case DataPrepperExpressionParser.Function:
                cachedFunctionStrings.computeIfAbsent(nodeStringValue, this::parseFunctionMetadata);
                FunctionMetadata functionMetadata = cachedFunctionStrings.get(nodeStringValue);
                return expressionFunctionProvider.provideFunction(functionMetadata.functionName, functionMetadata.argList, event, convertLiteralType);
            case DataPrepperExpressionParser.EscapedJsonPointer:
                final String jsonPointerWithoutQuotes = nodeStringValue.substring(1, nodeStringValue.length() - 1);
                return resolveJsonPointerValue(jsonPointerWithoutQuotes, event);
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
            case DataPrepperExpressionParser.SET_DELIMITER:
                return nodeType;
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
            final ExpressionFunctionProvider expressionFunctionProvider) {
        this.literalTypeConversions = literalTypeConversions;
        convertLiteralType = (value) -> {
            if (literalTypeConversions.containsKey(value.getClass())) {
                return literalTypeConversions.get(value.getClass()).apply(value);
            } else {
                throw new ExpressionCoercionException("Unsupported type for value " + value);
            }
        };
        this.expressionFunctionProvider = expressionFunctionProvider;
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
            throw new ExpressionCoercionException("Invalid function format: missing opening parenthesis");
        }
        final String functionName = nodeStringValue.substring(0, funcNameIndex);
        final int argsEndIndex = nodeStringValue.indexOf(")", funcNameIndex);
        if (argsEndIndex == -1) {
            throw new ExpressionCoercionException("Invalid function format: missing closing parenthesis");
        }

        List<Object> argList = new ArrayList<>();
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
                                "Invalid string argument: check if any argument is missing a closing double quote or contains comma that's not escaped with `\\`.");
                    }
                    argList.add(trimmedArg);
                } else {
                    throw new ExpressionCoercionException("Unsupported type passed as function argument");
                }
            }
        }

        FunctionMetadata metadata = new FunctionMetadata();
        metadata.functionName = functionName;
        metadata.argList = argList;
        return metadata;
    }

    private static class FunctionMetadata {
        String functionName;
        List<Object> argList;
    }

    private Object resolveJsonPointerValue(final String jsonPointer, final Event event) {
        final Object value = event.get(jsonPointer, Object.class);
        if (value == null) {
            return null;
        }
        return convertLiteralType.apply(value);
    }
}
