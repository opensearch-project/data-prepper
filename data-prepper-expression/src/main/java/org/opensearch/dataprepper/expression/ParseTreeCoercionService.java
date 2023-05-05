/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

@Named
class ParseTreeCoercionService {
    private final Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions;

    @Inject
    public ParseTreeCoercionService(final Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions) {
        this.literalTypeConversions = literalTypeConversions;
    }

    public Object applyFunction(final String functionName, final String[] args, Event event) {
        // Supports length(String) and length(JsonPointer)
        // For example., length("abcd"), length("/keyName")
        if (functionName.equals("length")) {
            if (args.length > 1) {
                throw new RuntimeException("length() takes only one argument");
            }
            String arg = args[0].trim();
            if (arg.charAt(0) == '\"') {
                if (arg.charAt(arg.length()-1) != '\"') {
                    throw new RuntimeException("Invalid string passed to length()");
                }
                return Integer.valueOf(arg.length()-2);
            } else if (arg.charAt(0) == '/') {
                String s = (String)resolveJsonPointerValue(arg, event);
                if (s != null) {
                    return Integer.valueOf(s.length());
                }
            } else {
                throw new RuntimeException("Unexpected argument to length()");
            }
        } else {
            throw new RuntimeException("Unknown function");
        }
        return null;
    }

    public Object coercePrimaryTerminalNode(final TerminalNode node, final Event event) {
        final int nodeType = node.getSymbol().getType();
        final String nodeStringValue = node.getText();
        switch (nodeType) {
            case DataPrepperExpressionParser.Function:
                final int funcNameIndex = nodeStringValue.indexOf("(");
                final String functionName = nodeStringValue.substring(0, funcNameIndex);
                final int argsEndIndex = nodeStringValue.indexOf(")", funcNameIndex);
                final String argsStr = nodeStringValue.substring(funcNameIndex+1, argsEndIndex);
                final String[] args = argsStr.split(",");
                return applyFunction(functionName, args, event);
            case DataPrepperExpressionParser.EscapedJsonPointer:
                final String jsonPointerWithoutQuotes = nodeStringValue.substring(1, nodeStringValue.length() - 1);
                return resolveJsonPointerValue(jsonPointerWithoutQuotes, event);
            case DataPrepperExpressionParser.JsonPointer:
                return resolveJsonPointerValue(nodeStringValue, event);
            case DataPrepperExpressionParser.String:
                final String nodeStringValueWithQuotesStripped = nodeStringValue.substring(1, nodeStringValue.length() - 1);
                return nodeStringValueWithQuotesStripped;
            case DataPrepperExpressionParser.Integer:
                Long longValue = Long.valueOf(nodeStringValue);
                if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
                    return longValue;
                }
                return Integer.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Float:
                return Float.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Boolean:
                return Boolean.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Null:
                return null;
            default:
                throw new ExpressionCoercionException("Unsupported terminal node type symbol string: " +
                        DataPrepperExpressionParser.VOCABULARY.getDisplayName(nodeType));
        }
    }

    public <T> T coerce(final Object obj, Class<T> clazz) throws ExpressionCoercionException {
        if (obj.getClass().isAssignableFrom(clazz)) {
            return (T) obj;
        }
        throw new ExpressionCoercionException("Unable to cast " + obj.getClass().getName() + " into " + clazz.getName());
    }

    private Object resolveJsonPointerValue(final String jsonPointer, final Event event) {
        final Object value = event.get(jsonPointer, Object.class);
        if (value == null) {
            return null;
        } else if (literalTypeConversions.containsKey(value.getClass())) {
            return literalTypeConversions.get(value.getClass()).apply(value);
        } else {
            throw new ExpressionCoercionException("Unsupported type for value " + value);
        }
    }
}
