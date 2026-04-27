/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

@Named
class ParseTreeCoercionService {
    private static final Pattern QUOTE_PATTERN = Pattern.compile("^\"{1,3}|\"{1,3}$");
    private final ExpressionFunctionProvider expressionFunctionProvider;
    private final Function<Object, Object> convertLiteralType;
    private final EventKeyFactory eventKeyFactory;

    public Object coercePrimaryTerminalNode(final TerminalNode node, final Event event) {
        Objects.requireNonNull(node, "TerminalNode cannot be null");
        final int nodeType = node.getSymbol().getType();
        final String nodeStringValue = node.getText();
        switch (nodeType) {
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
            case DataPrepperExpressionParser.Null:
                return null;
            case DataPrepperExpressionParser.DataTypes:
                return nodeStringValue;
            default:
                throw new ExpressionCoercionException("Unsupported terminal node type symbol string: " +
                        DataPrepperExpressionParser.VOCABULARY.getDisplayName(nodeType));
        }
    }

    public Object evaluateFunction(final String functionName, final List<Object> args, final Event event) {
        return expressionFunctionProvider.provideFunction(functionName, args, event, convertLiteralType);
    }

    public EventKey createEventKey(final String jsonPointer) {
        return eventKeyFactory.createEventKey(jsonPointer);
    }

    @Inject
    public ParseTreeCoercionService(
            final Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions,
            final ExpressionFunctionProvider expressionFunctionProvider,
            final EventKeyFactory eventKeyFactory) {
        Objects.requireNonNull(literalTypeConversions, "literalTypeConversions cannot be null");
        Objects.requireNonNull(expressionFunctionProvider, "expressionFunctionProvider cannot be null");
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

    Object resolveJsonPointerValue(final String jsonPointer, final Event event) {
        EventKey eventKey = this.eventKeyFactory.createEventKey(jsonPointer);
        final Object value = event.get(eventKey, Object.class);
        return value != null ? convertLiteralType.apply(value) : null;
    }
}
