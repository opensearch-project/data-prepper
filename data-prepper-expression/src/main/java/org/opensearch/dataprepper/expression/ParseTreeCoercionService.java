/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import javax.inject.Named;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Named
class ParseTreeCoercionService {
    private static final Map<Class<? extends Serializable>, Function<Object, Object>> literalTypes;

    static {
        literalTypes = new HashMap<>();
        literalTypes.put(String.class, Function.identity());
        literalTypes.put(Boolean.class, Function.identity());
        literalTypes.put(Integer.class, Function.identity());
        literalTypes.put(Float.class, Function.identity());
        literalTypes.put(Double.class, v -> ((Double) v).floatValue());
    }

    public Object coercePrimaryTerminalNode(final TerminalNode node, final Event event) {
        final int nodeType = node.getSymbol().getType();
        final String nodeStringValue = node.getText();
        switch (nodeType) {
            case DataPrepperExpressionParser.EscapedJsonPointer:
                final String jsonPointerWithoutQuotes = nodeStringValue.substring(1, nodeStringValue.length() - 1);
                return resolveJsonPointerValue(jsonPointerWithoutQuotes, event);
            case DataPrepperExpressionParser.JsonPointer:
                return resolveJsonPointerValue(nodeStringValue, event);
            case DataPrepperExpressionParser.String:
                return nodeStringValue;
            case DataPrepperExpressionParser.Integer:
                return Integer.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Float:
                return Float.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Boolean:
                return Boolean.valueOf(nodeStringValue);
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
        } else if (literalTypes.containsKey(value.getClass())) {
            return literalTypes.get(value.getClass()).apply(value);
        } else {
            throw new ExpressionCoercionException("Unsupported type for value " + value);
        }
    }
}
