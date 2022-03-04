/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import javax.inject.Named;

@Named
public class CoercionService {
    public Object coerceTerminalNode(final TerminalNode node, final Event event) throws CoercionException {
        final int nodeType = node.getSymbol().getType();
        final String nodeStringValue = node.getText();
        switch (nodeType) {
            case DataPrepperExpressionParser.JsonPointer:
                return event.get(node.getText(), Object.class);
            case DataPrepperExpressionParser.EscapedJsonPointer:
            case DataPrepperExpressionParser.String:
                return nodeStringValue;
            case DataPrepperExpressionParser.Integer:
                return Integer.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Float:
                return Float.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Boolean:
                return Boolean.valueOf(nodeStringValue);
            default:
                throw new CoercionException("Unsupported terminal node type symbol string: " +
                        DataPrepperExpressionParser.VOCABULARY.getDisplayName(nodeType));
        }
    }
}
