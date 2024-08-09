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
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;

@Named
class ParseTreeCoercionService {
    private final Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions;
    private ExpressionFunctionProvider expressionFunctionProvider;
    private Function<Object, Object> convertLiteralType;

    @Inject
    public ParseTreeCoercionService(final Map<Class<? extends Serializable>, Function<Object, Object>> literalTypeConversions, ExpressionFunctionProvider expressionFunctionProvider) {
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

    public Object coercePrimaryTerminalNode(final TerminalNode node, final Event event) {
        final int nodeType = node.getSymbol().getType();
        final String nodeStringValue = node.getText();
        switch (nodeType) {
            case DataPrepperExpressionParser.Function:
                final int funcNameIndex = nodeStringValue.indexOf("(");
                final String functionName = nodeStringValue.substring(0, funcNameIndex);
                final int argsEndIndex = nodeStringValue.indexOf(")", funcNameIndex);
                final String argsStr = nodeStringValue.substring(funcNameIndex+1, argsEndIndex);
                // Split at commas if there's no backslash before the commas, because commas can be part of a function parameter
                final String[] args = argsStr.split("(?<!\\\\),");
                List<Object> argList = new ArrayList<>();
                for (final String arg: args) {
                    String trimmedArg = arg.trim();
                    if (trimmedArg.charAt(0) == '/') {
                        argList.add(trimmedArg);
                    } else if (trimmedArg.charAt(0) == '"') {
                        if (trimmedArg.length() < 2 || trimmedArg.charAt(trimmedArg.length()-1) != '"') {
                            throw new RuntimeException("Invalid string argument: check if any argument is missing a closing double quote or contains comma that's not escaped with `\\`.");
                        }
                        argList.add(trimmedArg);
                    } else {
                        throw new RuntimeException("Unsupported type passed as function argument");
                    }
                }
                return expressionFunctionProvider.provideFunction(functionName, argList, event, convertLiteralType);
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
            case DataPrepperExpressionParser.SetInitializer:
                String[] setMembers = nodeStringValue.trim().substring(1,nodeStringValue.length()-1).split(",");
                int stringMemberCount = 0;
                boolean floatMember = false;
                for (int i= 0; i < setMembers.length; i++) {
                    String s = setMembers[i].trim();
                    if (s.length() > 1 && s.charAt(0) == '"' && s.charAt(s.length()-1) == '"') {
                        stringMemberCount++;
                    } else if (s.contains(".")) {
                        floatMember = true;
                    }
                }
                if (stringMemberCount > 0 && stringMemberCount != setMembers.length) {
                    throw new RuntimeException("All set members should be of same type");
                }
                if (stringMemberCount == setMembers.length) {
                    Set<String> stringSet = Arrays.stream(setMembers)
                    .map(s -> {
                        s = s.trim();
                        return s.substring(1,s.length()-1);
                     })
                     .collect(Collectors.toSet());
                    return stringSet;
                } else if (floatMember) {
                    Set<Float> doubleSet = Arrays.stream(setMembers)
                    .map(s -> {
                        s = s.trim();
                        try {
                            return Float.parseFloat(s);
                        } catch (NumberFormatException e) {
                            throw new RuntimeException("Invalid Float member");
                        }
                     })
                     .collect(Collectors.toSet());
                     return doubleSet;
                } else {
                    Set<Integer> integerSet = Arrays.stream(setMembers)
                    .map(s -> {
                        s = s.trim();
                        try {
                            return Integer.parseInt(s);
                        } catch (NumberFormatException e) {
                            throw new RuntimeException("Invalid Integer member");
                        }
                     })
                     .collect(Collectors.toSet());
                     return integerSet;
                }

            case DataPrepperExpressionParser.Float:
                return Float.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Boolean:
                return Boolean.valueOf(nodeStringValue);
            case DataPrepperExpressionParser.Null:
                return null;
            case DataPrepperExpressionParser.DataTypes:
                return nodeStringValue;

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
        } 
        return convertLiteralType.apply(value);
    }
}
