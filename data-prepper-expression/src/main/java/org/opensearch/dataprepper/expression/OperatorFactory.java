/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.springframework.context.annotation.Bean;

import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

@Named
public class OperatorFactory {

    @Bean
    public Operator<Boolean> notEqualOperator(final EqualOperator equalOperator) {
        return new NegateOperator(DataPrepperExpressionParser.NOT_EQUAL, equalOperator);
    }

    @Bean
    public Operator<Boolean> notInOperator(final InOperator inOperator) {
        return new NegateOperator(DataPrepperExpressionParser.NOT_IN_SET, inOperator);
    }

    @Bean
    public Operator<Boolean> regexNotEqualOperator(final RegexEqualOperator regexEqualOperator) {
        return new NegateOperator(DataPrepperExpressionParser.NOT_MATCH_REGEX_PATTERN, regexEqualOperator);
    }

    @Bean
    public NumericCompareOperator greaterThanOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> intOperations =
                new HashMap<Class<? extends Number>, BiFunction<Object, Object, Boolean>>() {{
                    put(Integer.class, (lhs, rhs) -> (Integer) lhs > (Integer) rhs);
                    put(Float.class, (lhs, rhs) -> (Integer) lhs > (Float) rhs);}};
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> floatOperations =
                new HashMap<Class<? extends Number>, BiFunction<Object, Object, Boolean>>() {{
                    put(Float.class, (lhs, rhs) -> (Float) lhs > (Integer) rhs);
                    put(Integer.class, (lhs, rhs) -> (Float) lhs > (Integer) rhs);}};

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);

        return new NumericCompareOperator(DataPrepperExpressionParser.GT, operandsToOperationMap);
    }

    @Bean
    public NumericCompareOperator greaterThanOrEqualOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> intOperations =
                new HashMap<Class<? extends Number>, BiFunction<Object, Object, Boolean>>() {{
            put(Integer.class, (lhs, rhs) -> (Integer) lhs >= (Integer) rhs);
            put(Float.class, (lhs, rhs) -> (Integer) lhs >= (Float) rhs);}};
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> floatOperations =
                new HashMap<Class<? extends Number>, BiFunction<Object, Object, Boolean>>() {{
                    put(Float.class, (lhs, rhs) -> (Float) lhs >= (Integer) rhs);
                    put(Integer.class, (lhs, rhs) -> (Float) lhs >= (Integer) rhs);}};

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);

        return new NumericCompareOperator(DataPrepperExpressionParser.GTE, operandsToOperationMap);
    }

    @Bean
    public NumericCompareOperator lessThanOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> intOperations =
                new HashMap<Class<? extends Number>, BiFunction<Object, Object, Boolean>>() {{
                    put(Integer.class, (lhs, rhs) -> (Integer) lhs < (Integer) rhs);
                    put(Float.class, (lhs, rhs) -> (Integer) lhs < (Float) rhs);}};
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> floatOperations =
                new HashMap<Class<? extends Number>, BiFunction<Object, Object, Boolean>>() {{
                    put(Float.class, (lhs, rhs) -> (Float) lhs < (Integer) rhs);
                    put(Integer.class, (lhs, rhs) -> (Float) lhs < (Integer) rhs);}};

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);

        return new NumericCompareOperator(DataPrepperExpressionParser.LT, operandsToOperationMap);
    }

    @Bean
    public NumericCompareOperator lessThanOrEqualOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> intOperations =
                new HashMap<Class<? extends Number>, BiFunction<Object, Object, Boolean>>() {{
                    put(Integer.class, (lhs, rhs) -> (Integer) lhs <= (Integer) rhs);
                    put(Float.class, (lhs, rhs) -> (Integer) lhs <= (Float) rhs);}};
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> floatOperations =
                new HashMap<Class<? extends Number>, BiFunction<Object, Object, Boolean>>() {{
                    put(Float.class, (lhs, rhs) -> (Float) lhs <= (Integer) rhs);
                    put(Integer.class, (lhs, rhs) -> (Float) lhs <= (Integer) rhs);}};

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);

        return new NumericCompareOperator(DataPrepperExpressionParser.LTE, operandsToOperationMap);
    }
}
