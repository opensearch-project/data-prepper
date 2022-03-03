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
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

@Named
public class OperatorFactory {
    public final BiPredicate<Object, Object> regexEquals = (x, y) -> ((String) x).matches((String) y);
    public final BiPredicate<Object, Object> equals = Objects::equals;
    public final BiPredicate<Object, Object> inSet = (x, y) -> ((Set<?>) y).contains(x);

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
                    put(Integer.class, (lhs, rhs) -> (Float) lhs > (Integer) rhs);
                    put(Float.class, (lhs, rhs) -> (Float) lhs > (Float) rhs);}};

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
                    put(Integer.class, (lhs, rhs) -> (Float) lhs >= (Integer) rhs);
                    put(Float.class, (lhs, rhs) -> (Float) lhs >= (Float) rhs);}};

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
                    put(Integer.class, (lhs, rhs) -> (Float) lhs < (Integer) rhs);
                    put(Float.class, (lhs, rhs) -> (Float) lhs < (Float) rhs);}};

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
                    put(Integer.class, (lhs, rhs) -> (Float) lhs <= (Integer) rhs);
                    put(Float.class, (lhs, rhs) -> (Float) lhs <= (Float) rhs);}};

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);

        return new NumericCompareOperator(DataPrepperExpressionParser.LTE, operandsToOperationMap);
    }

    @Bean
    public GenericRegexMatchOperator regexEqualOperator() {
        return new GenericRegexMatchOperator(DataPrepperExpressionParser.MATCH_REGEX_PATTERN, regexEquals);
    }

    @Bean
    public GenericRegexMatchOperator regexNotEqualOperator() {
        return new GenericRegexMatchOperator(DataPrepperExpressionParser.NOT_MATCH_REGEX_PATTERN, regexEquals.negate());
    }

    @Bean
    public GenericEqualOperator equalOperator() {
        return new GenericEqualOperator(DataPrepperExpressionParser.EQUAL, equals);
    }

    @Bean
    public GenericEqualOperator notEqualOperator() {
        return new GenericEqualOperator(DataPrepperExpressionParser.NOT_EQUAL, equals.negate());
    }

    @Bean
    public GenericInSetOperator inSetOperator() {
        return new GenericInSetOperator(DataPrepperExpressionParser.IN_SET, inSet);
    }

    @Bean
    public GenericInSetOperator notInSetOperator() {
        return new GenericInSetOperator(DataPrepperExpressionParser.NOT_IN_SET, inSet.negate());
    }
}
