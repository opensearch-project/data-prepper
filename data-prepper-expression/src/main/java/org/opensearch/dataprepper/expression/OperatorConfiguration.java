/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.model.event.DataType;
import org.opensearch.dataprepper.model.pattern.Pattern;
import org.springframework.context.annotation.Bean;

import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

@Named
class OperatorConfiguration {
    public final BiPredicate<Object, Object> regexEquals = (x, y) -> Pattern.compile((String) y).matcher((String) x).matches();
    public final BiPredicate<Object, Object> equals = Objects::equals;
    public final BiPredicate<Object, Object> inSet = (x, y) -> ((Set<?>) y).contains(x);
    public final BiPredicate<Object, Object> typeOf = (x, y) -> DataType.isSameType(x, (String)y);

    @Bean
    public NumericCompareOperator greaterThanOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> intOperations =
                Map.of(
                        Integer.class, (lhs, rhs) -> (Integer) lhs > (Integer) rhs,
                        Float.class, (lhs, rhs) -> (Integer) lhs > (Float) rhs,
                        Long.class, (lhs, rhs) -> (Integer) lhs > (Long) rhs,
                        Double.class, (lhs, rhs) -> (Integer) lhs > (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> floatOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Float) lhs > (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Float) lhs > (Float) rhs,
                    Long.class, (lhs, rhs) -> (Float) lhs > (Long) rhs,
                    Double.class, (lhs, rhs) -> (Float) lhs > (Double) rhs
                );

        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> longOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Long) lhs > (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Long) lhs > (Float)rhs,
                    Long.class, (lhs, rhs) -> (Long) lhs > (Long) rhs,
                    Double.class, (lhs, rhs) -> (Long) lhs > (Double)rhs
                );

        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> doubleOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Double) lhs > (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Double) lhs > (Float) rhs,
                    Long.class, (lhs, rhs) -> (Double) lhs > (Long) rhs,
                    Double.class, (lhs, rhs) -> (Double) lhs > (Double) rhs
                );

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);
        operandsToOperationMap.put(Long.class, longOperations);
        operandsToOperationMap.put(Double.class, doubleOperations);

        return new NumericCompareOperator(DataPrepperExpressionParser.GT, operandsToOperationMap);
    }

    @Bean
    public NumericCompareOperator greaterThanOrEqualOperator() {
        final BiFunction<Object, Object, Boolean> doubleGreaterThanOrEquals = (lhs, rhs) -> ((Number)lhs).doubleValue() >= ((Number) rhs).doubleValue();
        final BiFunction<Object, Object, Boolean> floatGreaterThanOrEquals = (lhs, rhs) -> ((Number)lhs).floatValue() >= ((Number) rhs).floatValue();
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> intOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Integer) lhs >= (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Integer) lhs >= (Float) rhs,
                    Long.class, (lhs, rhs) -> (Integer) lhs >= (Long) rhs,
                    Double.class, (lhs, rhs) -> (Integer) lhs >= (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> floatOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Float) lhs >= (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Float) lhs >= (Float) rhs,
                    Long.class, (lhs, rhs) -> (Float) lhs >= (Long) rhs,
                    Double.class, floatGreaterThanOrEquals
                );

        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> longOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Long) lhs >= (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Long) lhs >= (Float) rhs,
                    Long.class, (lhs, rhs) -> (Long) lhs >= (Long) rhs,
                    Double.class, (lhs, rhs) -> (Long) lhs >= (Double)rhs
                );

        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> doubleOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Double) lhs >= (Integer) rhs,
                    Float.class, doubleGreaterThanOrEquals,
                    Long.class, (lhs, rhs) -> (Double) lhs >= (Long) rhs,
                    Double.class, doubleGreaterThanOrEquals
                );

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);
        operandsToOperationMap.put(Long.class, longOperations);
        operandsToOperationMap.put(Double.class, doubleOperations);

        return new NumericCompareOperator(DataPrepperExpressionParser.GTE, operandsToOperationMap);
    }

    @Bean
    public NumericCompareOperator lessThanOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> intOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Integer) lhs < (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Integer) lhs < (Float) rhs,
                    Long.class, (lhs, rhs) -> (Integer) lhs < (Long) rhs,
                    Double.class, (lhs, rhs) -> (Integer) lhs < (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> floatOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Float) lhs < (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Float) lhs < (Float) rhs,
                    Long.class, (lhs, rhs) -> (Float) lhs < (Long) rhs,
                    Double.class, (lhs, rhs) -> (Float) lhs < (Double) rhs
                );

        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> longOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Long) lhs < (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Long) lhs < (Float) rhs,
                    Long.class, (lhs, rhs) -> (Long) lhs < (Long) rhs,
                    Double.class, (lhs, rhs) -> (Long) lhs < (Double) rhs
                );

        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> doubleOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Double) lhs < (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Double) lhs < (Float) rhs,
                    Long.class, (lhs, rhs) -> (Double) lhs < (Long) rhs,
                    Double.class, (lhs, rhs) -> (Double) lhs < (Double) rhs
                );

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);
        operandsToOperationMap.put(Long.class, longOperations);
        operandsToOperationMap.put(Double.class, doubleOperations);

        return new NumericCompareOperator(DataPrepperExpressionParser.LT, operandsToOperationMap);
    }

    @Bean
    public NumericCompareOperator lessThanOrEqualOperator() {
        final BiFunction<Object, Object, Boolean> doubleLessThanOrEquals = (lhs, rhs) -> ((Number)lhs).doubleValue() <= ((Number) rhs).doubleValue();
        final BiFunction<Object, Object, Boolean> floatLessThanOrEquals = (lhs, rhs) -> ((Number)lhs).floatValue() <= ((Number) rhs).floatValue();
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> intOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Integer) lhs <= (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Integer) lhs <= (Float) rhs,
                    Long.class, (lhs, rhs) -> (Integer) lhs <= (Long) rhs,
                    Double.class, (lhs, rhs) -> (Integer) lhs <= (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> floatOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Float) lhs <= (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Float) lhs <= (Float) rhs,
                    Long.class, (lhs, rhs) -> (Float) lhs <= (Long) rhs,
                    Double.class, floatLessThanOrEquals
                );

        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> longOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Long) lhs <= (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Long) lhs <=  (Float) rhs,
                    Long.class, (lhs, rhs) -> (Long) lhs <= (Long) rhs,
                    Double.class, (lhs, rhs) -> (Long) lhs <=  (Double) rhs
                );

        final Map<Class<? extends Number>, BiFunction<Object, Object, Boolean>> doubleOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Double) lhs <= (Integer) rhs,
                    Float.class, doubleLessThanOrEquals,
                    Long.class, (lhs, rhs) -> (Double) lhs <= (Long) rhs,
                    Double.class, doubleLessThanOrEquals
                );

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);
        operandsToOperationMap.put(Long.class, longOperations);
        operandsToOperationMap.put(Double.class, doubleOperations);

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
        final BiPredicate<Object, Object> floatEquals = (lhs, rhs) -> ((Number)lhs).floatValue() == ((Number) rhs).floatValue();
        final BiPredicate<Object, Object> doubleEquals = (lhs, rhs) -> ((Number)lhs).doubleValue() == ((Number) rhs).doubleValue();

        final Map<Class<?>, Map<Class<?>, BiPredicate<Object, Object>>> equalStrategy = new HashMap<>();

        final Map<Class<?>, BiPredicate<Object, Object>> intOperations = new HashMap<>();
        intOperations.put(Integer.class, (lhs, rhs) -> (int) lhs == (int) rhs);
        intOperations.put(Float.class, floatEquals);
        intOperations.put(Long.class, (lhs, rhs) -> (int) lhs == (long) rhs);
        intOperations.put(Double.class, doubleEquals);

        final Map<Class<?>, BiPredicate<Object, Object>> floatOperations = new HashMap<>();
        floatOperations.put(Integer.class, floatEquals);
        floatOperations.put(Float.class, floatEquals);
        floatOperations.put(Long.class, floatEquals);
        floatOperations.put(Double.class, floatEquals);

        final Map<Class<?>, BiPredicate<Object, Object>> longOperations = new HashMap<>();
        longOperations.put(Integer.class, (lhs, rhs) -> (long) lhs == (int) rhs);
        longOperations.put(Float.class, floatEquals);
        longOperations.put(Long.class, (lhs, rhs) -> (long) lhs == (long) rhs);
        longOperations.put(Double.class, doubleEquals);

        final Map<Class<?>, BiPredicate<Object, Object>> doubleOperations = new HashMap<>();
        doubleOperations.put(Integer.class, doubleEquals);
        doubleOperations.put(Float.class, floatEquals);
        doubleOperations.put(Long.class, doubleEquals);
        doubleOperations.put(Double.class, doubleEquals);

        equalStrategy.put(Integer.class, intOperations);
        equalStrategy.put(Float.class, floatOperations);
        equalStrategy.put(Long.class, longOperations);
        equalStrategy.put(Double.class, doubleOperations);

        return new GenericEqualOperator(
                DataPrepperExpressionParser.EQUAL,
                DataPrepperExpressionParser.RULE_equalityOperatorExpression,
                equalStrategy
        );
    }

    @Bean
    public Operator<Boolean> notEqualOperator(final GenericEqualOperator equalOperator) {
        return new GenericNotOperator(
                DataPrepperExpressionParser.NOT_EQUAL,
                DataPrepperExpressionParser.RULE_equalityOperatorExpression,
                equalOperator
        );
    }

    @Bean
    public GenericInSetOperator inSetOperator() {
        return new GenericInSetOperator(DataPrepperExpressionParser.IN_SET, inSet);
    }

    @Bean
    public GenericInSetOperator notInSetOperator() {
        return new GenericInSetOperator(DataPrepperExpressionParser.NOT_IN_SET, inSet.negate());
    }

    @Bean
    public GenericTypeOfOperator typeOfOperator() {
        return new GenericTypeOfOperator(DataPrepperExpressionParser.TYPEOF, typeOf);
    }

    @Bean
    public AddBinaryOperator addOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> intOperations =
                Map.of(
                        Integer.class, (lhs, rhs) -> (Integer) lhs + (Integer) rhs,
                        Float.class, (lhs, rhs) -> (Integer) lhs + (Float) rhs,
                        Long.class, (lhs, rhs) -> (Integer) lhs + (Long) rhs,
                        Double.class, (lhs, rhs) -> (double)(int)lhs + (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> floatOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Float) lhs + (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Float) lhs + (Float) rhs,
                    Long.class, (lhs, rhs) -> (Float) lhs + (Long) rhs,
                    Double.class, (lhs, rhs) -> (Float) lhs + (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> longOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Long) lhs + (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Long) lhs + (Float)rhs,
                    Long.class, (lhs, rhs) -> (Long) lhs + (Long) rhs,
                    Double.class, (lhs, rhs) -> (Long) lhs + (Double)rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> doubleOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Double) lhs + (double)(int)rhs,
                    Float.class, (lhs, rhs) -> (Double) lhs + (Float) rhs,
                    Long.class, (lhs, rhs) -> (Double) lhs + (Long) rhs,
                    Double.class, (lhs, rhs) -> (Double) lhs + (Double) rhs
                );

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);
        operandsToOperationMap.put(Long.class, longOperations);
        operandsToOperationMap.put(Double.class, doubleOperations);

        return new AddBinaryOperator(DataPrepperExpressionParser.PLUS, operandsToOperationMap);
    }

    @Bean
    public ArithmeticSubtractOperator subtractOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> intOperations =
                Map.of(
                        Integer.class, (lhs, rhs) -> (Integer) lhs - (Integer) rhs,
                        Float.class, (lhs, rhs) -> (Integer) lhs - (Float) rhs,
                        Long.class, (lhs, rhs) -> (Integer) lhs - (Long) rhs,
                        Double.class, (lhs, rhs) -> (Integer) lhs - (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> floatOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Float) lhs - (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Float) lhs - (Float) rhs,
                    Long.class, (lhs, rhs) -> (Float) lhs - (Long) rhs,
                    Double.class, (lhs, rhs) -> (Float) lhs - (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> longOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Long) lhs - (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Long) lhs - (Float)rhs,
                    Long.class, (lhs, rhs) -> (Long) lhs - (Long) rhs,
                    Double.class, (lhs, rhs) -> (Long) lhs - (Double)rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> doubleOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Double) lhs - (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Double) lhs - (Float) rhs,
                    Long.class, (lhs, rhs) -> (Double) lhs - (Long) rhs,
                    Double.class, (lhs, rhs) -> (Double) lhs - (Double) rhs
                );

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);
        operandsToOperationMap.put(Long.class, longOperations);
        operandsToOperationMap.put(Double.class, doubleOperations);

        final Map<Class<? extends Number>, Function<Number, ? extends Number>> strategy = new HashMap<>();
        strategy.put(Integer.class, arg -> -arg.intValue());
        strategy.put(Long.class, arg -> -arg.longValue());
        strategy.put(Float.class, arg -> -arg.floatValue());

        return new ArithmeticSubtractOperator(DataPrepperExpressionParser.SUBTRACT, operandsToOperationMap, strategy);
    }

    @Bean
    public ArithmeticBinaryOperator multiplyOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> intOperations =
                Map.of(
                        Integer.class, (lhs, rhs) -> (Integer) lhs * (Integer) rhs,
                        Float.class, (lhs, rhs) -> (Integer) lhs * (Float) rhs,
                        Long.class, (lhs, rhs) -> (Integer) lhs * (Long) rhs,
                        Double.class, (lhs, rhs) -> (Integer) lhs * (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> floatOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Float) lhs * (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Float) lhs * (Float) rhs,
                    Long.class, (lhs, rhs) -> (Float) lhs * (Long) rhs,
                    Double.class, (lhs, rhs) -> (Float) lhs * (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> longOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Long) lhs * (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Long) lhs * (Float)rhs,
                    Long.class, (lhs, rhs) -> (Long) lhs * (Long) rhs,
                    Double.class, (lhs, rhs) -> (Long) lhs * (Double)rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> doubleOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (Double) lhs * (Integer) rhs,
                    Float.class, (lhs, rhs) -> (Double) lhs * (Float) rhs,
                    Long.class, (lhs, rhs) -> (Double) lhs * (Long) rhs,
                    Double.class, (lhs, rhs) -> (Double) lhs * (Double) rhs
                );

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);
        operandsToOperationMap.put(Long.class, longOperations);
        operandsToOperationMap.put(Double.class, doubleOperations);

        return new ArithmeticBinaryOperator(DataPrepperExpressionParser.MULTIPLY, operandsToOperationMap);
    }

    @Bean
    public ArithmeticBinaryOperator divideOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> intOperations =
                Map.of(
                        Integer.class, (lhs, rhs) -> ((double)(int)lhs) / ((double)(int)rhs),
                        Float.class, (lhs, rhs) -> (float)(int)lhs / (Float) rhs,
                        Long.class, (lhs, rhs) -> (double)(int)lhs / (Long) rhs,
                        Double.class, (lhs, rhs) -> (double)(int)lhs / (Double) rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> floatOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (float)lhs / (float)(int)rhs,
                    Float.class, (lhs, rhs) ->  (float)lhs / (float)rhs,
                    Long.class, (lhs, rhs) -> (float)lhs / (float)(long)rhs,
                    Double.class, (lhs, rhs) -> (double)(float)lhs / (double)rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> longOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (double)(long)lhs / (double)(int)rhs,
                    Float.class, (lhs, rhs) -> (float)(long)lhs / (Float)rhs,
                    Long.class, (lhs, rhs) -> (double)(long)lhs / (double)(long)rhs,
                    Double.class, (lhs, rhs) -> (double)(long)lhs / (double)rhs
                );
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> doubleOperations =
                Map.of(
                    Integer.class, (lhs, rhs) -> (double)lhs / (double)(int)rhs,
                    Float.class, (lhs, rhs) -> (double)lhs / (Float) rhs,
                    Long.class, (lhs, rhs) -> (double)lhs / (double)(long)rhs,
                    Double.class, (lhs, rhs) -> (double)lhs / (Double) rhs
                );

        operandsToOperationMap.put(Integer.class, intOperations);
        operandsToOperationMap.put(Float.class, floatOperations);
        operandsToOperationMap.put(Long.class, longOperations);
        operandsToOperationMap.put(Double.class, doubleOperations);

        return new ArithmeticBinaryOperator(DataPrepperExpressionParser.DIVIDE, operandsToOperationMap);
    }

    @Bean
    public ArithmeticBinaryOperator modOperator() {
        final Map<Class<? extends Number>, Map<Class<? extends Number>, BiFunction<Object, Object, Number>>>
                operandsToOperationMap = new HashMap<>();
        final Map<Class<? extends Number>, BiFunction<Object, Object, Number>> intOperations =
                Map.of(
                        Integer.class, (lhs, rhs) -> ((int)lhs) % ((int)rhs)
                );

        operandsToOperationMap.put(Integer.class, intOperations);

        return new ArithmeticBinaryOperator(DataPrepperExpressionParser.MOD, operandsToOperationMap);
    }
}
