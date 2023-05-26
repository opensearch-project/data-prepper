package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.util.TestObject;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class OperatorConfigurationTest {
    @InjectMocks
    private OperatorConfiguration configuration;

    @ParameterizedTest
    @MethodSource("validTestArgProvider")
    void equalOperator(final Object lhs, final Object rhs, final boolean isArgsEqual) {
        final GenericEqualOperator genericEqualOperator = configuration.equalOperator();
        assertThat(genericEqualOperator, isA(Operator.class));
        assertThat(
                lhs + " == " + rhs + " should equal is " + isArgsEqual,
                genericEqualOperator.evaluate(lhs, rhs),
                is(isArgsEqual)
        );
    }

    @ParameterizedTest
    @MethodSource("validSubtractTestArgProvider")
    void unarySubtractOperator(final Number operand, final Number expected) {
        final ArithmeticSubtractOperator arithmeticSubtractOperator = configuration.subtractOperator();
        assertThat(arithmeticSubtractOperator, isA(Operator.class));
        assertThat(
                operand + " should equal to " + expected,
                arithmeticSubtractOperator.evaluate(operand),
                is(expected)
        );
    }

    private static Stream<Arguments> validTestArgProvider() {
        return Stream.of(
                Arguments.of(10, 10, true),
                Arguments.of(10, 10L, true),
                Arguments.of(10, 10.0f, true),
                Arguments.of(10, 10.0, true),
                Arguments.of(10, 150, false),
                Arguments.of(10, 150L, false),
                Arguments.of(10, 10.05f, false),
                Arguments.of(10, 10.05, false),

                Arguments.of(-10.0f, -10, true),
                Arguments.of(-10.0f, -10L, true),
                Arguments.of(3.14f, 3.14f, true),
                Arguments.of(3.14f, 3.14, true),
                Arguments.of(-10.0f, -150, false),
                Arguments.of(-10.0f, -150L, false),
                Arguments.of(3.14f, 3.154f, false),
                Arguments.of(3.14f, 3.154, false),

                Arguments.of(10L, 10, true),
                Arguments.of(10L, 10L, true),
                Arguments.of(10L, 10.0f, true),
                Arguments.of(10L, 10.0, true),
                Arguments.of(10L, 150, false),
                Arguments.of(10L, 150L, false),
                Arguments.of(10L, 10.05f, false),
                Arguments.of(10L, 10.05, false),

                Arguments.of(-10.0, -10, true),
                Arguments.of(-10.0, -10L, true),
                Arguments.of(3.14, 3.14f, true),
                Arguments.of(3.14, 3.14, true),
                Arguments.of(-10.0, -150, false),
                Arguments.of(3.14, 3.154f, false),
                Arguments.of(-10.0, -150L, false),
                Arguments.of(3.14, 3.154, false),

                Arguments.of(2000000000, 2000000000, true),
                Arguments.of(2000000000, 2000000000.1f, true),
                Arguments.of(new TestObject("1"), new TestObject("1"), true),
                Arguments.of(new TestObject("1"), new TestObject("2"), false),
                Arguments.of(new TestObject("1"), new TestObject("3"), false),
                Arguments.of(null, new TestObject("1"), false),
                Arguments.of(new TestObject("1"), null, false),
                Arguments.of(10, 'a', false),
                Arguments.of(null, null, true)
        );
    }

    private static Stream<Arguments> validSubtractTestArgProvider() {
        return Stream.of(
                Arguments.of(10, -10),
                Arguments.of(10f, -10f),
                Arguments.of(10L, -10L),
                Arguments.of(Integer.MAX_VALUE, -Integer.MAX_VALUE),
                Arguments.of(Float.MIN_NORMAL, -Float.MIN_NORMAL),
                Arguments.of(-42.95f, 42.95f)
            );
    }

    void notEqualOperator() {
        final Operator<Boolean> notEqualOperator = configuration.notEqualOperator(null);

        assertThat(notEqualOperator, is(notNullValue()));
        assertThat(notEqualOperator instanceof GenericNotOperator, is(true));
    }
}
