/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.sink.dlq;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import java.util.Random;
import java.util.UUID;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LambdaSinkFailedDlqDataTest {

    @Nested
    class Getters {

        private String functionName;
        private int status;
        private String message;
        private Object data;

        private LambdaSinkFailedDlqData testObject;
        @BeforeEach
        public void setUp() {
            functionName = UUID.randomUUID().toString();
            status = new Random().nextInt(500);
            message = UUID.randomUUID().toString();
            data = UUID.randomUUID();

            testObject = LambdaSinkFailedDlqData.builder()
                .withFunctionName(functionName)
                .withStatus(status)
                .withMessage(message)
                .withData(data)
                .build();
        }

        @Test
        public void testGetDocument() {
            assertThat(testObject.getData(), is(equalTo(data)));
        }

        @Test
        public void testGetIndex() {
            assertThat(testObject.getFunctionName(), is(equalTo(functionName)));
        }

        @Test
        public void testGetStatus() {
            assertThat(testObject.getStatus(), is(equalTo(status)));
        }

        @Test
        public void testGetMessage() {
            assertThat(testObject.getMessage(), is(equalTo(message)));
        }
    }

    @Nested
    class EqualsAndHashCodeAndToString {

        private String functionName;
        private int status;
        private String message;
        private Object data;

        private LambdaSinkFailedDlqData testObject;
        @BeforeEach
        public void setUp() {
            functionName = UUID.randomUUID().toString();
            status = new Random().nextInt(500);
            message = UUID.randomUUID().toString();
            data = UUID.randomUUID();

            testObject = LambdaSinkFailedDlqData.builder()
                .withFunctionName(functionName)
                .withStatus(status)
                .withMessage(message)
                .withData(data)
                .build();
        }

        @Test
        void test_equals_returns_false_for_null() {
            assertThat(testObject.equals(null), CoreMatchers.is(CoreMatchers.equalTo(false)));
        }

        @Test
        void test_equals_returns_false_for_other_class() {
            assertThat(testObject.equals(randomUUID()), CoreMatchers.is(CoreMatchers.equalTo(false)));
        }

        @Test
        void test_equals_on_same_instance_returns_true() {
            assertThat(testObject.equals(testObject), CoreMatchers.is(CoreMatchers.equalTo(true)));
        }

        @Test
        void test_equals_a_clone_of_the_same_instance_returns_true() {

            final LambdaSinkFailedDlqData otherTestObject = LambdaSinkFailedDlqData.builder()
                .withFunctionName(functionName)
                .withStatus(status)
                .withMessage(message)
                .withData(data)
                .build();

            assertThat(testObject.equals(otherTestObject), CoreMatchers.is(CoreMatchers.equalTo(true)));
        }

        @Test
        void test_equals_returns_false_for_two_instances_with_different_values() {

            final LambdaSinkFailedDlqData otherTestObject = LambdaSinkFailedDlqData.builder()
                .withFunctionName(functionName)
                .withStatus(status)
                .withMessage(message)
                .withData(UUID.randomUUID())
                .build();

            assertThat(testObject, CoreMatchers.is(not(CoreMatchers.equalTo(otherTestObject))));
        }

        @Test
        void test_hash_codes_for_two_instances_have_different_values() {

            final LambdaSinkFailedDlqData otherTestObject = LambdaSinkFailedDlqData.builder()
                .withFunctionName(UUID.randomUUID().toString())
                .withStatus(status)
                .withMessage(message)
                .withData(data)
                .build();

            assertThat(testObject.hashCode(), CoreMatchers.is(not(CoreMatchers.equalTo(otherTestObject.hashCode()))));
        }

        @Test
        void test_toString_has_all_values() {
            final String string = testObject.toString();

            assertThat(string, notNullValue());
            assertThat(string, allOf(
                containsString("LambdaSinkFailedDlqData"),
                containsString(functionName),
                containsString(String.valueOf(status)),
                containsString(message),
                containsString(data.toString())
            ));
        }
    }

}

