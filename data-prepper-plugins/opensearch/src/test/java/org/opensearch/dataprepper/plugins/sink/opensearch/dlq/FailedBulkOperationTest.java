/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.dlq;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.dataprepper.model.event.EventHandle;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

public class FailedBulkOperationTest {

    @Nested
    class Getters {

        private BulkOperationWrapper bulkOperation;
        private BulkResponseItem bulkResponseItem;
        private EventHandle eventHandle;
        private Throwable failure;

        private FailedBulkOperation testObject;
        @BeforeEach
        public void setUp() {
            bulkOperation = mock(BulkOperationWrapper.class);
            bulkResponseItem = mock(BulkResponseItem.class);
            eventHandle = mock(EventHandle.class);
            failure = new Exception();
            lenient().when(bulkOperation.getEventHandle()).thenReturn(eventHandle);

            testObject = FailedBulkOperation.builder()
                .withBulkOperation(bulkOperation)
                .withBulkResponseItem(bulkResponseItem)
                .withFailure(failure)
                .build();
        }

        @Test
        public void testGetBulkOperation() {
            assertThat(testObject.getBulkOperation(), is(equalTo(bulkOperation)));
        }

        @Test
        public void testGetEventHandle() {
            assertThat(testObject.getBulkOperation().getEventHandle(), is(equalTo(eventHandle)));
        }

        @Test
        public void testGetBulkResponseItem() {
            assertThat(testObject.getBulkResponseItem(), is(equalTo(bulkResponseItem)));
        }

        @Test
        public void testGetFailure() {
            assertThat(testObject.getFailure(), is(equalTo(failure)));
        }

    }

    @Nested
    class Builder {
        private BulkOperationWrapper bulkOperation;
        private BulkResponseItem bulkResponseItem;
        private Throwable failure;

        @BeforeEach
        public void setUp() {
            bulkOperation = mock(BulkOperationWrapper.class);
            bulkResponseItem = mock(BulkResponseItem.class);
            failure = new Exception();
        }

        @Test
        public void testWithMissingFailure() {
            final FailedBulkOperation testObject = FailedBulkOperation.builder()
                .withBulkOperation(bulkOperation)
                .withBulkResponseItem(bulkResponseItem)
                .build();

            assertThat(testObject, is(notNullValue()));
        }

        @Test
        public void testWithMissingBulkResponseItem() {
            final FailedBulkOperation testObject = FailedBulkOperation.builder()
                .withBulkOperation(bulkOperation)
                .withFailure(failure)
                .build();

            assertThat(testObject, is(notNullValue()));
        }

        @Test
        public void testWithMissingBulkResponseItemAndFailureThrowsException() {
            assertThrows(IllegalArgumentException.class, () -> FailedBulkOperation.builder()
                .withBulkOperation(bulkOperation)
                .build());
        }

        @Test
        public void testBuilderWithMissingBulkOperationThrowsException() {
            assertThrows(NullPointerException.class, () -> FailedBulkOperation.builder()
                .withBulkResponseItem(bulkResponseItem)
                .withFailure(failure)
                .build());
        }
    }

    @Nested
    class EqualsAndHashCodeAndToString {

        private BulkOperationWrapper bulkOperation;
        private BulkResponseItem bulkResponseItem;
        private Throwable failure;

        private FailedBulkOperation testObject;
        @BeforeEach
        public void setUp() {
            bulkOperation = mock(BulkOperationWrapper.class);
            bulkResponseItem = mock(BulkResponseItem.class);
            failure = new Exception();

            testObject = FailedBulkOperation.builder()
                .withBulkOperation(bulkOperation)
                .withBulkResponseItem(bulkResponseItem)
                .withFailure(failure)
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

            final FailedBulkOperation otherTestObject = FailedBulkOperation.builder()
                .withBulkOperation(bulkOperation)
                .withBulkResponseItem(bulkResponseItem)
                .withFailure(failure)
                .build();

            assertThat(testObject.equals(otherTestObject), CoreMatchers.is(CoreMatchers.equalTo(true)));
        }

        @Test
        void test_equals_returns_false_for_two_instances_with_different_values() {

            final FailedBulkOperation otherTestObject = FailedBulkOperation.builder()
                .withBulkOperation(bulkOperation)
                .withBulkResponseItem(bulkResponseItem)
                .withFailure(new RuntimeException())
                .build();

            assertThat(testObject, CoreMatchers.is(not(CoreMatchers.equalTo(otherTestObject))));
        }

        @Test
        void test_hash_codes_for_two_instances_have_different_values() {

            final FailedBulkOperation otherTestObject = FailedBulkOperation.builder()
                .withBulkOperation(bulkOperation)
                .withBulkResponseItem(bulkResponseItem)
                .withFailure(new RuntimeException())
                .build();

            assertThat(testObject.hashCode(), CoreMatchers.is(not(CoreMatchers.equalTo(otherTestObject.hashCode()))));
        }

        @Test
        void test_toString_has_all_values() {
            final String string = testObject.toString();

            assertThat(string, notNullValue());
            assertThat(string, allOf(
                containsString("FailedBulkOperation"),
                containsString(bulkOperation.toString()),
                containsString(bulkResponseItem.toString()),
                containsString(failure.toString())
            ));
        }
    }
}
