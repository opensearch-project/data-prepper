package org.opensearch.dataprepper.plugins.sink.opensearch.dlq;

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

public class FailedDlqDataTest {

    @Nested
    class Getters {

        private String index;
        private String indexId;
        private int status;
        private String message;
        private Object document;

        private FailedDlqData testObject;
        @BeforeEach
        public void setUp() {
            index = UUID.randomUUID().toString();
            indexId = UUID.randomUUID().toString();
            status = new Random().nextInt(500);
            message = UUID.randomUUID().toString();
            document = UUID.randomUUID();

            testObject = FailedDlqData.builder()
                .withIndex(index)
                .withIndexId(indexId)
                .withStatus(status)
                .withMessage(message)
                .withDocument(document)
                .build();
        }

        @Test
        public void testGetDocument() {
            assertThat(testObject.getDocument(), is(equalTo(document)));
        }

        @Test
        public void testGetIndex() {
            assertThat(testObject.getIndex(), is(equalTo(index)));
        }

        @Test
        public void testGetIndexId() {
            assertThat(testObject.getIndexId(), is(equalTo(indexId)));
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

        private String index;
        private String indexId;
        private int status;
        private String message;
        private Object document;

        private FailedDlqData testObject;
        @BeforeEach
        public void setUp() {
            index = UUID.randomUUID().toString();
            indexId = UUID.randomUUID().toString();
            status = new Random().nextInt(500);
            message = UUID.randomUUID().toString();
            document = UUID.randomUUID();

            testObject = FailedDlqData.builder()
                .withIndex(index)
                .withIndexId(indexId)
                .withStatus(status)
                .withMessage(message)
                .withDocument(document)
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

            final FailedDlqData otherTestObject = FailedDlqData.builder()
                .withIndex(index)
                .withIndexId(indexId)
                .withStatus(status)
                .withMessage(message)
                .withDocument(document)
                .build();

            assertThat(testObject.equals(otherTestObject), CoreMatchers.is(CoreMatchers.equalTo(true)));
        }

        @Test
        void test_equals_returns_false_for_two_instances_with_different_values() {

            final FailedDlqData otherTestObject = FailedDlqData.builder()
                .withIndex(index)
                .withIndexId(indexId)
                .withStatus(status)
                .withMessage(message)
                .withDocument(UUID.randomUUID())
                .build();

            assertThat(testObject, CoreMatchers.is(not(CoreMatchers.equalTo(otherTestObject))));
        }

        @Test
        void test_hash_codes_for_two_instances_have_different_values() {

            final FailedDlqData otherTestObject = FailedDlqData.builder()
                .withIndex(UUID.randomUUID().toString())
                .withIndexId(indexId)
                .withStatus(status)
                .withMessage(message)
                .withDocument(document)
                .build();

            assertThat(testObject.hashCode(), CoreMatchers.is(not(CoreMatchers.equalTo(otherTestObject.hashCode()))));
        }

        @Test
        void test_toString_has_all_values() {
            final String string = testObject.toString();

            assertThat(string, notNullValue());
            assertThat(string, allOf(
                containsString("FailedDlqData"),
                containsString(indexId),
                containsString(String.valueOf(status)),
                containsString(message),
                containsString(document.toString())
            ));
        }
    }

}
