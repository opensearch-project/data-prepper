/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class EnhancedSourcePartitionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private String partitionKey;
    private TestPartitionProgressState testPartitionProgressState;
    private TestInstantTypeProgressState testInstantTypeProgressState;

    @BeforeEach
    void setup() {
        partitionKey = UUID.randomUUID().toString();
        testPartitionProgressState = new TestPartitionProgressState(UUID.randomUUID().toString());
        testInstantTypeProgressState = new TestInstantTypeProgressState(Instant.now());

    }

    private EnhancedSourcePartition<TestPartitionProgressState> createObjectUnderTest() {
        return new TestEnhancedSourcePartition(partitionKey, testPartitionProgressState);
    }

    @Test
    void set_SourcePartitionStoreItem_sets_item_correctly() {
        final EnhancedSourcePartition<TestPartitionProgressState> objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getSourcePartitionStoreItem(), equalTo(null));

        final SourcePartitionStoreItem item = mock(SourcePartitionStoreItem.class);
        objectUnderTest.setSourcePartitionStoreItem(item);
        assertThat(objectUnderTest.getSourcePartitionStoreItem(), equalTo(item));
    }

    @Test
    void convertStringToPartitionState_with_null_state_returns_null() {
        final EnhancedSourcePartition<TestPartitionProgressState> objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.convertStringToPartitionProgressState(TestPartitionProgressState.class, null), equalTo(null));
    }

    @Test
    void convertStringToPartitionState_returns_null_when_JsonProcessingException_is_thrown() {
        final EnhancedSourcePartition<TestPartitionProgressState> objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.convertStringToPartitionProgressState(TestPartitionProgressState.class, UUID.randomUUID().toString()), equalTo(null));
    }

    @Test
    void convertStringToPartitionState_is_null_when_ClassCastException_is_thrown_with_null_class() {
        final EnhancedSourcePartition<TestPartitionProgressState> objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.convertStringToPartitionProgressState(null, UUID.randomUUID().toString()), equalTo(null));
    }

    @Test
    void convertFromStringToPartitionState_converts_as_expected() {
        final EnhancedSourcePartition<TestPartitionProgressState> objectUnderTest = createObjectUnderTest();

        final String serializedString = objectUnderTest.convertPartitionProgressStatetoString(Optional.of(testPartitionProgressState));

        final TestPartitionProgressState result = objectUnderTest.convertStringToPartitionProgressState(TestPartitionProgressState.class, serializedString);
        assertThat(result, notNullValue());
        assertThat(result.getTestValue(), equalTo(testPartitionProgressState.getTestValue()));
    }

    @Test
    void convertFromInstantToPartitionState_converts_as_expected() {
        final EnhancedSourcePartition<TestInstantTypeProgressState> objectUnderTest =
                new TestInstantTypeSourcePartition(partitionKey, testInstantTypeProgressState);

        final String serializedString = objectUnderTest.convertPartitionProgressStatetoString(Optional.of(testInstantTypeProgressState));

        final TestInstantTypeProgressState result =
                objectUnderTest.convertStringToPartitionProgressState(TestInstantTypeProgressState.class, serializedString);
        assertThat(result, notNullValue());
        assertThat(result.getTestValue(), equalTo(testInstantTypeProgressState.getTestValue()));
    }

    @Test
    void convertPartitionStateToStringWithEmptyState_returns_null() {
        final String result = createObjectUnderTest().convertPartitionProgressStatetoString(Optional.empty());
        assertThat(result, equalTo(null));
    }

    @Test
    void convertFromPartitionStateToString_converts() {
        final EnhancedSourcePartition<TestPartitionProgressState> objectUnderTest = createObjectUnderTest();

        final String result = objectUnderTest.convertPartitionProgressStatetoString(Optional.of(testPartitionProgressState));

        assertThat(result, notNullValue());
        assertThat(result, equalTo("{\"testValue\":\"" + testPartitionProgressState.getTestValue() + "\"}"));
    }

    @Test
    void convertFromPartitionStateToStringReturns_null_when_JsonProcessingException_is_thrown() {

        final TestInvalidPartitionProgressState invalidPartitionProgressState = new TestInvalidPartitionProgressState();
        final EnhancedSourcePartition<TestInvalidPartitionProgressState> objectUnderTest = new TestInvalidEnhancedSourcePartition(UUID.randomUUID().toString(),
                invalidPartitionProgressState);

        assertThat(objectUnderTest.convertPartitionProgressStatetoString(Optional.of(invalidPartitionProgressState)), equalTo(null));
    }

    @Test
    void convertFromStringToPartitionStateWithPrimitiveType_returns_expected_result() throws JsonProcessingException {
        final Map<String, Object> stateMap = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final EnhancedSourcePartition<Map<String, Object>> objectUnderTest =
                new TestMapStringObjectEnhancedSourcePartition(UUID.randomUUID().toString(), stateMap);

        final Map<String, Object> resultWithNullClass = objectUnderTest.convertStringToPartitionProgressState(null, objectMapper.writeValueAsString(stateMap));
        assertThat(resultWithNullClass, notNullValue());
        assertThat(resultWithNullClass, equalTo(stateMap));
    }

    public static class TestInstantTypeSourcePartition extends EnhancedSourcePartition<TestInstantTypeProgressState> {

        private final String partitionKey;
        private final TestInstantTypeProgressState testPartitionProgressState;

        public TestInstantTypeSourcePartition(final String partitionKey, final TestInstantTypeProgressState partitionProgressState) {
            this.partitionKey = partitionKey;
            this.testPartitionProgressState = partitionProgressState;
        }

        @Override
        public String getPartitionType() {
            return "TEST";
        }

        @Override
        public String getPartitionKey() {
            return partitionKey;
        }

        @Override
        public Optional<TestInstantTypeProgressState> getProgressState() {
            return Optional.of(testPartitionProgressState);
        }
    }

    public class TestEnhancedSourcePartition extends EnhancedSourcePartition<TestPartitionProgressState> {

        private final String partitionKey;
        private final TestPartitionProgressState testPartitionProgressState;

        public TestEnhancedSourcePartition(final String partitionKey, final TestPartitionProgressState partitionProgressState) {
            this.partitionKey = partitionKey;
            this.testPartitionProgressState = partitionProgressState;
        }

        @Override
        public String getPartitionType() {
            return "TEST";
        }

        @Override
        public String getPartitionKey() {
            return partitionKey;
        }

        @Override
        public Optional<TestPartitionProgressState> getProgressState() {
            return Optional.of(testPartitionProgressState);
        }
    }

    public class TestInvalidEnhancedSourcePartition extends EnhancedSourcePartition<TestInvalidPartitionProgressState> {

        private final String partitionKey;
        private final TestInvalidPartitionProgressState testPartitionProgressState;

        public TestInvalidEnhancedSourcePartition(final String partitionKey, final TestInvalidPartitionProgressState partitionProgressState) {
            this.partitionKey = partitionKey;
            this.testPartitionProgressState = partitionProgressState;
        }

        @Override
        public String getPartitionType() {
            return "TEST";
        }

        @Override
        public String getPartitionKey() {
            return partitionKey;
        }

        @Override
        public Optional<TestInvalidPartitionProgressState> getProgressState() {
            return Optional.of(testPartitionProgressState);
        }
    }

    public class TestMapStringObjectEnhancedSourcePartition extends EnhancedSourcePartition<Map<String, Object>> {

        private final String partitionKey;
        private final Map<String, Object> testPartitionProgressState;

        public TestMapStringObjectEnhancedSourcePartition(final String partitionKey, final Map<String, Object> partitionProgressState) {
            this.partitionKey = partitionKey;
            this.testPartitionProgressState = partitionProgressState;
        }

        @Override
        public String getPartitionType() {
            return "TEST";
        }

        @Override
        public String getPartitionKey() {
            return partitionKey;
        }

        @Override
        public Optional<Map<String, Object>> getProgressState() {
            return Optional.of(testPartitionProgressState);
        }
    }
}
