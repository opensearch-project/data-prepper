/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DefaultCoordinatorTest {

    @Mock
    private SourceCoordinationStore sourceCoordinationStore;

    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;

    private String sourceIdentifier;

    private DefaultEnhancedSourceCoordinator coordinator;

    private final String DEFAULT_PARTITION_TYPE = "TEST";


    @BeforeEach
    void setup() {
        sourceIdentifier = UUID.randomUUID().toString();
        lenient().when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(UUID.randomUUID().toString());
        lenient().when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceIdentifier + "|" + DEFAULT_PARTITION_TYPE);
    }

    private DefaultEnhancedSourceCoordinator createObjectUnderTest() {
        DefaultEnhancedSourceCoordinator coordinator = new DefaultEnhancedSourceCoordinator(sourceCoordinationStore, sourceIdentifier, sourcePartitionStoreItem -> new TestPartition(sourcePartitionStoreItem));
        return coordinator;
    }


    class TestPartition extends SourcePartition<String> {

        private final String partitionType;
        private final String partitionKey;

        public TestPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
            setSourcePartitionStoreItem(sourcePartitionStoreItem);

            String[] split = sourcePartitionStoreItem.getSourceIdentifier().split("\\|");
            if ("GLOBAL".equals(split[1])) {
                this.partitionType = null;
            } else {
                this.partitionType = split[1];
            }
            this.partitionKey = sourcePartitionStoreItem.getSourcePartitionKey();
        }

        public TestPartition(boolean isGlobal) {
            this.partitionType = isGlobal ? null : DEFAULT_PARTITION_TYPE;
            partitionKey = UUID.randomUUID().toString();
        }


        @Override
        public String getPartitionType() {
            return partitionType;
        }

        @Override
        public String getPartitionKey() {
            return partitionKey;
        }

        @Override
        public Optional<String> getProgressState() {
            return Optional.empty();
        }
    }

    @Test
    void test_initialize_should_run_correctly() {
        coordinator = createObjectUnderTest();
        coordinator.initialize();
        // Should call initializeStore
        verify(sourceCoordinationStore).initializeStore();

    }

    @Test
    void test_createPartition() {
        coordinator = createObjectUnderTest();
        // A normal type.
        TestPartition partition = new TestPartition(false);
        coordinator.createPartition(partition);
        verify(sourceCoordinationStore).tryCreatePartitionItem(eq(sourceIdentifier + "|" + DEFAULT_PARTITION_TYPE), anyString(), eq(SourcePartitionStatus.UNASSIGNED), anyLong(), eq(null));

        // GlobalState.
        TestPartition globalState = new TestPartition(true);
        coordinator.createPartition(globalState);
        verify(sourceCoordinationStore).tryCreatePartitionItem(eq(sourceIdentifier + "|GLOBAL"), anyString(), eq(null), anyLong(), eq(null));

    }

    @Test
    void test_acquireAvailablePartition_should_run_correctly() {
        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any()))
                .willReturn(Optional.of(sourcePartitionStoreItem))
                .willReturn(Optional.of(sourcePartitionStoreItem))
                .willReturn(Optional.empty());
        coordinator = createObjectUnderTest();

        Optional<SourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));

        Optional<SourcePartition> sourcePartition2 = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition2.isPresent(), equalTo(true));

        Optional<SourcePartition> sourcePartition3 = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition3.isPresent(), equalTo(false));

        verify(sourceCoordinationStore, times(3)).tryAcquireAvailablePartition(anyString(), anyString(), any(Duration.class));
    }


    @Test
    void test_saveProgressStateForPartition() {

        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any()))
                .willReturn(Optional.of(sourcePartitionStoreItem))
                .willReturn(Optional.of(sourcePartitionStoreItem))
                .willReturn(Optional.empty());
        coordinator = createObjectUnderTest();

        Optional<SourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));
        TestPartition partition = (TestPartition) sourcePartition.get();
        coordinator.saveProgressStateForPartition(partition);

        verify(sourceCoordinationStore).tryAcquireAvailablePartition(anyString(), anyString(), any(Duration.class));
        verify(sourceCoordinationStore).tryUpdateSourcePartitionItem(any(SourcePartitionStoreItem.class));

    }

    @Test
    void test_giveUpPartition() {
        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.of(sourcePartitionStoreItem));

        coordinator = createObjectUnderTest();

        Optional<SourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));
        TestPartition partition = (TestPartition) sourcePartition.get();

        coordinator.giveUpPartition(partition);

        verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
        verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);
        verify(sourcePartitionStoreItem).setPartitionOwner(null);

        verify(sourceCoordinationStore).tryAcquireAvailablePartition(anyString(), anyString(), any(Duration.class));
        verify(sourceCoordinationStore).tryUpdateSourcePartitionItem(any(SourcePartitionStoreItem.class));
    }

    @Test
    void test_completePartition() {
        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.of(sourcePartitionStoreItem));
        coordinator = createObjectUnderTest();

        Optional<SourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));
        TestPartition partition = (TestPartition) sourcePartition.get();

        coordinator.completePartition(partition);

        verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
        verify(sourcePartitionStoreItem).setReOpenAt(null);
        verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);
        verify(sourcePartitionStoreItem).setPartitionOwner(null);

        verify(sourceCoordinationStore).tryAcquireAvailablePartition(anyString(), anyString(), any(Duration.class));
        verify(sourceCoordinationStore).tryUpdateSourcePartitionItem(any(SourcePartitionStoreItem.class));
    }

    @Test
    void test_closePartition() {
        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.of(sourcePartitionStoreItem));
        coordinator = createObjectUnderTest();

        Optional<SourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));
        TestPartition partition = (TestPartition) sourcePartition.get();

        coordinator.closePartition(partition, Duration.ofMinutes(10), 1);
        verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.CLOSED);
        verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);
        verify(sourcePartitionStoreItem).setPartitionOwner(null);

        verify(sourceCoordinationStore).tryAcquireAvailablePartition(anyString(), anyString(), any(Duration.class));
        verify(sourceCoordinationStore).tryUpdateSourcePartitionItem(any(SourcePartitionStoreItem.class));

    }

    @Test
    void getPartition() {
        String partitionKey = UUID.randomUUID().toString();
        given(sourceCoordinationStore.getSourcePartitionItem(eq(sourceIdentifier + "|GLOBAL"), eq(partitionKey))).willReturn(Optional.of(sourcePartitionStoreItem));
        coordinator = createObjectUnderTest();
        Optional<SourcePartition> sourcePartition = coordinator.getPartition(partitionKey);
        assertThat(sourcePartition.isPresent(), equalTo(true));
    }
}