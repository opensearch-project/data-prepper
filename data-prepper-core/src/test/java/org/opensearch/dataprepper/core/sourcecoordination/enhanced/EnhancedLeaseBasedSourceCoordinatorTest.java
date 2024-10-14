/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.sourcecoordination.enhanced;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.parser.model.SourceCoordinationConfig;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;

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
class EnhancedLeaseBasedSourceCoordinatorTest {

    @Mock
    private SourceCoordinationStore sourceCoordinationStore;

    @Mock
    private SourceCoordinationConfig sourceCoordinationConfig;

    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;

    @Mock
    private PluginMetrics pluginMetrics;

    private String sourceIdentifier;

    private EnhancedLeaseBasedSourceCoordinator coordinator;

    private static final String DEFAULT_PARTITION_TYPE = "TEST";


    @BeforeEach
    void setup() {
        sourceIdentifier = UUID.randomUUID().toString();
        lenient().when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(UUID.randomUUID().toString());
        lenient().when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceIdentifier + "|" + DEFAULT_PARTITION_TYPE);
    }

    private EnhancedLeaseBasedSourceCoordinator createObjectUnderTest() {
        EnhancedLeaseBasedSourceCoordinator coordinator = new EnhancedLeaseBasedSourceCoordinator(sourceCoordinationStore, sourceCoordinationConfig, pluginMetrics, sourceIdentifier, sourcePartitionStoreItem -> new TestEnhancedSourcePartition(sourcePartitionStoreItem));
        return coordinator;
    }


    static class TestEnhancedSourcePartition extends EnhancedSourcePartition<String> {

        private final String partitionType;
        private final String partitionKey;

        public TestEnhancedSourcePartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
            setSourcePartitionStoreItem(sourcePartitionStoreItem);

            String[] split = sourcePartitionStoreItem.getSourceIdentifier().split("\\|");
            if ("GLOBAL".equals(split[1])) {
                this.partitionType = null;
            } else {
                this.partitionType = split[1];
            }
            this.partitionKey = sourcePartitionStoreItem.getSourcePartitionKey();
        }

        public TestEnhancedSourcePartition(boolean isGlobal) {
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
        TestEnhancedSourcePartition partition = new TestEnhancedSourcePartition(false);
        coordinator.createPartition(partition);
        verify(sourceCoordinationStore).tryCreatePartitionItem(eq(sourceIdentifier + "|" + DEFAULT_PARTITION_TYPE), anyString(), eq(SourcePartitionStatus.UNASSIGNED), anyLong(), eq(null), eq(false));

        // GlobalState.
        TestEnhancedSourcePartition globalState = new TestEnhancedSourcePartition(true);
        coordinator.createPartition(globalState);
        verify(sourceCoordinationStore).tryCreatePartitionItem(eq(sourceIdentifier + "|GLOBAL"), anyString(), eq(null), anyLong(), eq(null), eq(true));

    }

    @Test
    void test_acquireAvailablePartition_should_run_correctly() {
        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any()))
                .willReturn(Optional.of(sourcePartitionStoreItem))
                .willReturn(Optional.of(sourcePartitionStoreItem))
                .willReturn(Optional.empty());
        coordinator = createObjectUnderTest();

        Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));

        Optional<EnhancedSourcePartition> sourcePartition2 = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition2.isPresent(), equalTo(true));

        Optional<EnhancedSourcePartition> sourcePartition3 = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
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

        Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));
        TestEnhancedSourcePartition partition = (TestEnhancedSourcePartition) sourcePartition.get();
        coordinator.saveProgressStateForPartition(partition, null);

        verify(sourceCoordinationStore).tryAcquireAvailablePartition(anyString(), anyString(), any(Duration.class));
        verify(sourceCoordinationStore).tryUpdateSourcePartitionItem(any(SourcePartitionStoreItem.class));

    }

    @Test
    void test_giveUpPartition() {
        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.of(sourcePartitionStoreItem));

        coordinator = createObjectUnderTest();

        Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));
        TestEnhancedSourcePartition partition = (TestEnhancedSourcePartition) sourcePartition.get();

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

        Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));
        TestEnhancedSourcePartition partition = (TestEnhancedSourcePartition) sourcePartition.get();

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

        Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DEFAULT_PARTITION_TYPE);
        assertThat(sourcePartition.isPresent(), equalTo(true));
        TestEnhancedSourcePartition partition = (TestEnhancedSourcePartition) sourcePartition.get();

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
        Optional<EnhancedSourcePartition> sourcePartition = coordinator.getPartition(partitionKey);
        assertThat(sourcePartition.isPresent(), equalTo(true));
    }
}