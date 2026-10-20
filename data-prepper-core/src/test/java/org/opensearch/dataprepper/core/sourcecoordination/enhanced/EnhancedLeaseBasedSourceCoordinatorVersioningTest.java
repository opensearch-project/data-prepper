/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.sourcecoordination.enhanced;

import io.micrometer.core.instrument.Counter;
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
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EnhancedLeaseBasedSourceCoordinatorVersioningTest {

    private static final String PARTITION_TYPE = "TEST";

    @Mock
    private SourceCoordinationConfig sourceCoordinationConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    private String sourceIdentifier;
    private VersionedInMemoryStore store;

    @BeforeEach
    void setup() {
        sourceIdentifier = UUID.randomUUID().toString();
        when(pluginMetrics.counter(anyString())).thenReturn(mock(Counter.class));
        store = new VersionedInMemoryStore();
    }

    private EnhancedLeaseBasedSourceCoordinator createObjectUnderTest() {
        return new EnhancedLeaseBasedSourceCoordinator(store, sourceCoordinationConfig, pluginMetrics, sourceIdentifier,
                TestPartition::new);
    }

    @Test
    void renewing_a_lease_does_not_stop_the_partition_from_being_completed() {
        final EnhancedLeaseBasedSourceCoordinator coordinator = createObjectUnderTest();
        final String partitionKey = UUID.randomUUID().toString();
        store.seedUnassigned(sourceIdentifier + "|" + PARTITION_TYPE, partitionKey);

        final EnhancedSourcePartition partition = coordinator.acquireAvailablePartition(PARTITION_TYPE).orElseThrow();

        coordinator.renewPartitionOwnership(partition);
        coordinator.renewPartitionOwnership(partition);
        coordinator.renewPartitionOwnership(partition);

        assertDoesNotThrow(() -> coordinator.completePartition(partition),
                "completePartition must still succeed after the lease has been renewed");
    }

    @Test
    void renewing_a_lease_does_not_stop_the_partition_from_being_given_up() {
        final EnhancedLeaseBasedSourceCoordinator coordinator = createObjectUnderTest();
        final String partitionKey = UUID.randomUUID().toString();
        store.seedUnassigned(sourceIdentifier + "|" + PARTITION_TYPE, partitionKey);

        final EnhancedSourcePartition partition = coordinator.acquireAvailablePartition(PARTITION_TYPE).orElseThrow();
        coordinator.renewPartitionOwnership(partition);
        coordinator.renewPartitionOwnership(partition);

        assertDoesNotThrow(() -> coordinator.giveUpPartition(partition),
                "giveUpPartition must still succeed after the lease has been renewed");
    }

    @Test
    void renewing_a_lease_does_not_stop_progress_state_from_being_saved() {
        final EnhancedLeaseBasedSourceCoordinator coordinator = createObjectUnderTest();
        final String partitionKey = UUID.randomUUID().toString();
        store.seedUnassigned(sourceIdentifier + "|" + PARTITION_TYPE, partitionKey);

        final EnhancedSourcePartition partition = coordinator.acquireAvailablePartition(PARTITION_TYPE).orElseThrow();
        coordinator.renewPartitionOwnership(partition);

        assertDoesNotThrow(() -> coordinator.saveProgressStateForPartition(partition, Duration.ofMinutes(1)),
                "saveProgressStateForPartition must still succeed after the lease has been renewed");
    }

    @Test
    void renewing_after_the_partition_is_completed_is_a_no_op() {
        final EnhancedLeaseBasedSourceCoordinator coordinator = createObjectUnderTest();
        final String partitionKey = UUID.randomUUID().toString();
        store.seedUnassigned(sourceIdentifier + "|" + PARTITION_TYPE, partitionKey);

        final EnhancedSourcePartition partition = coordinator.acquireAvailablePartition(PARTITION_TYPE).orElseThrow();
        coordinator.completePartition(partition);
        final long versionAfterComplete = store.versionOf(partitionKey);

        assertDoesNotThrow(() -> coordinator.renewPartitionOwnership(partition));

        assertThat(store.versionOf(partitionKey), equalTo(versionAfterComplete));
        assertThat(store.statusOf(partitionKey), equalTo(SourcePartitionStatus.COMPLETED));
    }

    private static class VersionedInMemoryStore implements SourceCoordinationStore {
        private final Map<String, TestItem> stored = new HashMap<>();

        long versionOf(final String partitionKey) {
            return stored.get(partitionKey).version;
        }

        SourcePartitionStatus statusOf(final String partitionKey) {
            return stored.get(partitionKey).status;
        }

        void seedUnassigned(final String sourceIdentifier, final String partitionKey) {
            stored.put(partitionKey, new TestItem(sourceIdentifier, partitionKey, SourcePartitionStatus.UNASSIGNED, null, 0L));
        }

        @Override
        public Optional<SourcePartitionStoreItem> tryAcquireAvailablePartition(final String sourceIdentifier, final String ownerId,
                                                                               final Duration ownershipTimeout) {
            for (final TestItem candidate : stored.values()) {
                if (SourcePartitionStatus.UNASSIGNED.equals(candidate.status)) {
                    final TestItem acquired = candidate.copy();
                    acquired.setPartitionOwnershipTimeout(Instant.now().plus(ownershipTimeout));
                    acquired.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
                    tryUpdateSourcePartitionItem(acquired);
                    return Optional.of(acquired);
                }
            }
            return Optional.empty();
        }

        @Override
        public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem) {
            final TestItem item = (TestItem) updateItem;
            final long expectedVersion = item.version;
            item.version = expectedVersion + 1L;
            final TestItem current = stored.get(item.partitionKey);
            if (current == null || current.version != expectedVersion) {
                throw new PartitionUpdateException(String.format("Version conflict for partition %s, expected version %s",
                        item.partitionKey, expectedVersion), null);
            }
            stored.put(item.partitionKey, item.copy());
        }

        @Override
        public void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem, final Instant priorityForUnassignedPartitions) {
            tryUpdateSourcePartitionItem(updateItem);
        }

        @Override
        public void initializeStore() {
        }

        @Override
        public Optional<SourcePartitionStoreItem> getSourcePartitionItem(final String sourceIdentifier, final String sourcePartitionKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<SourcePartitionStoreItem> querySourcePartitionItemsByStatus(final String sourceIdentifier,
                                                                                final SourcePartitionStatus status,
                                                                                final String startPartitionPriority) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<SourcePartitionStoreItem> queryAllSourcePartitionItems(final String sourceIdentifier) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryCreatePartitionItem(final String sourceIdentifier, final String partitionKey, final SourcePartitionStatus status,
                                              final Long closedCount, final String partitionProgressState, final boolean isGlobal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void tryDeletePartitionItem(final SourcePartitionStoreItem deleteItem) {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestItem implements SourcePartitionStoreItem {
        private final String sourceIdentifier;
        private final String partitionKey;
        private SourcePartitionStatus status;
        private Instant ownershipTimeout;
        private long version;

        TestItem(final String sourceIdentifier, final String partitionKey, final SourcePartitionStatus status,
                 final Instant ownershipTimeout, final long version) {
            this.sourceIdentifier = sourceIdentifier;
            this.partitionKey = partitionKey;
            this.status = status;
            this.ownershipTimeout = ownershipTimeout;
            this.version = version;
        }

        TestItem copy() {
            return new TestItem(sourceIdentifier, partitionKey, status, ownershipTimeout, version);
        }

        @Override
        public String getSourceIdentifier() {
            return sourceIdentifier;
        }

        @Override
        public String getSourcePartitionKey() {
            return partitionKey;
        }

        @Override
        public SourcePartitionStatus getSourcePartitionStatus() {
            return status;
        }

        @Override
        public Instant getPartitionOwnershipTimeout() {
            return ownershipTimeout;
        }

        @Override
        public void setSourcePartitionStatus(final SourcePartitionStatus status) {
            this.status = status;
        }

        @Override
        public void setPartitionOwnershipTimeout(final Instant ownershipTimeout) {
            this.ownershipTimeout = ownershipTimeout;
        }

        @Override
        public String getPartitionOwner() {
            return null;
        }

        @Override
        public String getPartitionProgressState() {
            return null;
        }

        @Override
        public Instant getReOpenAt() {
            return null;
        }

        @Override
        public Long getClosedCount() {
            return 0L;
        }

        @Override
        public void setSourcePartitionKey(final String sourcePartitionKey) {
        }

        @Override
        public void setPartitionOwner(final String partitionOwner) {
        }

        @Override
        public void setPartitionProgressState(final String partitionProgressState) {
        }

        @Override
        public void setReOpenAt(final Instant reOpenAt) {
        }

        @Override
        public void setClosedCount(final Long closedCount) {
        }
    }

    private static class TestPartition extends EnhancedSourcePartition<String> {
        TestPartition(final SourcePartitionStoreItem item) {
            setSourcePartitionStoreItem(item);
        }

        @Override
        public String getPartitionKey() {
            return getSourcePartitionStoreItem().getSourcePartitionKey();
        }

        @Override
        public String getPartitionType() {
            return PARTITION_TYPE;
        }

        @Override
        public Optional<String> getProgressState() {
            return Optional.empty();
        }
    }
}
