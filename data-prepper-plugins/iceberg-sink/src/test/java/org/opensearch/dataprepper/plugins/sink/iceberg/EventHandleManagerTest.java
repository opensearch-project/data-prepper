/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition.WriteResultPartition;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventHandleManagerTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;

    private ConcurrentHashMap<Long, List<EventHandle>> currentEventHandles;
    private ConcurrentHashMap<String, List<EventHandle>> pendingEventHandles;

    @BeforeEach
    void setUp() {
        currentEventHandles = new ConcurrentHashMap<>();
        pendingEventHandles = new ConcurrentHashMap<>();
    }

    @Test
    void eventHandles_collectedDuringOutput_movedToPendingOnFlush() {
        final long threadId = 1L;
        final EventHandle handle1 = mock(EventHandle.class);
        final EventHandle handle2 = mock(EventHandle.class);

        // Simulate output(): collect EventHandles
        currentEventHandles.computeIfAbsent(threadId, id -> new ArrayList<>()).add(handle1);
        currentEventHandles.computeIfAbsent(threadId, id -> new ArrayList<>()).add(handle2);

        assertEquals(2, currentEventHandles.get(threadId).size());

        // Simulate registerWriteResult(): move to pending
        final String partitionId = "partition-1";
        final List<EventHandle> handles = currentEventHandles.remove(threadId);
        pendingEventHandles.put(partitionId, handles);

        assertTrue(currentEventHandles.isEmpty());
        assertEquals(2, pendingEventHandles.get(partitionId).size());
    }

    @Test
    void releaseCommittedEventHandles_releasesHandlesForCompletedPartitions() {
        final EventHandle handle1 = mock(EventHandle.class);
        final EventHandle handle2 = mock(EventHandle.class);
        final EventHandle handle3 = mock(EventHandle.class);

        pendingEventHandles.put("partition-1", List.of(handle1, handle2));
        pendingEventHandles.put("partition-2", List.of(handle3));

        // Simulate: only partition-1 is completed
        final WriteResultPartition completedPartition = mock(WriteResultPartition.class);
        when(completedPartition.getPartitionKey()).thenReturn("partition-1");
        when(coordinator.queryCompletedPartitions(eq(WriteResultPartition.PARTITION_TYPE), any(Instant.class)))
                .thenReturn(List.of(completedPartition));

        // Execute polling logic
        final List<EnhancedSourcePartition> completed =
                coordinator.queryCompletedPartitions(WriteResultPartition.PARTITION_TYPE, Instant.now().minusSeconds(10));
        for (final EnhancedSourcePartition partition : completed) {
            final List<EventHandle> handles = pendingEventHandles.remove(partition.getPartitionKey());
            if (handles != null) {
                handles.forEach(h -> h.release(true));
            }
        }

        verify(handle1).release(true);
        verify(handle2).release(true);
        verify(handle3, never()).release(any(Boolean.class));
        assertTrue(pendingEventHandles.containsKey("partition-2"));
        assertEquals(1, pendingEventHandles.size());
    }

    @Test
    void releaseCommittedEventHandles_noOpWhenPendingEmpty() {
        when(coordinator.queryCompletedPartitions(any(), any()))
                .thenReturn(Collections.emptyList());

        // Should not throw
        final List<EnhancedSourcePartition> completed =
                coordinator.queryCompletedPartitions(WriteResultPartition.PARTITION_TYPE, Instant.now());
        assertTrue(completed.isEmpty());
    }

    @Test
    void shutdown_releasesAllPendingHandlesAsFailed() {
        final EventHandle pendingHandle = mock(EventHandle.class);
        final EventHandle currentHandle = mock(EventHandle.class);

        pendingEventHandles.put("partition-1", List.of(pendingHandle));
        currentEventHandles.put(1L, List.of(currentHandle));

        // Simulate shutdown
        pendingEventHandles.values().forEach(handles ->
                handles.forEach(h -> h.release(false)));
        pendingEventHandles.clear();
        currentEventHandles.values().forEach(handles ->
                handles.forEach(h -> h.release(false)));
        currentEventHandles.clear();

        verify(pendingHandle).release(false);
        verify(currentHandle).release(false);
        assertTrue(pendingEventHandles.isEmpty());
        assertTrue(currentEventHandles.isEmpty());
    }

    @Test
    void multipleFlushes_createSeparatePendingEntries() {
        final long threadId = 1L;
        final EventHandle handle1 = mock(EventHandle.class);
        final EventHandle handle2 = mock(EventHandle.class);

        // First output + flush
        currentEventHandles.computeIfAbsent(threadId, id -> new ArrayList<>()).add(handle1);
        pendingEventHandles.put("partition-1", currentEventHandles.remove(threadId));

        // Second output + flush
        currentEventHandles.computeIfAbsent(threadId, id -> new ArrayList<>()).add(handle2);
        pendingEventHandles.put("partition-2", currentEventHandles.remove(threadId));

        assertEquals(2, pendingEventHandles.size());
        assertEquals(1, pendingEventHandles.get("partition-1").size());
        assertEquals(1, pendingEventHandles.get("partition-2").size());
    }
}
