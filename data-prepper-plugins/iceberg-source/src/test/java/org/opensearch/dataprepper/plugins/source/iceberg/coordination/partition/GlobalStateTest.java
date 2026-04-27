/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GlobalStateTest {

    @Test
    void getPartitionType_returnsNull() {
        final GlobalState globalState = new GlobalState("test-state", Map.of("key", "value"));
        assertThat(globalState.getPartitionType(), nullValue());
    }

    @Test
    void getPartitionKey_returnsProvidedStateName() {
        final GlobalState globalState = new GlobalState("test-state", Map.of());
        assertThat(globalState.getPartitionKey(), equalTo("test-state"));
    }

    @Test
    void getProgressState_returnsProvidedState() {
        final Map<String, Object> state = Map.of("snapshotId", 42, "completed", true);
        final GlobalState globalState = new GlobalState("test-state", state);
        assertThat(globalState.getProgressState().isPresent(), equalTo(true));
        assertThat(globalState.getProgressState().get(), equalTo(state));
    }

    @Test
    void setProgressState_replacesState() {
        final GlobalState globalState = new GlobalState("test-state", Map.of("old", "value"));
        final Map<String, Object> newState = Map.of("new", "value");
        globalState.setProgressState(newState);
        assertThat(globalState.getProgressState().get(), equalTo(newState));
    }

    @Test
    void fromSourcePartitionStoreItem_returnsRestoredState() {
        final SourcePartitionStoreItem item = mock(SourcePartitionStoreItem.class);
        when(item.getSourceIdentifier()).thenReturn("prefix|unknown");
        when(item.getSourcePartitionKey()).thenReturn("snapshot-completion-db.table1");
        when(item.getPartitionProgressState()).thenReturn("{\"completed\":true,\"snapshotId\":42}");

        final GlobalState globalState = new GlobalState(item);
        assertThat(globalState.getPartitionKey(), equalTo("snapshot-completion-db.table1"));
        assertThat(globalState.getProgressState().isPresent(), equalTo(true));
        assertThat(globalState.getProgressState().get().get("completed"), equalTo(true));
        assertThat(globalState.getProgressState().get().get("snapshotId"), equalTo(42));
    }
}
