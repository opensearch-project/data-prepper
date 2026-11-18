/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventHandle;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class BatchTest {

    @Test
    void constructor_sourceListsChangedAfterwards_batchListsUnchanged() {
        final EventHandle handle = mock(EventHandle.class);
        final byte[] message = "m".getBytes(StandardCharsets.UTF_8);
        final List<EventHandle> handles = new ArrayList<>(List.of(handle));
        final List<byte[]> messages = new ArrayList<>(List.of(message));
        final Entity entity = Entity.builder().build();
        final Map<String, String> headers = Map.of("h", "v");

        final Batch batch = new Batch(handles, messages, entity, headers);
        handles.clear();
        messages.add("other".getBytes(StandardCharsets.UTF_8));

        assertThat(batch.getEventHandles(), equalTo(List.of(handle)));
        assertThat(batch.getMessages().size(), equalTo(1));
        assertThat(batch.getMessages().get(0), sameInstance(message));
        assertThat(batch.getEntity(), sameInstance(entity));
        assertThat(batch.getHeaders(), sameInstance(headers));
        assertThrows(UnsupportedOperationException.class, () -> batch.getEventHandles().add(handle));
    }

    @Test
    void constructor_emptyOrMismatchedLists_throws() {
        assertThrows(IllegalArgumentException.class, () -> new Batch(List.of(), List.of(), null, null));
        assertThrows(IllegalArgumentException.class,
                () -> new Batch(List.of(mock(EventHandle.class)), List.of(), null, null));
    }

    @Test
    void getReleased_equalButDistinctHandles_trackedByIdentity() {
        final EventHandle first = new AlwaysEqualHandle();
        final EventHandle second = new AlwaysEqualHandle();
        final Batch batch = new Batch(List.of(first, second), List.of(new byte[0], new byte[0]), null, null);

        assertThat(batch.getReleased().add(first), equalTo(true));
        assertThat(batch.getReleased().add(second), equalTo(true));
        assertThat(batch.getReleased().add(first), equalTo(false));
    }

    private static final class AlwaysEqualHandle implements EventHandle {
        @Override
        public boolean release(final boolean result) {
            return true;
        }

        @Override
        public void setExternalOriginationTime(final Instant externalOriginationTime) {
        }

        @Override
        public Instant getExternalOriginationTime() {
            return null;
        }

        @Override
        public Instant getInternalOriginationTime() {
            return null;
        }

        @Override
        public void onRelease(final BiConsumer<EventHandle, Boolean> releaseConsumer) {
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof AlwaysEqualHandle;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }
}
