/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import static org.awaitility.Awaitility.await;

class ReadBufferHelper {
    static Map.Entry<Collection<Record<Event>>, CheckpointState> awaitRead(final KafkaBuffer objectUnderTest) {
        final Map.Entry<Collection<Record<Event>>, CheckpointState>[] lastReadResult = new Map.Entry[1];
        await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> {
                    lastReadResult[0] = objectUnderTest.read(500);
                    return lastReadResult[0] != null && lastReadResult[0].getKey() != null && lastReadResult[0].getKey().size() >= 1;
                });
        return lastReadResult[0];
    }
}
