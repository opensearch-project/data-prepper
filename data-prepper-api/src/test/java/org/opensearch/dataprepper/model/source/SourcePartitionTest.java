/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SourcePartitionTest {

    @Test
    void sourcePartitionBuilderWithNullPartitionKeyThrowsNullPointerException() {

        assertThrows(NullPointerException.class, () -> {
            SourcePartition.builder()
                    .withPartitionKey(null)
                    .withPartitionState(Collections.emptyMap())
                    .build();
        });
    }

    @Test
    void sourcePartitionBuilder_returns_expected_SourcePartition() {
        final String partitionKey = UUID.randomUUID().toString();
        final String partitionState = UUID.randomUUID().toString();

        final SourcePartition sourcePartition = SourcePartition.builder()
                .withPartitionKey(partitionKey)
                .withPartitionState(partitionState)
                .build();

        assertThat(sourcePartition, notNullValue());
        assertThat(sourcePartition.getPartitionKey(), equalTo(partitionKey));
        assertThat(sourcePartition.getPartitionState(), equalTo(partitionState));
    }
}
