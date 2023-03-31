/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SourcePartitionTest {

    @Test
    void sourcePartitionBuilderWithNullPartitionThrowsNullPointerException() {

        assertThrows(NullPointerException.class, () -> {
            SourcePartition.builder(String.class)
                    .withPartition(null)
                    .withPartitionState(UUID.randomUUID().toString())
                    .build();
        });
    }

    @Test
    void sourcePartitionBuilder_returns_expected_SourcePartition() {
        final String partitionKey = UUID.randomUUID().toString();
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(partitionKey).build();
        final String partitionState = UUID.randomUUID().toString();

        final SourcePartition sourcePartition = SourcePartition.builder(String.class)
                .withPartition(partitionIdentifier)
                .withPartitionState(partitionState)
                .build();

        assertThat(sourcePartition, notNullValue());
        assertThat(sourcePartition.getPartition(), equalTo(partitionIdentifier));
        assertThat(sourcePartition.getPartition().getPartitionKey(), equalTo(partitionKey));
        assertThat(sourcePartition.getPartitionState(), equalTo(partitionState));
    }
}
