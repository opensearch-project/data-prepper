/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class PartitionIdentifierTest {

    @Test
    void testPartitionIdentifierBuilder() {
        final String partitionKey = UUID.randomUUID().toString();

        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(partitionKey).build();

        assertThat(partitionIdentifier.getPartitionKey(), equalTo(partitionKey));
    }

    @Test
    void testPartitionIdentifierEquals_and_hashCode() {
        final String partitionKey = UUID.randomUUID().toString();
        final String differentPartitionKey = UUID.randomUUID().toString();

        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(partitionKey).build();
        final PartitionIdentifier equalPartitionIdentifier = PartitionIdentifier.builder().withPartitionKey(partitionKey).build();

        assertThat(partitionIdentifier.equals(equalPartitionIdentifier), equalTo(true));
        assertThat(partitionIdentifier.hashCode(), equalTo(equalPartitionIdentifier.hashCode()));

        final PartitionIdentifier notEqualPartitionIdentifier = PartitionIdentifier.builder().withPartitionKey(differentPartitionKey).build();
        assertThat(partitionIdentifier.equals(notEqualPartitionIdentifier), equalTo(false));
        assertNotEquals(partitionIdentifier.hashCode(), notEqualPartitionIdentifier.hashCode());

        assertThat(partitionIdentifier.equals(partitionIdentifier), equalTo(true));
        assertThat(partitionIdentifier.equals(null), equalTo(false));

    }
}
