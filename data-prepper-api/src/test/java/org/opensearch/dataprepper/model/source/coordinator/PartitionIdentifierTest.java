/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class PartitionIdentifierTest {

    @Test
    void testPartitionIdentifierBuilder() {
        final String partitionKey = UUID.randomUUID().toString();

        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(partitionKey).build();

        assertThat(partitionIdentifier.getPartitionKey(), equalTo(partitionKey));
    }
}
