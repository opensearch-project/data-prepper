/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SourcePartitionStatusTest {

    @Test
    public void testSourcePartitionStatus() {
        assertThat(SourcePartitionStatus.ASSIGNED.name(), equalTo(SourcePartitionStatus.ASSIGNED.name()));
        assertThat(SourcePartitionStatus.UNASSIGNED.name(), equalTo(SourcePartitionStatus.UNASSIGNED.name()));
        assertThat(SourcePartitionStatus.COMPLETED.name(), equalTo(SourcePartitionStatus.COMPLETED.name()));
        assertThat(SourcePartitionStatus.CLOSED.name(), equalTo(SourcePartitionStatus.CLOSED.name()));
    }
}
