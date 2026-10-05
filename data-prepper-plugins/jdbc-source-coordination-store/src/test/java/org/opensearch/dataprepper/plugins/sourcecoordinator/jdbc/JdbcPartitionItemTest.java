/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.jdbc;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class JdbcPartitionItemTest {

    @Test
    void getters_and_setters() {
        final JdbcPartitionItem item = new JdbcPartitionItem();
        final Instant now = Instant.now();

        item.setSourceIdentifier("source-1");
        item.setSourcePartitionKey("partition-1");
        item.setPartitionOwner("owner-1");
        item.setPartitionProgressState("{\"offset\":100}");
        item.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        item.setPartitionOwnershipTimeout(now);
        item.setReOpenAt(now.plusSeconds(60));
        item.setClosedCount(3L);
        item.setPartitionPriority(now.toString());
        item.setVersion(5);

        assertThat(item.getSourceIdentifier(), equalTo("source-1"));
        assertThat(item.getSourcePartitionKey(), equalTo("partition-1"));
        assertThat(item.getPartitionOwner(), equalTo("owner-1"));
        assertThat(item.getPartitionProgressState(), equalTo("{\"offset\":100}"));
        assertThat(item.getSourcePartitionStatus(), equalTo(SourcePartitionStatus.ASSIGNED));
        assertThat(item.getPartitionOwnershipTimeout(), equalTo(now));
        assertThat(item.getReOpenAt(), equalTo(now.plusSeconds(60)));
        assertThat(item.getClosedCount(), equalTo(3L));
        assertThat(item.getPartitionPriority(), equalTo(now.toString()));
        assertThat(item.getVersion(), equalTo(5L));
    }

    @Test
    void defaults_are_null_or_zero() {
        final JdbcPartitionItem item = new JdbcPartitionItem();

        assertThat(item.getSourceIdentifier(), is(nullValue()));
        assertThat(item.getSourcePartitionKey(), is(nullValue()));
        assertThat(item.getPartitionOwner(), is(nullValue()));
        assertThat(item.getPartitionProgressState(), is(nullValue()));
        assertThat(item.getSourcePartitionStatus(), is(nullValue()));
        assertThat(item.getPartitionOwnershipTimeout(), is(nullValue()));
        assertThat(item.getReOpenAt(), is(nullValue()));
        assertThat(item.getClosedCount(), is(nullValue()));
        assertThat(item.getPartitionPriority(), is(nullValue()));
        assertThat(item.getVersion(), equalTo(0L));
    }
}
