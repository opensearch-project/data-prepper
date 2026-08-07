/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

class CheckpointEntryTest {

    private CheckpointEntry checkpointEntry;

    @BeforeEach
    void setUp() {
        checkpointEntry = new CheckpointEntry();
    }

    @Test
    void defaultConstructorSetsZeroOffsets() {
        assertThat(checkpointEntry.getReadOffset(), equalTo(0L));
        assertThat(checkpointEntry.getCommittedOffset(), equalTo(0L));
    }

    @Test
    void defaultConstructorSetsActiveStatus() {
        assertThat(checkpointEntry.getStatus(), equalTo(CheckpointStatus.ACTIVE));
    }

    @Test
    void defaultConstructorSetsLastUpdatedMillis() {
        final long before = System.currentTimeMillis();
        final CheckpointEntry entry = new CheckpointEntry();
        final long after = System.currentTimeMillis();

        assertThat(entry.getLastUpdatedMillis(), greaterThanOrEqualTo(before));
        assertThat(entry.getLastUpdatedMillis(), lessThanOrEqualTo(after));
    }

    @Test
    void parameterizedConstructorSetsAllFields() {
        final CheckpointEntry entry = new CheckpointEntry(100L, 50L, CheckpointStatus.COMPLETED);

        assertThat(entry.getReadOffset(), equalTo(100L));
        assertThat(entry.getCommittedOffset(), equalTo(50L));
        assertThat(entry.getStatus(), equalTo(CheckpointStatus.COMPLETED));
    }

    @Test
    void setReadOffsetUpdatesValueAndTimestamp() {
        final long beforeUpdate = System.currentTimeMillis();
        checkpointEntry.setReadOffset(500L);
        final long afterUpdate = System.currentTimeMillis();

        assertThat(checkpointEntry.getReadOffset(), equalTo(500L));
        assertThat(checkpointEntry.getLastUpdatedMillis(), greaterThanOrEqualTo(beforeUpdate));
        assertThat(checkpointEntry.getLastUpdatedMillis(), lessThanOrEqualTo(afterUpdate));
    }

    @Test
    void setCommittedOffsetUpdatesValueAndTimestamp() {
        final long beforeUpdate = System.currentTimeMillis();
        checkpointEntry.setCommittedOffset(300L);
        final long afterUpdate = System.currentTimeMillis();

        assertThat(checkpointEntry.getCommittedOffset(), equalTo(300L));
        assertThat(checkpointEntry.getLastUpdatedMillis(), greaterThanOrEqualTo(beforeUpdate));
        assertThat(checkpointEntry.getLastUpdatedMillis(), lessThanOrEqualTo(afterUpdate));
    }

    @Test
    void setStatusUpdatesValueAndTimestamp() {
        final long beforeUpdate = System.currentTimeMillis();
        checkpointEntry.setStatus(CheckpointStatus.COMPLETED);
        final long afterUpdate = System.currentTimeMillis();

        assertThat(checkpointEntry.getStatus(), equalTo(CheckpointStatus.COMPLETED));
        assertThat(checkpointEntry.getLastUpdatedMillis(), greaterThanOrEqualTo(beforeUpdate));
        assertThat(checkpointEntry.getLastUpdatedMillis(), lessThanOrEqualTo(afterUpdate));
    }

    @Test
    void snapshotReturnsNewInstanceWithSameValues() {
        checkpointEntry.setReadOffset(200L);
        checkpointEntry.setCommittedOffset(100L);
        checkpointEntry.setStatus(CheckpointStatus.COMPLETED);

        final CheckpointEntry snapshot = checkpointEntry.snapshot();

        assertThat(snapshot, notNullValue());
        assertThat(snapshot.getReadOffset(), equalTo(200L));
        assertThat(snapshot.getCommittedOffset(), equalTo(100L));
        assertThat(snapshot.getStatus(), equalTo(CheckpointStatus.COMPLETED));
    }

    @Test
    void snapshotIsIndependentOfOriginal() {
        checkpointEntry.setReadOffset(200L);
        checkpointEntry.setCommittedOffset(100L);

        final CheckpointEntry snapshot = checkpointEntry.snapshot();

        checkpointEntry.setReadOffset(999L);
        checkpointEntry.setCommittedOffset(888L);
        checkpointEntry.setStatus(CheckpointStatus.COMPLETED);

        assertThat(snapshot.getReadOffset(), equalTo(200L));
        assertThat(snapshot.getCommittedOffset(), equalTo(100L));
        assertThat(snapshot.getStatus(), equalTo(CheckpointStatus.ACTIVE));
    }

    @Test
    void multipleAdvancesAccumulateCorrectly() {
        checkpointEntry.setReadOffset(100L);
        checkpointEntry.setReadOffset(200L);
        checkpointEntry.setReadOffset(300L);

        assertThat(checkpointEntry.getReadOffset(), equalTo(300L));
    }

    @Test
    void statusTransitionFromActiveToCompleted() {
        assertThat(checkpointEntry.getStatus(), equalTo(CheckpointStatus.ACTIVE));

        checkpointEntry.setStatus(CheckpointStatus.COMPLETED);

        assertThat(checkpointEntry.getStatus(), equalTo(CheckpointStatus.COMPLETED));
    }
}
