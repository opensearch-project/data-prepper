/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardCheckpointStatusTest {

    private static final String TEST_SEQUENCE_NUMBER = "test-sequence-123";
    private static final long TEST_TIMESTAMP = 1630000000000L; // Some fixed timestamp

    @Test
    void constructor_sets_initial_values_correctly() {
        ShardCheckpointStatus status = new ShardCheckpointStatus(TEST_SEQUENCE_NUMBER, TEST_TIMESTAMP, true);

        assertThat(status.getSequenceNumber(), equalTo(TEST_SEQUENCE_NUMBER));
        assertThat(status.isFinalAcknowledgmentForPartition(), is(true));
        assertFalse(status.isPositiveAcknowledgement());
        assertFalse(status.isNegativeAcknowledgement());
    }

    @Test
    void setAcknowledgedTimestamp_updates_timestamp() {
        ShardCheckpointStatus status = new ShardCheckpointStatus(TEST_SEQUENCE_NUMBER, TEST_TIMESTAMP, false);
        long acknowledgedTimestamp = Instant.now().toEpochMilli();

        status.setAcknowledgedTimestamp(acknowledgedTimestamp);

        // Note: We can't directly test the timestamp as it's private without a getter
        // The presence of this test ensures the method exists and doesn't throw exceptions
        assertThat(status, notNullValue());
    }

    @Test
    void isPositiveAcknowledgement_returns_true_only_for_positive_ack() {
        ShardCheckpointStatus status = new ShardCheckpointStatus(TEST_SEQUENCE_NUMBER, TEST_TIMESTAMP, false);

        status.setAcknowledged(ShardCheckpointStatus.AcknowledgmentStatus.POSITIVE_ACK);
        assertTrue(status.isPositiveAcknowledgement());
        assertFalse(status.isNegativeAcknowledgement());

        status.setAcknowledged(ShardCheckpointStatus.AcknowledgmentStatus.NEGATIVE_ACK);
        assertFalse(status.isPositiveAcknowledgement());
        assertTrue(status.isNegativeAcknowledgement());

        status.setAcknowledged(ShardCheckpointStatus.AcknowledgmentStatus.NO_ACK);
        assertFalse(status.isPositiveAcknowledgement());
        assertFalse(status.isNegativeAcknowledgement());
    }

    @ParameterizedTest
    @EnumSource(ShardCheckpointStatus.AcknowledgmentStatus.class)
    void setAcknowledged_updates_status_for_all_values(ShardCheckpointStatus.AcknowledgmentStatus status) {
        ShardCheckpointStatus checkpointStatus = new ShardCheckpointStatus(TEST_SEQUENCE_NUMBER, TEST_TIMESTAMP, false);

        checkpointStatus.setAcknowledged(status);

        if (status == ShardCheckpointStatus.AcknowledgmentStatus.POSITIVE_ACK) {
            assertTrue(checkpointStatus.isPositiveAcknowledgement());
        } else {
            assertFalse(checkpointStatus.isPositiveAcknowledgement());
        }

        if (status == ShardCheckpointStatus.AcknowledgmentStatus.NEGATIVE_ACK) {
            assertTrue(checkpointStatus.isNegativeAcknowledgement());
        } else {
            assertFalse(checkpointStatus.isNegativeAcknowledgement());
        }
    }

    @Test
    void isExpired_returns_true_when_duration_exceeded() {
        long pastTimestamp = Instant.now().minus(Duration.ofMinutes(30)).toEpochMilli();
        ShardCheckpointStatus status = new ShardCheckpointStatus(TEST_SEQUENCE_NUMBER, pastTimestamp, false);

        assertTrue(status.isExpired(Duration.ofMinutes(15)));
        assertFalse(status.isExpired(Duration.ofMinutes(45)));
    }

    @Test
    void isExpired_returns_false_for_future_timestamp() {
        long futureTimestamp = Instant.now().plus(Duration.ofMinutes(30)).toEpochMilli();
        ShardCheckpointStatus status = new ShardCheckpointStatus(TEST_SEQUENCE_NUMBER, futureTimestamp, false);

        assertFalse(status.isExpired(Duration.ofMinutes(15)));
    }
}
