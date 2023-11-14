/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class SourceTopicConfigTest {
    private SourceTopicConfig createObjectUnderTest() {
        return new SourceTopicConfig();
    }

    @Test
    void verify_default_values() {
        SourceTopicConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getAutoCommit(), equalTo(SourceTopicConfig.DEFAULT_AUTO_COMMIT));
        assertThat(objectUnderTest.getCommitInterval(), equalTo(SourceTopicConfig.DEFAULT_COMMIT_INTERVAL));
        assertThat(objectUnderTest.getFetchMaxWait(), equalTo(SourceTopicConfig.DEFAULT_FETCH_MAX_WAIT));
        assertThat(objectUnderTest.getFetchMinBytes(), equalTo(ByteCount.parse(SourceTopicConfig.DEFAULT_FETCH_MIN_BYTES).getBytes()));
        assertThat(objectUnderTest.getFetchMaxBytes(), equalTo(ByteCount.parse(SourceTopicConfig.DEFAULT_FETCH_MAX_BYTES).getBytes()));
        assertThat(objectUnderTest.getMaxPartitionFetchBytes(), equalTo(ByteCount.parse(SourceTopicConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES).getBytes()));

        assertThat(objectUnderTest.getSessionTimeOut(), equalTo(SourceTopicConfig.DEFAULT_SESSION_TIMEOUT));
        assertThat(objectUnderTest.getAutoOffsetReset(), equalTo(SourceTopicConfig.DEFAULT_AUTO_OFFSET_RESET));
        assertThat(objectUnderTest.getThreadWaitingTime(), equalTo(SourceTopicConfig.DEFAULT_THREAD_WAITING_TIME));
        assertThat(objectUnderTest.getMaxPollInterval(), equalTo(SourceTopicConfig.DEFAULT_MAX_POLL_INTERVAL));
        assertThat(objectUnderTest.getConsumerMaxPollRecords(), equalTo(SourceTopicConfig.DEFAULT_CONSUMER_MAX_POLL_RECORDS));
        assertThat(objectUnderTest.getWorkers(), equalTo(SourceTopicConfig.DEFAULT_NUM_OF_WORKERS));
        assertThat(objectUnderTest.getHeartBeatInterval(), equalTo(SourceTopicConfig.DEFAULT_HEART_BEAT_INTERVAL_DURATION));
    }

    @Test
    void getFetchMaxBytes_on_large_value() throws NoSuchFieldException, IllegalAccessException {
        SourceTopicConfig objectUnderTest = createObjectUnderTest();

        setField(SourceTopicConfig.class, objectUnderTest, "fetchMaxBytes", "60mb");
        assertThrows(RuntimeException.class, () -> objectUnderTest.getFetchMaxBytes());
    }

    @Test
    void invalid_getFetchMaxBytes_zero_bytes() throws NoSuchFieldException, IllegalAccessException {
        SourceTopicConfig objectUnderTest = createObjectUnderTest();

        setField(SourceTopicConfig.class, objectUnderTest, "fetchMaxBytes", "0b");
        assertThrows(RuntimeException.class, () -> objectUnderTest.getFetchMaxBytes());
    }
}