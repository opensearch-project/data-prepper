/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.kafka.configuration.KmsConfig;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class BufferTopicConfigTest {
    private BufferTopicConfig createObjectUnderTest() {
        return new BufferTopicConfig();
    }

    @Test
    void verify_default_values() {
        BufferTopicConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getAutoCommit(), equalTo(BufferTopicConfig.DEFAULT_AUTO_COMMIT));
        assertThat(objectUnderTest.getCommitInterval(), equalTo(BufferTopicConfig.DEFAULT_COMMIT_INTERVAL));
        assertThat(objectUnderTest.getFetchMaxWait(), equalTo((int) BufferTopicConfig.DEFAULT_FETCH_MAX_WAIT.toMillis()));
        assertThat(objectUnderTest.getFetchMinBytes(), equalTo(BufferTopicConfig.DEFAULT_FETCH_MIN_BYTES.getBytes()));
        assertThat(objectUnderTest.getFetchMaxBytes(), equalTo(BufferTopicConfig.DEFAULT_FETCH_MAX_BYTES.getBytes()));
        assertThat(objectUnderTest.getMaxPartitionFetchBytes(), equalTo(BufferTopicConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES.getBytes()));

        assertThat(objectUnderTest.getSessionTimeOut(), equalTo(BufferTopicConfig.DEFAULT_SESSION_TIMEOUT));
        assertThat(objectUnderTest.getAutoOffsetReset(), equalTo(BufferTopicConfig.DEFAULT_AUTO_OFFSET_RESET));
        assertThat(objectUnderTest.getThreadWaitingTime(), equalTo(BufferTopicConfig.DEFAULT_THREAD_WAITING_TIME));
        assertThat(objectUnderTest.getMaxPollInterval(), equalTo(BufferTopicConfig.DEFAULT_MAX_POLL_INTERVAL));
        assertThat(objectUnderTest.getConsumerMaxPollRecords(), equalTo(BufferTopicConfig.DEFAULT_CONSUMER_MAX_POLL_RECORDS));
        assertThat(objectUnderTest.getWorkers(), equalTo(BufferTopicConfig.DEFAULT_NUM_OF_WORKERS));
        assertThat(objectUnderTest.getHeartBeatInterval(), equalTo(BufferTopicConfig.DEFAULT_HEART_BEAT_INTERVAL_DURATION));
    }

    @Test
    void getFetchMaxBytes_on_large_value() throws NoSuchFieldException, IllegalAccessException {
        BufferTopicConfig objectUnderTest = createObjectUnderTest();

        setField(BufferTopicConfig.class, objectUnderTest, "fetchMaxBytes", ByteCount.parse("60mb"));
        assertThrows(RuntimeException.class, () -> objectUnderTest.getFetchMaxBytes());
    }

    @Test
    void invalid_getFetchMaxBytes_zero_bytes() throws NoSuchFieldException, IllegalAccessException {
        BufferTopicConfig objectUnderTest = createObjectUnderTest();

        setField(BufferTopicConfig.class, objectUnderTest, "fetchMaxBytes", ByteCount.zeroBytes());
        assertThrows(RuntimeException.class, () -> objectUnderTest.getFetchMaxBytes());
    }

    @Test
    void invalid_encryption_at_rest_setting_with_both_encryption_id_and_encryption_key()
            throws NoSuchFieldException, IllegalAccessException {
        BufferTopicConfig objectUnderTest = createObjectUnderTest();

        setField(BufferTopicConfig.class, objectUnderTest, "encryptionId", UUID.randomUUID().toString());
        setField(BufferTopicConfig.class, objectUnderTest, "encryptionKey", UUID.randomUUID().toString());
        assertThat(objectUnderTest.IsEncryptionAtRestSettingValid(), is(false));
    }

    @Test
    void invalid_encryption_at_rest_setting_with_both_encryption_id_and_kms()
            throws NoSuchFieldException, IllegalAccessException {
        BufferTopicConfig objectUnderTest = createObjectUnderTest();

        setField(BufferTopicConfig.class, objectUnderTest, "encryptionId", UUID.randomUUID().toString());
        setField(BufferTopicConfig.class, objectUnderTest, "kmsConfig", mock(KmsConfig.class));
        assertThat(objectUnderTest.IsEncryptionAtRestSettingValid(), is(false));
    }

}
