/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.kafka.configuration.KmsConfig;

import java.time.Duration;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class BufferTopicConfigTest {
    // ParameterMessageInterpolator avoids requiring a Jakarta EL provider on the test classpath.
    private static final Validator VALIDATOR;

    static {
        try (final ValidatorFactory factory = Validation.byDefaultProvider().configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()) {
            VALIDATOR = factory.getValidator();
        }
    }

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
        assertThat(objectUnderTest.getConnectionsMaxIdle(), equalTo(BufferTopicConfig.DEFAULT_CONNECTIONS_MAX_IDLE));
    }

    @Test
    void verify_custom_connections_max_idle() throws NoSuchFieldException, IllegalAccessException {
        BufferTopicConfig objectUnderTest = createObjectUnderTest();

        setField(BufferTopicConfig.class, objectUnderTest, "connectionsMaxIdle", Duration.ofSeconds(180));
        assertThat(objectUnderTest.getConnectionsMaxIdle(), equalTo(Duration.ofSeconds(180)));
    }

    @Test
    void connections_max_idle_at_or_above_one_second_is_valid() throws NoSuchFieldException, IllegalAccessException {
        BufferTopicConfig objectUnderTest = createObjectUnderTest();
        setField(BufferTopicConfig.class, objectUnderTest, "connectionsMaxIdle", Duration.ofSeconds(1));
        assertThat(VALIDATOR.validateProperty(objectUnderTest, "connectionsMaxIdle"), empty());
    }

    @Test
    void connections_max_idle_below_one_second_fails_validation() throws NoSuchFieldException, IllegalAccessException {
        BufferTopicConfig objectUnderTest = createObjectUnderTest();
        setField(BufferTopicConfig.class, objectUnderTest, "connectionsMaxIdle", Duration.ofMillis(500));
        assertThat(VALIDATOR.validateProperty(objectUnderTest, "connectionsMaxIdle"), not(empty()));
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
