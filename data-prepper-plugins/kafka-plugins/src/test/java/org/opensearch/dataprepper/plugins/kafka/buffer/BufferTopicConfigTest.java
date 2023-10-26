/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
}