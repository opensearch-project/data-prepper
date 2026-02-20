/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import org.apache.commons.lang3.RandomStringUtils;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SqsSinkConfigTest {
    private SqsSinkConfig sqsSinkConfig;
    SqsThresholdConfig sqsThresholdConfig;

    @BeforeEach
    void setUp() {
        sqsThresholdConfig = mock(SqsThresholdConfig.class);
        sqsSinkConfig = new SqsSinkConfig();
    }

    @Test
    void TestDefaultConfig() {
        assertThat(sqsSinkConfig.getMaxRetries(), equalTo(SqsSinkConfig.DEFAULT_MAX_RETRIES));
        assertThat(sqsSinkConfig.getQueueUrl(), equalTo(null));
        assertThat(sqsSinkConfig.getGroupId(), equalTo(null));
        assertThat(sqsSinkConfig.getDeDuplicationId(), equalTo(null));
        assertThat(sqsSinkConfig.getDlq(), equalTo(null));
        assertThat(sqsSinkConfig.getCodec(), equalTo(null));
        assertThat(sqsSinkConfig.getAwsConfig(), equalTo(null));
        assertThat(sqsSinkConfig.getThresholdConfig().getMaxEventsPerMessage(), equalTo(SqsThresholdConfig.DEFAULT_MESSAGES_PER_EVENT));
        assertThat(sqsSinkConfig.getThresholdConfig().getMaxMessageSizeBytes(), equalTo(SqsThresholdConfig.DEFAULT_MAX_MESSAGE_SIZE.getBytes()));
        assertThat(sqsSinkConfig.getThresholdConfig().getFlushInterval(), equalTo(SqsThresholdConfig.DEFAULT_FLUSH_INTERVAL_TIME));
    }

    @Test
    void TestCustomConfig() throws Exception {
        AwsConfig awsConfig = mock(AwsConfig.class);
        reflectivelySetField(sqsSinkConfig, "awsConfig", awsConfig);
        assertThat(sqsSinkConfig.getAwsConfig(), equalTo(awsConfig));
        final int TEST_MAX_RETRIES = 10;
        reflectivelySetField(sqsSinkConfig, "maxRetries", TEST_MAX_RETRIES);
        assertThat(sqsSinkConfig.getMaxRetries(), equalTo(TEST_MAX_RETRIES));
        final String testQUrl = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(sqsSinkConfig, "queueUrl", testQUrl);
        assertThat(sqsSinkConfig.getQueueUrl(), equalTo(testQUrl));
        final String testGroupId = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(sqsSinkConfig, "groupId", testGroupId);
        assertThat(sqsSinkConfig.getGroupId(), equalTo(testGroupId));
        final String testDeDupId = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(sqsSinkConfig, "deDuplicationId", testDeDupId);
        assertThat(sqsSinkConfig.getDeDuplicationId(), equalTo(testDeDupId));
        reflectivelySetField(sqsSinkConfig, "thresholdConfig", sqsThresholdConfig);
        assertThat(sqsSinkConfig.getThresholdConfig(), equalTo(sqsThresholdConfig));
        PluginModel codec = mock(PluginModel.class);
        reflectivelySetField(sqsSinkConfig, "codec", codec);
        assertThat(sqsSinkConfig.getCodec(), equalTo(codec));
    }

    @Test
    void TestValidDynamicQUrlConfigs() throws Exception {

        final String testQUrl = RandomStringUtils.randomAlphabetic(10)+"${"+RandomStringUtils.randomAlphabetic(5)+"}";
        reflectivelySetField(sqsSinkConfig, "queueUrl", testQUrl);
        assertTrue(sqsSinkConfig.isValidConfig());
    }

    @Test
    void TestValidConfigs() throws Exception {
        final String testQUrl = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(sqsSinkConfig, "queueUrl", testQUrl);
        assertTrue(sqsSinkConfig.isValidConfig());
        String testGroupId = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(sqsSinkConfig, "groupId", testGroupId);
        assertFalse(sqsSinkConfig.isValidConfig());
        reflectivelySetField(sqsSinkConfig, "groupId", null);
        String testDeDupId = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(sqsSinkConfig, "deDuplicationId", testDeDupId);
        assertFalse(sqsSinkConfig.isValidConfig());
    }

    @Test
    void TestValidFiFoQConfigs() throws Exception {
        final String testQUrl = RandomStringUtils.randomAlphabetic(10)+".fifo";
        reflectivelySetField(sqsSinkConfig, "queueUrl", testQUrl);
        assertTrue(sqsSinkConfig.isValidConfig());
        String testGroupId = RandomStringUtils.randomAlphabetic(10);
        reflectivelySetField(sqsSinkConfig, "groupId", testGroupId);
        assertTrue(sqsSinkConfig.isValidConfig());
        
        testGroupId = RandomStringUtils.randomAlphabetic(10)+"${abcd}";
        reflectivelySetField(sqsSinkConfig, "groupId", testGroupId);
        reflectivelySetField(sqsSinkConfig, "thresholdConfig", sqsThresholdConfig);
        when(sqsThresholdConfig.getMaxEventsPerMessage()).thenReturn(1);
        assertTrue(sqsSinkConfig.isValidConfig());
        when(sqsThresholdConfig.getMaxEventsPerMessage()).thenReturn(2);
        assertFalse(sqsSinkConfig.isValidConfig());
        testGroupId = RandomStringUtils.randomAlphabetic(10);
        String testDeDupId = RandomStringUtils.randomAlphabetic(10)+"${abcd}";
        reflectivelySetField(sqsSinkConfig, "groupId", testGroupId);
        reflectivelySetField(sqsSinkConfig, "deDuplicationId", testDeDupId);
        assertFalse(sqsSinkConfig.isValidConfig());
        
    }

    @Test
    void TestValidCodecConfig() throws Exception {
        reflectivelySetField(sqsSinkConfig, "codec", null);
        reflectivelySetField(sqsSinkConfig, "thresholdConfig", sqsThresholdConfig);
        when(sqsThresholdConfig.getMaxEventsPerMessage()).thenReturn(2);
        assertTrue(sqsSinkConfig.isValidCodecConfig());
        when(sqsThresholdConfig.getMaxEventsPerMessage()).thenReturn(1);
        assertTrue(sqsSinkConfig.isValidCodecConfig());
        PluginModel codec = mock(PluginModel.class);
        when(codec.getPluginName()).thenReturn("ndjson");
        reflectivelySetField(sqsSinkConfig, "codec", codec);
        when(sqsThresholdConfig.getMaxEventsPerMessage()).thenReturn(2);
        assertFalse(sqsSinkConfig.isValidCodecConfig());
    }

    private void reflectivelySetField(final SqsSinkConfig sqsSinkConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = SqsSinkConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(sqsSinkConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
    
    
}

