/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerProperties;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class KafkaBufferConfigTest {
    private KafkaBufferConfig createObjectUnderTest() {
        return new KafkaBufferConfig();
    }

    @Test
    void getBootstrapServers_returns_null_when_not_set() {
        assertThat(createObjectUnderTest().getBootstrapServers(), nullValue());
    }

    @Test
    void getBootstrapServers_returns_configured_value() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        List<String> servers = List.of("localhost:9092");
        setField(KafkaBufferConfig.class, objectUnderTest, "bootstrapServers", servers);
        assertThat(objectUnderTest.getBootstrapServers(), equalTo(servers));
    }

    @Test
    void getAuthConfig_returns_configured_value() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        AuthConfig authConfig = mock(AuthConfig.class);
        setField(KafkaBufferConfig.class, objectUnderTest, "authConfig", authConfig);
        assertThat(objectUnderTest.getAuthConfig(), equalTo(authConfig));
    }

    @Test
    void getSchemaConfig_returns_null() {
        assertThat(createObjectUnderTest().getSchemaConfig(), nullValue());
    }

    @Test
    void getTopic_returns_first_topic() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        BufferTopicConfig topicConfig = mock(BufferTopicConfig.class);
        setField(KafkaBufferConfig.class, objectUnderTest, "topics", Collections.singletonList(topicConfig));
        assertThat(objectUnderTest.getTopic(), equalTo(topicConfig));
    }

    @Test
    void getTopics_returns_configured_topics() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        BufferTopicConfig topicConfig = mock(BufferTopicConfig.class);
        List<BufferTopicConfig> topics = Collections.singletonList(topicConfig);
        setField(KafkaBufferConfig.class, objectUnderTest, "topics", topics);
        assertThat(objectUnderTest.getTopics(), equalTo(topics));
    }

    @Test
    void getKafkaProducerProperties_returns_configured_value() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        KafkaProducerProperties properties = mock(KafkaProducerProperties.class);
        setField(KafkaBufferConfig.class, objectUnderTest, "kafkaProducerProperties", properties);
        assertThat(objectUnderTest.getKafkaProducerProperties(), equalTo(properties));
    }

    @Test
    void getPartitionKey_returns_pipeline_buffer() {
        assertThat(createObjectUnderTest().getPartitionKey(), equalTo("pipeline-buffer"));
    }

    @Test
    void getDlq_returns_empty() {
        assertThat(createObjectUnderTest().getDlq().isEmpty(), equalTo(true));
    }

    @Test
    void getAwsConfig_returns_configured_value() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        AwsConfig awsConfig = mock(AwsConfig.class);
        setField(KafkaBufferConfig.class, objectUnderTest, "awsConfig", awsConfig);
        assertThat(objectUnderTest.getAwsConfig(), equalTo(awsConfig));
    }

    @Test
    void getEncryptionConfig_returns_default_when_not_set() {
        assertThat(createObjectUnderTest().getEncryptionConfig(), notNullValue());
    }

    @Test
    void getEncryptionConfig_returns_configured_value() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);
        setField(KafkaBufferConfig.class, objectUnderTest, "encryptionConfig", encryptionConfig);
        assertThat(objectUnderTest.getEncryptionConfig(), equalTo(encryptionConfig));
    }

    @Test
    void getClientDnsLookup_returns_null() {
        assertThat(createObjectUnderTest().getClientDnsLookup(), nullValue());
    }

    @Test
    void getAcknowledgementsEnabled_returns_true() {
        assertTrue(createObjectUnderTest().getAcknowledgementsEnabled());
    }

    @Test
    void getDrainTimeout_returns_default_value() {
        assertThat(createObjectUnderTest().getDrainTimeout(), equalTo(Duration.ofSeconds(30)));
    }

    @Test
    void getDrainTimeout_returns_configured_value() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        Duration drainTimeout = Duration.ofSeconds(60);
        setField(KafkaBufferConfig.class, objectUnderTest, "drainTimeout", drainTimeout);
        assertThat(objectUnderTest.getDrainTimeout(), equalTo(drainTimeout));
    }

    @Test
    void getAcknowledgementsTimeout_returns_default_value() {
        assertThat(createObjectUnderTest().getAcknowledgementsTimeout(), equalTo(KafkaBufferConfig.DEFAULT_ACKNOWLEDGEMENTS_TIMEOUT));
    }

    @Test
    void getAcknowledgementsTimeout_returns_configured_value() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        Duration acknowledgementsTimeout = Duration.ofHours(1);
        setField(KafkaBufferConfig.class, objectUnderTest, "acknowledgementsTimeout", acknowledgementsTimeout);
        assertThat(objectUnderTest.getAcknowledgementsTimeout(), equalTo(acknowledgementsTimeout));
    }

    @Test
    void getCustomMetricPrefix_returns_empty_when_not_set() {
        assertThat(createObjectUnderTest().getCustomMetricPrefix().isEmpty(), equalTo(true));
    }

    @Test
    void getCustomMetricPrefix_returns_configured_value() throws NoSuchFieldException, IllegalAccessException {
        KafkaBufferConfig objectUnderTest = createObjectUnderTest();
        String prefix = "custom_prefix";
        setField(KafkaBufferConfig.class, objectUnderTest, "customMetricPrefix", prefix);
        assertThat(objectUnderTest.getCustomMetricPrefix().get(), equalTo(prefix));
    }
}
