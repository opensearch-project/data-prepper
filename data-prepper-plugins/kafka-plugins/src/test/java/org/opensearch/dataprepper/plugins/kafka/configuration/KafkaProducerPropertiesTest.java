/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.util.Random;

class KafkaProducerPropertiesTest {

    private KafkaProducerProperties kafkaProducerProperties;
    private String randomTestString;
    private Long randomTestLong;
    private Integer randomTestInt;

    KafkaProducerProperties createObjectUnderTest() {
        return new KafkaProducerProperties();
    }

    @BeforeEach
    void setUp() {
        kafkaProducerProperties = createObjectUnderTest();
        randomTestString = RandomStringUtils.randomAlphabetic(10);
        Random random = new Random();
        randomTestLong = random.nextLong();
        randomTestInt = random.nextInt();
    }

    @Test
    void test_defaultValues() {
        assertThat(kafkaProducerProperties.getBufferMemory(), equalTo(KafkaProducerProperties.DEFAULT_BYTE_CAPACITY));
        assertThat(kafkaProducerProperties.getConnectionsMaxIdleMs(), equalTo(KafkaProducerProperties.DEFAULT_CONNECTION_MAX_IDLE_MS.toMillis()));
        assertThat(kafkaProducerProperties.getDeliveryTimeoutMs(), equalTo(KafkaProducerProperties.DEFAULT_DELIVERY_TIMEOUT_MS.toMillis()));
        assertThat(kafkaProducerProperties.getMaxBlockMs(), equalTo(KafkaProducerProperties.DEFAULT_MAX_BLOCK_MS.toMillis()));
        assertThat(kafkaProducerProperties.getLingerMs(), equalTo(KafkaProducerProperties.DEFAULT_LINGER_MS));
        assertThat(kafkaProducerProperties.getMaxRequestSize(), equalTo(KafkaProducerProperties.DEFAULT_MAX_REQUEST_SIZE));
        assertThat(kafkaProducerProperties.getReceiveBufferBytes(), equalTo(KafkaProducerProperties.DEFAULT_BYTE_CAPACITY));
        assertThat(kafkaProducerProperties.getRequestTimeoutMs(), equalTo(KafkaProducerProperties.DEFAULT_REQUEST_TIMEOUT_MS.toMillis()));
        assertThat(kafkaProducerProperties.getSendBufferBytes(), equalTo(KafkaProducerProperties.DEFAULT_BYTE_CAPACITY));
        assertThat(kafkaProducerProperties.getSocketConnectionSetupMaxTimeout(), equalTo(KafkaProducerProperties.DEFAULT_SOCKET_CONNECTION_SETUP_MAX_TIMEOUT.toMillis()));
        assertThat(kafkaProducerProperties.getSocketConnectionSetupTimeout(), equalTo(KafkaProducerProperties.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT.toMillis()));
        assertThat(kafkaProducerProperties.getMetadataMaxAgeMs(), equalTo(KafkaProducerProperties.DEFAULT_METADATA_MAX_AGE_MS.toMillis()));
        assertThat(kafkaProducerProperties.getMetadataMaxIdleMs(), equalTo(KafkaProducerProperties.DEFAULT_METADATA_MAX_IDLE_MS.toMillis()));
        assertThat(kafkaProducerProperties.getMetricsSampleWindowMs(), equalTo(KafkaProducerProperties.DEFAULT_METRICS_SAMPLE_WINDOW_MS.toMillis()));
        assertThat(kafkaProducerProperties.getPartitionerAvailabilityTimeoutMs(), equalTo(KafkaProducerProperties.DEFAULT_PARTITIONER_AVAILABILITY_TIMEOUT_MS.toMillis()));
        assertThat(kafkaProducerProperties.getReconnectBackoffMaxMs(), equalTo(KafkaProducerProperties.DEFAULT_RECONNECT_BACKOFF_MAX_MS.toMillis()));
        assertThat(kafkaProducerProperties.getReconnectBackoffMs(), equalTo(KafkaProducerProperties.DEFAULT_RECONNECT_BACKOFF_MS.toMillis()));
        assertThat(kafkaProducerProperties.getRetryBackoffMs(), equalTo(KafkaProducerProperties.DEFAULT_RETRY_BACKOFF_MS.toMillis()));
    }

    @Test
    void test_nonDefaultValues() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(kafkaProducerProperties, "bufferMemory", randomTestString);
        
        assertThat(kafkaProducerProperties.getBufferMemory(), equalTo(randomTestString));
        reflectivelySetField(kafkaProducerProperties, "connectionsMaxIdleMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getConnectionsMaxIdleMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "deliveryTimeoutMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getDeliveryTimeoutMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "maxBlockMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getMaxBlockMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "lingerMs", randomTestLong);
        assertThat(kafkaProducerProperties.getLingerMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "maxRequestSize", randomTestInt);
        assertThat(kafkaProducerProperties.getMaxRequestSize(), equalTo(randomTestInt));

        reflectivelySetField(kafkaProducerProperties, "receiveBufferBytes", randomTestString);
        assertThat(kafkaProducerProperties.getReceiveBufferBytes(), equalTo(randomTestString));

        reflectivelySetField(kafkaProducerProperties, "requestTimeoutMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getRequestTimeoutMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "sendBufferBytes", randomTestString);
        assertThat(kafkaProducerProperties.getSendBufferBytes(), equalTo(randomTestString));

        reflectivelySetField(kafkaProducerProperties, "socketConnectionSetupMaxTimeout", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getSocketConnectionSetupMaxTimeout(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "socketConnectionSetupTimeout", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getSocketConnectionSetupTimeout(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "metadataMaxAgeMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getMetadataMaxAgeMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "metadataMaxIdleMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getMetadataMaxIdleMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "metricsSampleWindowMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getMetricsSampleWindowMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "partitionerAvailabilityTimeoutMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getPartitionerAvailabilityTimeoutMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "reconnectBackoffMaxMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getReconnectBackoffMaxMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "reconnectBackoffMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getReconnectBackoffMs(), equalTo(randomTestLong));

        reflectivelySetField(kafkaProducerProperties, "retryBackoffMs", Duration.ofMillis(randomTestLong));
        assertThat(kafkaProducerProperties.getRetryBackoffMs(), equalTo(randomTestLong));
    }

    private void reflectivelySetField(final KafkaProducerProperties properties, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = KafkaProducerProperties.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(properties, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
