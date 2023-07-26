/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerProperties;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * * This is static property configurer for related information given in pipeline.yml
 */
public class SinkPropertyConfigurer {

    private static final Logger LOG = LoggerFactory.getLogger(SinkPropertyConfigurer.class);

    private static final String VALUE_SERIALIZER = "value.serializer";

    private static final String KEY_SERIALIZER = "key.serializer";

    private static final String SESSION_TIMEOUT_MS_CONFIG = "30000";

    private static final String REGISTRY_URL = "schema.registry.url";

    private static final String REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";

    private static final String CREDENTIALS_SOURCE = "basic.auth.credentials.source";

    public static final String BUFFER_MEMORY = "buffer.memory";

    public static final String COMPRESSION_TYPE = "compression.type";

    public static final String RETRIES = "retries";

    public static final String BATCH_SIZE = "batch.size";

    public static final String CLIENT_DNS_LOOKUP = "client.dns.lookup";

    public static final String CLIENT_ID = "client.id";

    public static final String CONNECTIONS_MAX_IDLE_MS = "connections.max.idle.ms";

    public static final String DELIVERY_TIMEOUT_MS = "delivery.timeout.ms";

    public static final String LINGER_MS = "linger.ms";

    public static final String MAX_BLOCK_MS = "max.block.ms";

    public static final String MAX_REQUEST_SIZE = "max.request.size";

    public static final String PARTITIONER_CLASS = "partitioner.class";

    public static final String PARTITIONER_IGNORE_KEYS = "partitioner.ignore.keys";

    public static final String RECEIVE_BUFFER_BYTES = "receive.buffer.bytes";

    public static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";

    public static final String SEND_BUFFER_BYTES = "send.buffer.bytes";

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS = "socket.connection.setup.timeout.max.ms";

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS = "socket.connection.setup.timeout.ms";

    public static final String ACKS = "acks";

    public static final String ENABLE_IDEMPOTENCE = "enable.idempotence";

    public static final String INTERCEPTOR_CLASSES = "interceptor.classes";

    public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";

    public static final String METADATA_MAX_AGE_MS = "metadata.max.age.ms";

    public static final String METADATA_MAX_IDLE_MS = "metadata.max.idle.ms";

    public static final String METRIC_REPORTERS = "metric.reporters";

    public static final String METRICS_NUM_SAMPLES = "metrics.num.samples";

    public static final String METRICS_RECORDING_LEVEL = "metrics.recording.level";

    public static final String METRICS_SAMPLE_WINDOW_MS = "metrics.sample.window.ms";

    public static final String PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE = "partitioner.adaptive.partitioning.enable";

    public static final String PARTITIONER_AVAILABILITY_TIMEOUT_MS = "partitioner.availability.timeout.ms";

    public static final String RECONNECT_BACKOFF_MAX_MS = "reconnect.backoff.max.ms";

    public static final String RECONNECT_BACKOFF_MS = "reconnect.backoff.ms";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";

    public static Properties getProducerProperties(final KafkaSinkConfig kafkaSinkConfig) {
        final Properties properties = new Properties();

        setCommonServerProperties(properties, kafkaSinkConfig);

        setPropertiesForSerializer(properties, kafkaSinkConfig.getSerdeFormat());

        if (kafkaSinkConfig.getSchemaConfig() != null) {
            setSchemaProps(kafkaSinkConfig, properties);
        }
        if (kafkaSinkConfig.getKafkaProducerProperties() != null) {
            setPropertiesProviderByKafkaProducer(kafkaSinkConfig.getKafkaProducerProperties(), properties);
        }

        if (kafkaSinkConfig.getAuthConfig().getSaslAuthConfig().getPlainTextAuthConfig() != null) {
            AuthenticationPropertyConfigurer.setSaslPlainTextProperties(kafkaSinkConfig.getAuthConfig()
                    .getSaslAuthConfig().getPlainTextAuthConfig(), properties);
        } else if (kafkaSinkConfig.getAuthConfig().getSaslAuthConfig().getOAuthConfig() != null) {
            AuthenticationPropertyConfigurer.setOauthProperties(kafkaSinkConfig.getAuthConfig().getSaslAuthConfig()
                    .getOAuthConfig(), properties);
        } else if (kafkaSinkConfig.getAuthConfig().getSaslAuthConfig().getPlain() != null) {
            AuthenticationPropertyConfigurer.setSaslPlainProperties(kafkaSinkConfig.getAuthConfig().getSaslAuthConfig().getPlain(), properties);
        }

        return properties;
    }

    private static void setCommonServerProperties(final Properties properties, final KafkaSinkConfig kafkaSinkConfig) {
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkConfig.getBootStrapServers());
        properties.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS_CONFIG);
    }

    private static void setPropertiesForSerializer(final Properties properties, final String serdeFormat) {
        properties.put(KEY_SERIALIZER, StringSerializer.class.getName());
        if (serdeFormat.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(VALUE_SERIALIZER, JsonSerializer.class.getName());
        } else if (serdeFormat.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(VALUE_SERIALIZER, KafkaAvroSerializer.class.getName());
        } else {
            properties.put(VALUE_SERIALIZER, StringSerializer.class.getName());
        }
    }

    private static void validateForRegistryURL(final KafkaSinkConfig kafkaSinkConfig) {
        String serdeFormat = kafkaSinkConfig.getSerdeFormat();
        if (serdeFormat.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            if (kafkaSinkConfig.getSchemaConfig() == null || kafkaSinkConfig.getSchemaConfig().getRegistryURL() == null ||
                    kafkaSinkConfig.getSchemaConfig().getRegistryURL().isBlank() || kafkaSinkConfig.getSchemaConfig().getRegistryURL().isEmpty()) {
                throw new RuntimeException("Schema registry is mandatory when serde type is avro");
            }
        }
        if (serdeFormat.equalsIgnoreCase(MessageFormat.PLAINTEXT.toString())) {
            if (kafkaSinkConfig.getSchemaConfig() != null &&
                    kafkaSinkConfig.getSchemaConfig().getRegistryURL() != null) {
                throw new RuntimeException("Schema registry is not required for type plaintext");
            }
        }
    }

    public static void setSchemaProps(final KafkaSinkConfig kafkaSinkConfig, final Properties properties) {
        validateForRegistryURL(kafkaSinkConfig);
        SchemaConfig schemaConfig = kafkaSinkConfig.getSchemaConfig();
        final String registryURL = schemaConfig != null ? schemaConfig.getRegistryURL() : null;
        if (registryURL != null && !registryURL.isEmpty()) {
            properties.put(REGISTRY_URL, registryURL);
        }
        if (!ObjectUtils.isEmpty(schemaConfig.getBasicAuthCredentialsSource())) {
            properties.put(CREDENTIALS_SOURCE, schemaConfig.getBasicAuthCredentialsSource());
        }
        if (!ObjectUtils.isEmpty(schemaConfig.getSchemaRegistryApiKey()) && !(ObjectUtils.isEmpty(schemaConfig.getSchemaRegistryApiSecret()))) {
            final String apiKey = schemaConfig.getSchemaRegistryApiKey();
            final String apiSecret = schemaConfig.getSchemaRegistryApiSecret();
            properties.put(REGISTRY_BASIC_AUTH_USER_INFO, apiKey + ":" + apiSecret);
        }
    }

    private static void setPropertiesProviderByKafkaProducer(final KafkaProducerProperties producerProperties, final
    Properties properties) {

        if (producerProperties.getBufferMemory() != null) {
            properties.put(BUFFER_MEMORY, ByteCount.parse(producerProperties.getBufferMemory()).getBytes());
        }
        if (producerProperties.getCompressionType() != null) {
            properties.put(COMPRESSION_TYPE, producerProperties.getCompressionType());
        }
        properties.put(RETRIES, producerProperties.getRetries());

        if (producerProperties.getBatchSize() > 0) {
            properties.put(BATCH_SIZE, producerProperties.getBatchSize());
        }
        if (producerProperties.getClientDnsLookup() != null) {
            properties.put(CLIENT_DNS_LOOKUP, producerProperties.getClientDnsLookup());
        }
        if (producerProperties.getClientId() != null) {
            properties.put(CLIENT_ID, producerProperties.getClientId());
        }
        if (producerProperties.getConnectionsMaxIdleMs() > 0) {
            properties.put(CONNECTIONS_MAX_IDLE_MS, producerProperties.getConnectionsMaxIdleMs());
        }
        if (producerProperties.getDeliveryTimeoutMs() > 0) {
            properties.put(DELIVERY_TIMEOUT_MS, producerProperties.getDeliveryTimeoutMs().intValue());
        }
        if (producerProperties.getLingerMs() > 0) {
            properties.put(LINGER_MS, (producerProperties.getLingerMs()));
        }
        if (producerProperties.getMaxBlockMs() > 0) {
            properties.put(MAX_BLOCK_MS, producerProperties.getMaxBlockMs());
        }
        if (producerProperties.getMaxRequestSize() > 0) {
            properties.put(MAX_REQUEST_SIZE, producerProperties.getMaxRequestSize());
        }
        if (producerProperties.getPartitionerClass() != null) {
            properties.put(PARTITIONER_CLASS, producerProperties.getPartitionerClass().getName());
        }
        if (producerProperties.getPartitionerIgnoreKeys() != null) {
            properties.put(PARTITIONER_IGNORE_KEYS, producerProperties.getPartitionerIgnoreKeys());
        }
        if (producerProperties.getReceiveBufferBytes() != null) {
            final Long receiveBufferBytes = ByteCount.parse(producerProperties.getReceiveBufferBytes()).getBytes();
            properties.put(RECEIVE_BUFFER_BYTES, receiveBufferBytes.intValue());
        }
        if (producerProperties.getRequestTimeoutMs() > 0) {
            properties.put(REQUEST_TIMEOUT_MS, producerProperties.getRequestTimeoutMs().intValue());
        }
        if (producerProperties.getSendBufferBytes() != null) {
            final Long sendBufferBytes = ByteCount.parse(producerProperties.getSendBufferBytes()).getBytes();
            properties.put(SEND_BUFFER_BYTES, sendBufferBytes.intValue());
        }
        if (producerProperties.getSocketConnectionSetupMaxTimeout() > 0) {
            properties.put(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS, producerProperties.getSocketConnectionSetupMaxTimeout());
        }
        if (producerProperties.getSocketConnectionSetupTimeout() > 0) {
            properties.put(SOCKET_CONNECTION_SETUP_TIMEOUT_MS, producerProperties.getSocketConnectionSetupTimeout());
        }
        if (producerProperties.getAcks() != null) {
            properties.put(ACKS, producerProperties.getAcks());
        }
        if (producerProperties.getEnableIdempotence() != null) {
            properties.put(ENABLE_IDEMPOTENCE, producerProperties.getEnableIdempotence());
        }


        List<String> interceptorClasses = producerProperties.getInterceptorClasses();
        if (interceptorClasses != null && !interceptorClasses.isEmpty()) {
            properties.put(INTERCEPTOR_CLASSES, String.join(",", interceptorClasses));
        }

        if (producerProperties.getMaxInFlightRequestsPerConnection() > 0) {
            properties.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, producerProperties.getMaxInFlightRequestsPerConnection());
        }

        if (producerProperties.getMetadataMaxAgeMs() > 0) {
            properties.put(METADATA_MAX_AGE_MS, producerProperties.getMetadataMaxAgeMs());
        }

        if (producerProperties.getMetadataMaxIdleMs() > 0) {
            properties.put(METADATA_MAX_IDLE_MS, producerProperties.getMetadataMaxIdleMs());
        }


        List<String> metricReporters = producerProperties.getMetricReporters();
        if (metricReporters != null && !metricReporters.isEmpty()) {
            properties.put(METRIC_REPORTERS, String.join(",", metricReporters));
        }

        if (producerProperties.getMetricsNumSamples() > 0) {
            properties.put(METRICS_NUM_SAMPLES, producerProperties.getMetricsNumSamples());
        }

        if (producerProperties.getMetricsRecordingLevel() != null) {
            properties.put(METRICS_RECORDING_LEVEL, producerProperties.getMetricsRecordingLevel());
        }

        if (producerProperties.getMetricsSampleWindowMs() > 0) {
            properties.put(METRICS_SAMPLE_WINDOW_MS, producerProperties.getMetricsSampleWindowMs());
        }

        properties.put(PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE, producerProperties.isPartitionerAdaptivePartitioningEnable());

        if (producerProperties.getPartitionerAvailabilityTimeoutMs() > 0) {
            properties.put(PARTITIONER_AVAILABILITY_TIMEOUT_MS, producerProperties.getPartitionerAvailabilityTimeoutMs());
        }

        if (producerProperties.getReconnectBackoffMaxMs() > 0) {
            properties.put(RECONNECT_BACKOFF_MAX_MS, producerProperties.getReconnectBackoffMaxMs());
        }

        if (producerProperties.getReconnectBackoffMs() > 0) {
            properties.put(RECONNECT_BACKOFF_MS, producerProperties.getReconnectBackoffMs());
        }

        if (producerProperties.getRetryBackoffMs() > 0) {
            properties.put(RETRY_BACKOFF_MS, producerProperties.getRetryBackoffMs());
        }


        LOG.info("Producer properties");
        properties.entrySet().forEach(prop -> {
            LOG.info("property " + prop.getKey() + " value" + prop.getValue());
        });

        LOG.info("Producer properties ends");
    }

}
