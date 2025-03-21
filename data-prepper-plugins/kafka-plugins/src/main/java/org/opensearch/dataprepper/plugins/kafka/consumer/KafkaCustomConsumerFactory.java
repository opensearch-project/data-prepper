/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfigAdapter;
import org.opensearch.dataprepper.plugins.kafka.common.PlaintextKafkaDataConfig;
import org.opensearch.dataprepper.plugins.kafka.common.aws.AwsContext;
import org.opensearch.dataprepper.plugins.kafka.common.key.KeyFactory;
import org.opensearch.dataprepper.plugins.kafka.common.serialization.SerializationFactory;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.ClientDNSLookupType;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicConsumerMetrics;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

public class KafkaCustomConsumerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCustomConsumerFactory.class);

    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final SerializationFactory serializationFactory;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private String schemaType = MessageFormat.PLAINTEXT.toString();

    public KafkaCustomConsumerFactory(SerializationFactory serializationFactory, AwsCredentialsSupplier awsCredentialsSupplier) {
        this.serializationFactory = serializationFactory;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    public List<KafkaCustomConsumer> createConsumersForTopic(final KafkaConsumerConfig kafkaConsumerConfig, final TopicConsumerConfig topic,
                                                             final Buffer<Record<Event>> buffer, final PluginMetrics pluginMetrics,
                                                             final AcknowledgementSetManager acknowledgementSetManager,
                                                             final ByteDecoder byteDecoder,
                                                             final AtomicBoolean shutdownInProgress,
                                                             final boolean topicNameInMetrics,
                                                             final CircuitBreaker circuitBreaker) {
        Properties authProperties = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(authProperties, kafkaConsumerConfig, LOG);
        KafkaTopicConsumerMetrics topicMetrics = new KafkaTopicConsumerMetrics(topic.getName(), pluginMetrics, topicNameInMetrics);
        Properties consumerProperties = getConsumerProperties(kafkaConsumerConfig, topic, authProperties);
        MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);

        final List<KafkaCustomConsumer> consumers = new ArrayList<>();

        AwsContext awsContext = new AwsContext(kafkaConsumerConfig, awsCredentialsSupplier);
        KeyFactory keyFactory = new KeyFactory(awsContext);

        PauseConsumePredicate pauseConsumePredicate = PauseConsumePredicate.circuitBreakingPredicate(circuitBreaker);

        try {
            final int numWorkers = topic.getWorkers();
            IntStream.range(0, numWorkers).forEach(index -> {
                KafkaDataConfig dataConfig = new KafkaDataConfigAdapter(keyFactory, topic);
                Deserializer<Object> keyDeserializer = (Deserializer<Object>) serializationFactory.getDeserializer(PlaintextKafkaDataConfig.plaintextDataConfig(dataConfig));
                Deserializer<Object> valueDeserializer = null;
                if(schema == MessageFormat.PLAINTEXT) {
                    valueDeserializer = KafkaSecurityConfigurer.getGlueSerializer(kafkaConsumerConfig, awsContext);
                }
                if(valueDeserializer == null) {
                    valueDeserializer = (Deserializer<Object>) serializationFactory.getDeserializer(dataConfig);
                }
                final KafkaConsumer kafkaConsumer = new KafkaConsumer<>(consumerProperties, keyDeserializer, valueDeserializer);

                consumers.add(new KafkaCustomConsumer(kafkaConsumer, shutdownInProgress, buffer, kafkaConsumerConfig, topic,
                    schemaType, acknowledgementSetManager, byteDecoder, topicMetrics, pauseConsumePredicate));

            });
        } catch (Exception e) {
            if (e instanceof BrokerNotAvailableException || e instanceof TimeoutException) {
                LOG.error("The Kafka broker is not available.");
            } else {
                LOG.error("Failed to setup the Kafka Source Plugin.", e);
            }
            throw new RuntimeException(e);
        }

        return consumers;
    }

    private Properties getConsumerProperties(final KafkaConsumerConfig sourceConfig, final TopicConsumerConfig topicConfig, final Properties authProperties) {
        Properties properties = (Properties)authProperties.clone();
        if (StringUtils.isNotEmpty(sourceConfig.getClientDnsLookup())) {
            ClientDNSLookupType dnsLookupType = ClientDNSLookupType.getDnsLookupType(sourceConfig.getClientDnsLookup());
            switch (dnsLookupType) {
                case USE_ALL_DNS_IPS:
                    properties.put("client.dns.lookup", ClientDNSLookupType.USE_ALL_DNS_IPS.toString());
                    break;
                case CANONICAL_BOOTSTRAP:
                    properties.put("client.dns.lookup", ClientDNSLookupType.CANONICAL_BOOTSTRAP.toString());
                    break;
                case DEFAULT:
                    properties.put("client.dns.lookup", ClientDNSLookupType.DEFAULT.toString());
                    break;
            }
        }
        setConsumerTopicProperties(properties, topicConfig, topicConfig.getGroupId());
        setSchemaRegistryProperties(sourceConfig, properties, topicConfig);
        LOG.debug(SENSITIVE, "Starting consumer with the properties : {}", properties);
        return properties;
    }


    public static void setConsumerTopicProperties(final Properties properties, final TopicConsumerConfig topicConfig,
                                                  final String groupId) {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        if (Objects.nonNull(topicConfig.getClientId())) {
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, topicConfig.getClientId());
        }
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (int)topicConfig.getMaxPartitionFetchBytes());
        properties.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, ((Long)topicConfig.getRetryBackoff().toMillis()).intValue());
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, ((Long)topicConfig.getReconnectBackoff().toMillis()).intValue());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            topicConfig.getAutoCommit());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
            ((Long)topicConfig.getCommitInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            topicConfig.getAutoOffsetReset());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
            topicConfig.getConsumerMaxPollRecords());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
            ((Long)topicConfig.getMaxPollInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ((Long)topicConfig.getSessionTimeOut().toMillis()).intValue());
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ((Long)topicConfig.getHeartBeatInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int)topicConfig.getFetchMaxBytes());
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, topicConfig.getFetchMaxWait());
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, (int)topicConfig.getFetchMinBytes());
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
    }

    private void setSchemaRegistryProperties(final KafkaConsumerConfig kafkaConsumerConfig, final Properties properties, final TopicConfig topicConfig) {
        SchemaConfig schemaConfig = kafkaConsumerConfig.getSchemaConfig();
        if (Objects.isNull(schemaConfig)) {
            setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(properties, topicConfig);
            return;
        }

        if (schemaConfig.getType() == SchemaRegistryType.AWS_GLUE) {
            return;
        } else if (schemaConfig.getType() == SchemaRegistryType.CONFLUENT) {
            setupConfluentSchemaRegistry(schemaConfig, kafkaConsumerConfig, properties, topicConfig);
        }

    }

    private void setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(Properties properties, final TopicConfig topicConfig) {
        MessageFormat dataFormat = topicConfig.getSerdeFormat();
        schemaType = dataFormat.toString();
    }

    private void setPropertiesForSchemaRegistryConnectivity(final KafkaConsumerConfig kafkaConsumerConfig, final Properties properties) {
        AuthConfig authConfig = kafkaConsumerConfig.getAuthConfig();
        String schemaRegistryApiKey = kafkaConsumerConfig.getSchemaConfig().getSchemaRegistryApiKey();
        String schemaRegistryApiSecret = kafkaConsumerConfig.getSchemaConfig().getSchemaRegistryApiSecret();
        //with plaintext authentication for schema registry
        if ("USER_INFO".equalsIgnoreCase(kafkaConsumerConfig.getSchemaConfig().getBasicAuthCredentialsSource())
            && authConfig.getSaslAuthConfig().getPlainTextAuthConfig() != null) {
            String schemaBasicAuthUserInfo = schemaRegistryApiKey.concat(":").concat(schemaRegistryApiSecret);
            properties.put("schema.registry.basic.auth.user.info", schemaBasicAuthUserInfo);
            properties.put("basic.auth.credentials.source", "USER_INFO");
        }

        if (authConfig != null && authConfig.getSaslAuthConfig() != null) {
            PlainTextAuthConfig plainTextAuthConfig = authConfig.getSaslAuthConfig().getPlainTextAuthConfig();
            OAuthConfig oAuthConfig = authConfig.getSaslAuthConfig().getOAuthConfig();
            if (oAuthConfig != null) {
                properties.put("sasl.mechanism", oAuthConfig.getOauthSaslMechanism());
                properties.put("security.protocol", oAuthConfig.getOauthSecurityProtocol());
            }
        }
    }

    private MessageFormat determineSchemaMessageFormat() {

        // TODO: ???
        return null;
    }

    private void setPropertiesForSchemaType(final KafkaConsumerConfig kafkaConsumerConfig, final Properties properties, final TopicConfig topic) {
        Map prop = properties;
        Map<String, String> propertyMap = (Map<String, String>) prop;
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl(kafkaConsumerConfig));
        properties.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, false);
        final CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(properties.getProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG),
            100, propertyMap);
        final SchemaConfig schemaConfig = kafkaConsumerConfig.getSchemaConfig();
        try {
            final String subject = topic.getName() + "-value";
            if (schemaConfig.getVersion() != null) {
                schemaType = schemaRegistryClient.getSchemaMetadata(subject,
                        kafkaConsumerConfig.getSchemaConfig().getVersion()).getSchemaType();
            } else {
                schemaType = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchemaType();
            }
        } catch (IOException | RestClientException e) {
            LOG.error("Failed to connect to the schema registry...");
            throw new RuntimeException(e);
        }
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        } else {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        }
    }

    private String getSchemaRegistryUrl(final KafkaConsumerConfig kafkaConsumerConfig) {
        return kafkaConsumerConfig.getSchemaConfig().getRegistryURL();
    }

    private void setupConfluentSchemaRegistry(final SchemaConfig schemaConfig, final KafkaConsumerConfig kafkaConsumerConfig,
                                              final Properties properties, final TopicConfig topicConfig) {
        if (StringUtils.isNotEmpty(schemaConfig.getRegistryURL())) {
            setPropertiesForSchemaRegistryConnectivity(kafkaConsumerConfig, properties);
            setPropertiesForSchemaType(kafkaConsumerConfig, properties, topicConfig);
        } else {
            throw new RuntimeException("RegistryURL must be specified for confluent schema registry");
        }
    }
}
