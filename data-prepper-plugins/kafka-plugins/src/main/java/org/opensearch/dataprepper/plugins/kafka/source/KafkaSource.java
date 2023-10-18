/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import kafka.common.BrokerEndPointNotAvailableException;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import org.opensearch.dataprepper.plugins.kafka.util.ClientDNSLookupType;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicMetrics;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * The starting point of the Kafka-source plugin and the Kafka consumer
 * properties and kafka multithreaded consumers are being handled here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Event>> {
    private static final String NO_RESOLVABLE_URLS_ERROR_MESSAGE = "No resolvable bootstrap urls given in bootstrap.servers";
    private static final long RETRY_SLEEP_INTERVAL = 30000;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    private final KafkaSourceConfig sourceConfig;
    private final AtomicBoolean shutdownInProgress;
    private ExecutorService executorService;
    private final PluginMetrics pluginMetrics;
    private KafkaCustomConsumer consumer;
    private KafkaConsumer kafkaConsumer;
    private final String pipelineName;
    private String consumerGroupID;
    private String schemaType = MessageFormat.PLAINTEXT.toString();
    private final AcknowledgementSetManager acknowledgementSetManager;
    private static CachedSchemaRegistryClient schemaRegistryClient;
    private GlueSchemaRegistryKafkaDeserializer glueDeserializer;
    private StringDeserializer stringDeserializer;
    private final List<ExecutorService> allTopicExecutorServices;
    private final List<KafkaCustomConsumer> allTopicConsumers;

    @DataPrepperPluginConstructor
    public KafkaSource(final KafkaSourceConfig sourceConfig,
                       final PluginMetrics pluginMetrics,
                       final AcknowledgementSetManager acknowledgementSetManager,
                       final PipelineDescription pipelineDescription,
                       final KafkaClusterConfigSupplier kafkaClusterConfigSupplier) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.stringDeserializer = new StringDeserializer();
        this.shutdownInProgress = new AtomicBoolean(false);
        this.allTopicExecutorServices = new ArrayList<>();
        this.allTopicConsumers = new ArrayList<>();
        this.updateConfig(kafkaClusterConfigSupplier);
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        Properties authProperties = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(authProperties, sourceConfig, LOG);
        sourceConfig.getTopics().forEach(topic -> {
            consumerGroupID = topic.getGroupId();
            KafkaTopicMetrics topicMetrics = new KafkaTopicMetrics(topic.getName(), pluginMetrics);
            Properties consumerProperties = getConsumerProperties(topic, authProperties);
            MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);
            try {
                int numWorkers = topic.getWorkers();
                executorService = Executors.newFixedThreadPool(numWorkers);
                allTopicExecutorServices.add(executorService);

                IntStream.range(0, numWorkers).forEach(index -> {
                    while (true) {
                        try {
                            kafkaConsumer = createKafkaConsumer(schema, consumerProperties);
                            break;
                        } catch (ConfigException ce) {
                            if (ce.getMessage().contains(NO_RESOLVABLE_URLS_ERROR_MESSAGE)) {
                                LOG.warn("Exception while creating Kafka consumer: ", ce);
                                LOG.warn("Bootstrap URL could not be resolved. Retrying in {} ms...", RETRY_SLEEP_INTERVAL);
                                try {
                                    sleep(RETRY_SLEEP_INTERVAL);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(ie);
                                }
                            } else {
                                throw ce;
                            }
                        }

                    }
                    consumer = new KafkaCustomConsumer(kafkaConsumer, shutdownInProgress, buffer, sourceConfig, topic, schemaType, acknowledgementSetManager, topicMetrics);
                    allTopicConsumers.add(consumer);

                    executorService.submit(consumer);
                });
            } catch (Exception e) {
                if (e instanceof BrokerNotAvailableException ||
                        e instanceof BrokerEndPointNotAvailableException || e instanceof TimeoutException) {
                    LOG.error("The kafka broker is not available...");
                } else {
                    LOG.error("Failed to setup the Kafka Source Plugin.", e);
                }
                throw new RuntimeException(e);
            }
            LOG.info("Started Kafka source for topic " + topic.getName());
        });
    }

    public KafkaConsumer<?, ?> createKafkaConsumer(final MessageFormat schema, final Properties consumerProperties) {
        switch (schema) {
            case JSON:
                return new KafkaConsumer<String, JsonNode>(consumerProperties);
            case AVRO:
                return new KafkaConsumer<String, GenericRecord>(consumerProperties);
            case PLAINTEXT:
            default:
                glueDeserializer = KafkaSecurityConfigurer.getGlueSerializer(sourceConfig);
                if (Objects.nonNull(glueDeserializer)) {
                    return new KafkaConsumer(consumerProperties, stringDeserializer, glueDeserializer);
                } else {
                    return new KafkaConsumer<String, String>(consumerProperties);
                }
        }
    }

    @Override
    public void stop() {
        shutdownInProgress.set(true);
        final long shutdownWaitTime = calculateLongestThreadWaitingTime();

        LOG.info("Shutting down {} Executor services", allTopicExecutorServices.size());
        allTopicExecutorServices.forEach(executor -> stopExecutor(executor, shutdownWaitTime));

        LOG.info("Closing {} consumers", allTopicConsumers.size());
        allTopicConsumers.forEach(consumer -> consumer.closeConsumer());

        LOG.info("Kafka source shutdown successfully...");
    }

    public void stopExecutor(final ExecutorService executorService, final long shutdownWaitTime) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(shutdownWaitTime, TimeUnit.SECONDS)) {
                LOG.info("Consumer threads are waiting for shutting down...");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            if (e.getCause() instanceof InterruptedException) {
                LOG.error("Interrupted during consumer shutdown, exiting uncleanly...", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private long calculateLongestThreadWaitingTime() {
        List<TopicConfig> topicsList = sourceConfig.getTopics();
        return topicsList.stream().
                map(
                        topics -> topics.getThreadWaitingTime().toSeconds()
                ).
                max(Comparator.comparingLong(time -> time)).
                orElse(1L);
    }

    KafkaConsumer getConsumer() {
        return kafkaConsumer;
    }

    private Properties getConsumerProperties(final TopicConfig topicConfig, final Properties authProperties) {
        Properties properties = (Properties) authProperties.clone();
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
        setConsumerTopicProperties(properties, topicConfig);
        setSchemaRegistryProperties(properties, topicConfig);
        LOG.info("Starting consumer with the properties : {}", properties);
        return properties;
    }

    private String getSchemaRegistryUrl() {
        return sourceConfig.getSchemaConfig().getRegistryURL();
    }

    private void setSchemaRegistryProperties(Properties properties, TopicConfig topicConfig) {
        SchemaConfig schemaConfig = sourceConfig.getSchemaConfig();
        if (Objects.isNull(schemaConfig)) {
            setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(properties, topicConfig);
            return;
        }

        if (schemaConfig.getType() == SchemaRegistryType.AWS_GLUE) {
            return;
        }

        /* else schema registry type is Confluent */
        if (StringUtils.isNotEmpty(schemaConfig.getRegistryURL())) {
            setPropertiesForSchemaRegistryConnectivity(properties);
            setPropertiesForSchemaType(properties, topicConfig);
        } else {
            throw new RuntimeException("RegistryURL must be specified for confluent schema registry");
        }
    }

    private void setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(Properties properties, final TopicConfig topicConfig) {
        MessageFormat dataFormat = topicConfig.getSerdeFormat();
        schemaType = dataFormat.toString();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        switch (dataFormat) {
            case JSON:
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
                break;
            default:
            case PLAINTEXT:
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class);
                break;
        }
    }

    private void setPropertiesForSchemaType(Properties properties, TopicConfig topic) {
        Map prop = properties;
        Map<String, String> propertyMap = (Map<String, String>) prop;
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("schema.registry.url", getSchemaRegistryUrl());
        properties.put("auto.register.schemas", false);
        schemaRegistryClient = new CachedSchemaRegistryClient(getSchemaRegistryUrl(),
                100, propertyMap);
        try {
            schemaType = schemaRegistryClient.getSchemaMetadata(topic.getName() + "-value",
                    sourceConfig.getSchemaConfig().getVersion()).getSchemaType();
        } catch (IOException | RestClientException e) {
            LOG.error("Failed to connect to the schema registry...");
            throw new RuntimeException(e);
        }
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
	    properties.put("json.value.type", "com.fasterxml.jackson.databind.JsonNode");
        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        } else {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
        }
    }

    private void setConsumerTopicProperties(Properties properties, TopicConfig topicConfig) {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (int) topicConfig.getMaxPartitionFetchBytes());
        properties.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, ((Long) topicConfig.getRetryBackoff().toMillis()).intValue());
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, ((Long) topicConfig.getReconnectBackoff().toMillis()).intValue());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                topicConfig.getAutoCommit());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                ((Long) topicConfig.getCommitInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                topicConfig.getAutoOffsetReset());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                topicConfig.getConsumerMaxPollRecords());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                ((Long) topicConfig.getMaxPollInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ((Long) topicConfig.getSessionTimeOut().toMillis()).intValue());
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ((Long) topicConfig.getHeartBeatInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int) topicConfig.getFetchMaxBytes());
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, topicConfig.getFetchMaxWait());
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, (int) topicConfig.getFetchMinBytes());
    }

    private void setPropertiesForSchemaRegistryConnectivity(Properties properties) {
        AuthConfig authConfig = sourceConfig.getAuthConfig();
        String schemaRegistryApiKey = sourceConfig.getSchemaConfig().getSchemaRegistryApiKey();
        String schemaRegistryApiSecret = sourceConfig.getSchemaConfig().getSchemaRegistryApiSecret();
        //with plaintext authentication for schema registry
        if ("USER_INFO".equalsIgnoreCase(sourceConfig.getSchemaConfig().getBasicAuthCredentialsSource())
                && authConfig.getSaslAuthConfig().getPlainTextAuthConfig() != null) {
            String schemaBasicAuthUserInfo = schemaRegistryApiKey.concat(":").concat(schemaRegistryApiSecret);
            properties.put("basic.auth.user.info", schemaBasicAuthUserInfo);
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

    protected void sleep(final long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private void updateConfig(final KafkaClusterConfigSupplier kafkaClusterConfigSupplier) {
        if (kafkaClusterConfigSupplier != null) {
            if (sourceConfig.getBootstrapServers() == null) {
                sourceConfig.setBootStrapServers(kafkaClusterConfigSupplier.getBootStrapServers());
            }
            if (sourceConfig.getAuthConfig() == null) {
                sourceConfig.setAuthConfig(kafkaClusterConfigSupplier.getAuthConfig());
            }
            if (sourceConfig.getAwsConfig() == null) {
                sourceConfig.setAwsConfig(kafkaClusterConfigSupplier.getAwsConfig());
            }
            if (sourceConfig.getEncryptionConfigRaw() == null) {
                sourceConfig.setEncryptionConfig(kafkaClusterConfigSupplier.getEncryptionConfig());
            }
        }
    }
}
