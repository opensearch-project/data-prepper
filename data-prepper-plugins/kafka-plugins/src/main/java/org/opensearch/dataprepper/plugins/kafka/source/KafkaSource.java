/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;

import org.opensearch.dataprepper.plugins.kafka.configuration.TopicsConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.MultithreadedConsumer;
import org.opensearch.dataprepper.plugins.kafka.util.AuthenticationType;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceJsonDeserializer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * The starting point of the Kafka-source plugin and the Kafka consumer
 * properties and kafka multithreaded consumers are being handled here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    private final KafkaSourceConfig sourceConfig;
    private ExecutorService executorService;
    private static final String KAFKA_WORKER_THREAD_PROCESSING_ERRORS = "kafkaWorkerThreadProcessingErrors";
    private final Counter kafkaWorkerThreadProcessingErrors;
    private final PluginMetrics pluginMetrics;
    private String consumerGroupID;
    private MultithreadedConsumer multithreadedConsumer;
    private int totalWorkers;

    @DataPrepperPluginConstructor
    public KafkaSource(final KafkaSourceConfig sourceConfig, final PluginMetrics pluginMetrics) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.kafkaWorkerThreadProcessingErrors = pluginMetrics.counter(KAFKA_WORKER_THREAD_PROCESSING_ERRORS);
    }

    @Override
    public void start(Buffer<Record<Object>> buffer) {
        sourceConfig.getTopics().forEach(topic -> {
            totalWorkers = 0;
            try {
                Properties consumerProperties = getConsumerProperties(topic);
                totalWorkers = topic.getWorkers();
                consumerGroupID = topic.getGroupId();
                executorService = Executors.newFixedThreadPool(totalWorkers);
                IntStream.range(0, totalWorkers).forEach(index -> {
                    String consumerId = consumerGroupID + " :: " + topic + " :: " + Integer.toString(index + 1);
                    multithreadedConsumer = new MultithreadedConsumer(consumerId,
                            consumerGroupID, consumerProperties, sourceConfig, buffer, pluginMetrics);
                    executorService.submit(multithreadedConsumer);
                });
            } catch (Exception e) {
                LOG.error("Failed to setup the Kafka Source Plugin.", e);
                throw new RuntimeException();
            }
        });
    }

    @Override
    public void stop() {
        LOG.info("Shutting down Consumers...");
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(
                    calculateLongestThreadWaitingTime(), TimeUnit.MILLISECONDS)) {
                LOG.info("Consumer threads are waiting for shutting down...");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            if (e.getCause() instanceof InterruptedException) {
                LOG.error("Interrupted during consumer shutdown, exiting uncleanly...");
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("Consumer shutdown successfully...");
    }

    private long calculateLongestThreadWaitingTime() {
        List<TopicsConfig> topicsList = sourceConfig.getTopics();
        return topicsList.stream().
                map(
                        topics -> topics.getThreadWaitingTime().toSeconds()
                ).
                max(Comparator.comparingLong(time -> time)).
                orElse(1L);
    }

    private Properties getConsumerProperties(TopicsConfig topicConfig) {
        String schemaType = "";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                topicConfig.getAutoCommitInterval().toSecondsPart());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                topicConfig.getAutoOffsetReset());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                topicConfig.getAutoCommit());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicConfig.getGroupId());
        schemaType = getSchemaType(sourceConfig.getSchemaConfig().getRegistryURL(), topicConfig.getName(), sourceConfig.getSchemaConfig().getVersion());
        if (schemaType.isEmpty()) {
            schemaType = MessageFormat.PLAINTEXT.toString();
        }
        setPropertiesForSchemaType(properties, schemaType);
        if (sourceConfig.getAuthType().equalsIgnoreCase(AuthenticationType.PLAINTEXT.toString())) {
            setPropertiesForAuth(properties);
        }
        return properties;
    }

    private void setPropertiesForSchemaType(Properties properties,final String schemaType) {
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceJsonDeserializer.class);
        } else if (schemaType.equalsIgnoreCase(MessageFormat.PLAINTEXT.toString())) {
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    KafkaAvroDeserializer.class);
            properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, sourceConfig.getSchemaConfig().getRegistryURL());
        }
    }

    private void setPropertiesForAuth(Properties properties) {
        String username = sourceConfig.getAuthConfig().getPlainTextAuthConfig().getUsername();
        String password = sourceConfig.getAuthConfig().getPlainTextAuthConfig().getPassword();
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        properties.put("security.protocol", "SASL_PLAINTEXT");
    }

    private static String getSchemaType(final String registryUrl, final String topicName, final int schemaVersion) {
        StringBuilder response = new StringBuilder();
        String schemaType = "";
        try {
            String urlPath = registryUrl + "subjects" + "/" + topicName + "-value/versions/" + schemaVersion;
            URL url = new URL(urlPath);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                while ((inputLine = reader.readLine()) != null) {
                    response.append(inputLine);
                }
                reader.close();
                ObjectMapper mapper = new ObjectMapper();
                Object json = mapper.readValue(response.toString(), Object.class);
                String indented = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
                JsonNode rootNode = mapper.readTree(indented);
                if (rootNode.has("schemaType")) {
                    schemaType = MessageFormat.JSON.toString();
                } else {
                    schemaType = MessageFormat.AVRO.toString();
                }
            } else {
                LOG.error("GET request failed while fetching the schema registry details : {}", responseCode);
            }
        } catch (IOException e) {
            LOG.error("An error while fetching the schema registry details : ", e);
        }
        return schemaType;
    }
}
