/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import org.apache.avro.generic.GenericRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsIamAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaSourceCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceJsonDeserializer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import com.amazonaws.services.kafka.model.GetBootstrapBrokersRequest;
import com.amazonaws.services.kafka.AWSKafka;
import com.amazonaws.services.kafka.AWSKafkaClientBuilder;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersResult;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.kafka.model.InternalServerErrorException;
import com.amazonaws.services.kafka.model.ConflictException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Comparator;
import java.util.Objects;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import java.util.concurrent.atomic.AtomicBoolean;
/**
 * The starting point of the Kafka-source plugin and the Kafka consumer
 * properties and kafka multithreaded consumers are being handled here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Event>> {
    private static final String KAFKA_WORKER_THREAD_PROCESSING_ERRORS = "kafkaWorkerThreadProcessingErrors";
    private static final int MAX_KAFKA_CLIENT_RETRIES = 10;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    private final KafkaSourceConfig sourceConfig;
    private AtomicBoolean shutdownInProgress;
    private ExecutorService executorService;
    private final Counter kafkaWorkerThreadProcessingErrors;
    private final PluginMetrics pluginMetrics;
    private KafkaSourceCustomConsumer consumer;
    private String pipelineName;
    private String schemaType = MessageFormat.PLAINTEXT.toString();
    private static final String SCHEMA_TYPE= "schemaType";
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final EncryptionType encryptionType;

    @DataPrepperPluginConstructor
    public KafkaSource(final KafkaSourceConfig sourceConfig,
                       final PluginMetrics pluginMetrics,
                       final AcknowledgementSetManager acknowledgementSetManager,
                       final PipelineDescription pipelineDescription) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.kafkaWorkerThreadProcessingErrors = pluginMetrics.counter(KAFKA_WORKER_THREAD_PROCESSING_ERRORS);
        shutdownInProgress = new AtomicBoolean(false);
        this.encryptionType = sourceConfig.getEncryptionType();
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        sourceConfig.getTopics().forEach(topic -> {
            Properties consumerProperties = getConsumerProperties(topic);
            MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);

            try {
                int numWorkers = topic.getWorkers();
                executorService = Executors.newFixedThreadPool(numWorkers);
                IntStream.range(0, numWorkers + 1).forEach(index -> {
                    KafkaConsumer kafkaConsumer;
                    switch (schema) {
                        case JSON:
                            kafkaConsumer = new KafkaConsumer<String, JsonNode>(consumerProperties);
                            break;
                        case AVRO:
                            kafkaConsumer = new KafkaConsumer<String, GenericRecord>(consumerProperties);
                            break;
                        case PLAINTEXT:
                        default:
                            kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
                            break;
                    }
                    consumer = new KafkaSourceCustomConsumer(kafkaConsumer, shutdownInProgress, buffer, sourceConfig, topic, schemaType, acknowledgementSetManager, pluginMetrics);

                    executorService.submit(consumer);
                });
            } catch (Exception e) {
                LOG.error("Failed to setup the Kafka Source Plugin.", e);
                throw new RuntimeException();
            }
            LOG.info("Started Kafka source for topic " + topic.getName());
        });
    }

    @Override
    public void stop() {
        LOG.info("Shutting down Consumers...");
        shutdownInProgress.set(true);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(
                    calculateLongestThreadWaitingTime(), TimeUnit.SECONDS)) {
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
        List<TopicConfig> topicsList = sourceConfig.getTopics();
        return topicsList.stream().
                map(
                        topics -> topics.getThreadWaitingTime().toSeconds()
                ).
                max(Comparator.comparingLong(time -> time)).
                orElse(1L);
    }

    public String getBootStrapServersForMsk(final AwsIamAuthConfig awsIamAuthConfig, final AwsConfig awsConfig) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        AWSCredentialsProvider credentialProvider;
        if (awsIamAuthConfig == AwsIamAuthConfig.DEFAULT) {
            credentialProvider = new DefaultAWSCredentialsProviderChain();
        } else if (awsIamAuthConfig == AwsIamAuthConfig.ROLE) {
            String sessionName = "data-prepper-kafka-session"+UUID.randomUUID();
            credentialProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(awsConfig.getStsRoleArn(), sessionName).build();
        } else {
            throw new RuntimeException("Unknown AWS IAM auth mode");
        }
        AWSKafka kafkaClient = AWSKafkaClientBuilder.standard()
                .withClientConfiguration(clientConfiguration)
                .withCredentials(credentialProvider)
                .withRegion(awsConfig.getRegion())
                .build();
        GetBootstrapBrokersRequest request = new GetBootstrapBrokersRequest();
        String clusterArn = awsConfig.getAwsMskConfig().getArn();
        request.setClusterArn(clusterArn);
        int numRetries = 0;
        boolean retryable;
        GetBootstrapBrokersResult result = null;
        do {
            retryable = false;
            try {
                result = kafkaClient.getBootstrapBrokers(request);
            } catch (InternalServerErrorException | ConflictException e) {
                retryable = true;
            } catch (Exception e) {
                break;
            }
        } while (retryable && numRetries++ < MAX_KAFKA_CLIENT_RETRIES);
        if (Objects.isNull(result)) {
            LOG.info("Failed to get bootstrap server information from MSK, using user configured bootstrap servers");
            return sourceConfig.getBootStrapServers();
        }
        // TODO return this based on the broker_connection_mode
        return result.getBootstrapBrokerStringSaslIam();
    }

    private Properties getConsumerProperties(final TopicConfig topicConfig) {
        Properties properties = new Properties();
        AwsIamAuthConfig awsIamAuthConfig = null;
        AwsConfig awsConfig = sourceConfig.getAwsConfig();
        if (sourceConfig.getAuthConfig() != null) {
            AuthConfig.SaslAuthConfig saslAuthConfig = sourceConfig.getAuthConfig().getSaslAuthConfig();
            if (saslAuthConfig != null) {
                awsIamAuthConfig = saslAuthConfig.getAwsIamAuthConfig();
                if (awsIamAuthConfig != null) {
                    if (encryptionType == EncryptionType.PLAINTEXT) {
                        throw new RuntimeException("Encryption Config must be SSL to use IAM authentication mechanism");
                    }
                    setAwsIamAuthProperties(properties, awsIamAuthConfig, awsConfig);
                } else if (saslAuthConfig.getOAuthConfig() != null) {
                } else if (saslAuthConfig.getPlainTextAuthConfig() != null) {
                    setPlainTextAuthProperties(properties);
                } else {
                    throw new RuntimeException("No SASL auth config specified");
                }
            }
        }
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                topicConfig.getAutoCommitInterval().toSecondsPart());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                topicConfig.getAutoOffsetReset());
        String bootstrapServers = sourceConfig.getBootStrapServers();
        if (Objects.nonNull(awsIamAuthConfig)) {
            bootstrapServers = getBootStrapServersForMsk(awsIamAuthConfig, awsConfig);
        }
        if (Objects.isNull(bootstrapServers) || bootstrapServers.isEmpty()) {
            throw new RuntimeException("Bootstrap servers are not specified");
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                topicConfig.getAutoCommit());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                topicConfig.getConsumerMaxPollRecords());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicConfig.getGroupId());
        if (sourceConfig.getSchemaConfig() != null) {
            schemaType = getSchemaType(sourceConfig.getSchemaConfig().getRegistryURL(), topicConfig.getName(), sourceConfig.getSchemaConfig().getVersion());
    }
        if (schemaType.isEmpty()) {
            schemaType = MessageFormat.PLAINTEXT.toString();
        }
        setPropertiesForSchemaType(properties, schemaType);
        LOG.info("Starting consumer with the properties : {}", properties);
        return properties;
    }

    private void setPropertiesForSchemaType(Properties properties, final String schemaType) {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceJsonDeserializer.class);
        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    KafkaAvroDeserializer.class);
            if (validateURL(getSchemaRegistryUrl())) {
                properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
            } else {
                throw new RuntimeException("Invalid Schema Registry URI");
            }
        } else {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
        }
    }

    private static boolean validateURL(String url) {
        try {
            URI uri = new URI(url);
            if (uri.getScheme() == null || uri.getHost() == null) {
                return false;
            }
            return true;
        } catch (URISyntaxException ex) {
            LOG.error("Invalid Schema Registry URI: ", ex);
            return false;
        }
    }

    private String getSchemaRegistryUrl() {
        return sourceConfig.getSchemaConfig().getRegistryURL();
    }

    private void setAwsIamAuthProperties(Properties properties, final AwsIamAuthConfig awsIamAuthConfig, final AwsConfig awsConfig) {
        if (awsConfig == null) {
            throw new RuntimeException("AWS Config is not specified");
        }
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "AWS_MSK_IAM");
        properties.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        if (awsIamAuthConfig == AwsIamAuthConfig.ROLE) {
            properties.put("sasl.jaas.config",
                "software.amazon.msk.auth.iam.IAMLoginModule required " +
                "awsRoleArn=\"" + awsConfig.getStsRoleArn()+
                "\" awsStsRegion=\""+ awsConfig.getRegion()+"\";");
        } else if (awsIamAuthConfig == AwsIamAuthConfig.DEFAULT) {
            properties.put("sasl.jaas.config",
                "software.amazon.msk.auth.iam.IAMLoginModule required;");
        }
    }

    private void setPlainTextAuthProperties(Properties properties) {

        String username = sourceConfig.getAuthConfig().getSaslAuthConfig().getPlainTextAuthConfig().getUsername();
        String password = sourceConfig.getAuthConfig().getSaslAuthConfig().getPlainTextAuthConfig().getPassword();
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        properties.put("security.protocol", "SASL_PLAINTEXT");
    }

    private static String getSchemaType(final String registryUrl, final String topicName, final int schemaVersion) {
        StringBuilder response = new StringBuilder();
        String schemaType = MessageFormat.PLAINTEXT.toString();
        try {
            String urlPath = registryUrl + "subjects/" + topicName + "-value/versions/" + schemaVersion;
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
                // If the entry exists but schema type doesn't exist then
                // the schemaType defaults to AVRO
                if (rootNode.has(SCHEMA_TYPE)) {
                    JsonNode node = rootNode.findValue(SCHEMA_TYPE);
                    schemaType = node.textValue();
                } else {
                    schemaType = MessageFormat.AVRO.toString();
                }
            } else {
                InputStream errorStream = connection.getErrorStream();
                String errorMessage = readErrorMessage(errorStream);
                // Plaintext is not a valid schematype in schema registry
                // So, if it doesn't exist in schema regitry, default
                // the schemaType to PLAINTEXT
                LOG.error("GET request failed while fetching the schema registry. Defaulting to schema type PLAINTEXT");
                return MessageFormat.PLAINTEXT.toString();

            }
        } catch (IOException e) {
            LOG.error("An error while fetching the schema registry details : ", e);
            throw new RuntimeException();
        }
        return schemaType;
    }

    private static String readErrorMessage(InputStream errorStream) throws IOException {
        if (errorStream == null) {
            return null;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
        StringBuilder errorMessage = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            errorMessage.append(line);
        }
        reader.close();
        errorStream.close();
        return errorMessage.toString();
    }
}
