/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import kafka.common.BrokerEndPointNotAvailableException;
import org.apache.avro.generic.GenericRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.micrometer.core.instrument.Counter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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

import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsIamAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaSourceCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceJsonDeserializer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersRequest;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersResponse;
import software.amazon.awssdk.services.kafka.model.InternalServerErrorException;
import software.amazon.awssdk.services.kafka.model.ConflictException;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.regions.Region;

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
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.Objects;
import java.util.Comparator;
import java.util.Properties;
import java.util.Optional;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private String consumerGroupID;
    private String schemaType = MessageFormat.PLAINTEXT.toString();
    private static final String SCHEMA_TYPE = "schemaType";
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final EncryptionType encryptionType;
    private static CachedSchemaRegistryClient schemaRegistryClient;

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
            consumerGroupID = getGroupId(topic.getName());
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
                if (e instanceof BrokerNotAvailableException ||
                        e instanceof BrokerEndPointNotAvailableException || e instanceof TimeoutException) {
                    LOG.error("The kafka broker is not available...");
                } else {
                    LOG.error("Failed to setup the Kafka Source Plugin.", e);
                }
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

    private String getGroupId(String name) {
        return pipelineName + "::" + name;
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
        AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        if (awsIamAuthConfig == AwsIamAuthConfig.ROLE) {
            String sessionName = "data-prepper-kafka-session" + UUID.randomUUID();
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(awsConfig.getRegion()))
                    .credentialsProvider(credentialsProvider)
                    .build();
            credentialsProvider = StsAssumeRoleCredentialsProvider
                    .builder()
                    .stsClient(stsClient)
                    .refreshRequest(
                            AssumeRoleRequest
                                    .builder()
                                    .roleArn(awsConfig.getStsRoleArn())
                                    .roleSessionName(sessionName)
                                    .build()
                    ).build();
        } else {
            throw new RuntimeException("Unknown AWS IAM auth mode");
        }
        final AwsConfig.AwsMskConfig awsMskConfig = awsConfig.getAwsMskConfig();
        KafkaClient kafkaClient = KafkaClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(awsConfig.getRegion()))
                .build();
        final GetBootstrapBrokersRequest request =
                GetBootstrapBrokersRequest
                        .builder()
                        .clusterArn(awsMskConfig.getArn())
                        .build();

        int numRetries = 0;
        boolean retryable;
        GetBootstrapBrokersResponse result = null;
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
        switch (awsMskConfig.getBrokerConnectionType()) {
            case PUBLIC:
                return result.bootstrapBrokerStringPublicSaslIam();
            case MULTI_VPC:
                return result.bootstrapBrokerStringVpcConnectivitySaslIam();
            default:
            case SINGLE_VPC:
                return result.bootstrapBrokerStringSaslIam();
        }
    }

    private Properties getConsumerProperties(final TopicConfig topicConfig) {
        Properties properties = new Properties();
        AwsIamAuthConfig awsIamAuthConfig = null;
        AwsConfig awsConfig = sourceConfig.getAwsConfig();
        if (sourceConfig.getAuthConfig() != null) {
            AuthConfig.SaslAuthConfig saslAuthConfig = sourceConfig.getAuthConfig().getSaslAuthConfig();
            if (saslAuthConfig != null) {
                awsIamAuthConfig = saslAuthConfig.getAwsIamAuthConfig();
                PlainTextAuthConfig plainTextAuthConfig = saslAuthConfig.getPlainTextAuthConfig();

                if (awsIamAuthConfig != null) {
                    if (encryptionType == EncryptionType.PLAINTEXT) {
                        throw new RuntimeException("Encryption Config must be SSL to use IAM authentication mechanism");
                    }
                    setAwsIamAuthProperties(properties, awsIamAuthConfig, awsConfig);
                } else if (saslAuthConfig.getOAuthConfig() != null) {
                    KafkaSourceSecurityConfigurer.setOauthProperties(sourceConfig, properties);
                } else if (plainTextAuthConfig != null) {
                    setPlainTextAuthProperties(properties, plainTextAuthConfig);
                } else {
                    throw new RuntimeException("No SASL auth config specified");
                }
            } else if (encryptionType == EncryptionType.SSL) {
                properties.put("security.protocol", "SSL");
                if (sourceConfig.getAuthConfig().getInsecure()) {
                    properties.put("ssl.engine.factory.class", InsecureSslEngineFactory.class);
                }
            }
        }
        String bootstrapServers = sourceConfig.getBootStrapServers();
        if (Objects.nonNull(awsIamAuthConfig)) {
            bootstrapServers = getBootStrapServersForMsk(awsIamAuthConfig, awsConfig);
        }
        if (Objects.isNull(bootstrapServers) || bootstrapServers.isEmpty()) {
            throw new RuntimeException("Bootstrap servers are not specified");
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            if (isKafkaClusterExists(sourceConfig.getBootStrapServers())) {
                throw new RuntimeException("Can't be able to connect to the given Kafka brokers... ");
            }

        if (StringUtils.isNotEmpty(sourceConfig.getClientDnsLookup())) {
            properties.put("client.dns.lookup", sourceConfig.getClientDnsLookup());
        }
        if (StringUtils.isNotEmpty(sourceConfig.getSslEndpointIdentificationAlgorithm())) {
            properties.put("ssl.endpoint.identification.algorithm", sourceConfig.getSslEndpointIdentificationAlgorithm());
        }
        setConsumerTopicProperties(properties, topicConfig);
        setSchemaRegistryProperties(properties, topicConfig);
        LOG.info("Starting consumer with the properties : {}", properties);
        return properties;
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
                            "awsRoleArn=\"" + awsConfig.getStsRoleArn() +
                            "\" awsStsRegion=\"" + awsConfig.getRegion() + "\";");
        } else if (awsIamAuthConfig == AwsIamAuthConfig.DEFAULT) {
            properties.put("sasl.jaas.config",
                    "software.amazon.msk.auth.iam.IAMLoginModule required;");
        }
    }

    private void setPlainTextAuthProperties(Properties properties, final PlainTextAuthConfig plainTextAuthConfig) {
        String username = plainTextAuthConfig.getUsername();
        String password = plainTextAuthConfig.getPassword();
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        if (encryptionType == EncryptionType.PLAINTEXT) {
            properties.put("security.protocol", "SASL_PLAINTEXT");
        } else { // EncryptionType.SSL
            properties.put("security.protocol", "SASL_SSL");
        }
        if (sourceConfig.getAuthConfig().getInsecure()) {
            properties.put("ssl.engine.factory.class", InsecureSslEngineFactory.class);
        }
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

    private void setSchemaRegistryProperties(Properties properties, TopicConfig topic) {
        SchemaConfig schemaConfig = sourceConfig.getSchemaConfig();
        if (schemaConfig != null && StringUtils.isNotEmpty(schemaConfig.getRegistryURL())) {
            setPropertiesForSchemaRegistryConnectivity(properties);
            setPropertiesForSchemaType(properties, topic);
        } else if (schemaConfig == null) {
            setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(properties);
        }
    }

    private void setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(Properties properties) {
        Optional<String> schema = Optional.of(Optional.ofNullable(sourceConfig.getSerdeFormat()).orElse(MessageFormat.PLAINTEXT.toString()));
        schemaType = schema.get();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceJsonDeserializer.class);
        } else if (schemaType.equalsIgnoreCase(MessageFormat.PLAINTEXT.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
        }
    }

    private void setPropertiesForSchemaType(Properties properties, TopicConfig topic) {
        Map prop = properties;
        Map<String, String> propertyMap = (Map<String, String>) prop;
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
        properties.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, false);
        schemaRegistryClient = new CachedSchemaRegistryClient(properties.getProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG),
                100, propertyMap);
        try {
            schemaType = schemaRegistryClient.getSchemaMetadata(topic.getName() + "-value",
                    sourceConfig.getSchemaConfig().getVersion()).getSchemaType();
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

    private void setConsumerTopicProperties(Properties properties, TopicConfig topicConfig) {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                topicConfig.getAutoCommit());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                topicConfig.getAutoCommitInterval().toSecondsPart());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                topicConfig.getAutoOffsetReset());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                topicConfig.getConsumerMaxPollRecords());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, topicConfig.getSessionTimeOut());
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, topicConfig.getHeartBeatInterval().toSecondsPart());
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, topicConfig.getFetchMaxBytes().intValue());
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, topicConfig.getFetchMaxWait());
    }

    private void setPropertiesForSchemaRegistryConnectivity(Properties properties) {
        AuthConfig authConfig = sourceConfig.getAuthConfig();
        String schemaRegistryApiKey = sourceConfig.getSchemaConfig().getSchemaRegistryApiKey();
        String schemaRegistryApiSecret = sourceConfig.getSchemaConfig().getSchemaRegistryApiSecret();
        //with plaintext authentication for schema registry
        if ("USER_INFO".equalsIgnoreCase(sourceConfig.getSchemaConfig().getBasicAuthCredentialsSource())
                && authConfig.getSaslAuthConfig().getPlainTextAuthConfig() != null) {
            String schemaBasicAuthUserInfo = schemaRegistryApiKey.concat(":").concat(schemaRegistryApiSecret);
            properties.put("schema.registry.basic.auth.user.info", schemaBasicAuthUserInfo);
            properties.put("basic.auth.credentials.source", "USER_INFO");
        }

        if (authConfig != null && authConfig.getSaslAuthConfig() != null) {
            PlainTextAuthConfig plainTextAuthConfig = authConfig.getSaslAuthConfig().getPlainTextAuthConfig();
            OAuthConfig oAuthConfig = authConfig.getSaslAuthConfig().getOAuthConfig();
            if (plainTextAuthConfig != null) {
                String sasl_mechanism = plainTextAuthConfig.getSaslMechanism();
                String protocol = plainTextAuthConfig.getSecurityProtocol();
                properties.put("sasl.mechanism", sasl_mechanism);
                properties.put("security.protocol", protocol);
            } else if (oAuthConfig != null) {
                properties.put("sasl.mechanism", oAuthConfig.getOauthSaslMechanism());
                properties.put("security.protocol", oAuthConfig.getOauthSecurityProtocol());
            }
        }
    }

    private void isTopicExists(String topicName, String bootStrapServer, Properties properties) {
        List<String> bootStrapServers = new ArrayList<>();
        String servers[];
        if (bootStrapServer.contains(",")) {
            servers = bootStrapServer.split(",");
            bootStrapServers.addAll(Arrays.asList(servers));
        } else {
            bootStrapServers.add(bootStrapServer);
        }
        properties.put("connections.max.idle.ms", 5000);
        properties.put("request.timeout.ms", 10000);
        try (AdminClient client = KafkaAdminClient.create(properties)) {
            boolean topicExists = client.listTopics().names().get().stream().anyMatch(name -> name.equalsIgnoreCase(topicName));
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                LOG.error("Topic does not exist: " + topicName);
            }
            throw new RuntimeException("Exception while checking the topics availability...");
        }
    }

    private boolean isKafkaClusterExists(String bootStrapServers) {
        Socket socket = null;
        String[] serverDetails = new String[0];
        String[] servers = new String[0];
        int counter = 0;
        try {
            if (bootStrapServers.contains(",")) {
                servers = bootStrapServers.split(",");
            } else {
                servers = new String[]{bootStrapServers};
            }
            if (CollectionUtils.isNotEmpty(Arrays.asList(servers))) {
                for (String bootstrapServer : servers) {
                    if (bootstrapServer.contains(":")) {
                        serverDetails = bootstrapServer.split(":");
                        if (StringUtils.isNotEmpty(serverDetails[0])) {
                            InetAddress inetAddress = InetAddress.getByName(serverDetails[0]);
                            socket = new Socket(inetAddress, Integer.parseInt(serverDetails[1]));
                        }
                    }
                }
            }
        } catch (IOException e) {
            counter++;
            LOG.error("Kafka broker : {} is not available...", getMaskedBootStrapDetails(serverDetails[0]));
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (counter == servers.length) {
            return true;
        }
        return false;
    }

    private String getMaskedBootStrapDetails(String serverIP) {
        if (serverIP == null || serverIP.length() <= 4) {
            return serverIP;
        }
        int maskedLength = serverIP.length() - 4;
        StringBuilder maskedString = new StringBuilder(maskedLength);
        for (int i = 0; i < maskedLength; i++) {
            maskedString.append('*');
        }
        return maskedString.append(serverIP.substring(maskedLength)).toString();
    }
}
