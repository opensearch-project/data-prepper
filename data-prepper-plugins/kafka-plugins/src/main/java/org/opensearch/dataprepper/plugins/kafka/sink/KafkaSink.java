/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.sink;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.producer.MultithreadedProducer;
import org.opensearch.dataprepper.plugins.kafka.util.AuthenticationPropertyConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation class of kafka--sink plugin. It is responsible for receive the collection of
 * {@link Event} and produce it to different kafka topics.
 */
@Deprecated
@DataPrepperPlugin(name = "kafka-sink", pluginType = Sink.class, pluginConfigurationType = KafkaSinkConfig.class)
public class KafkaSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);

    private final KafkaSinkConfig kafkaSinkConfig;

    private volatile boolean sinkInitialized;

    private static final Integer totalWorkers = 1;

    private MultithreadedProducer multithreadedProducer;

    private ExecutorService executorService;

    private static String schemaType = "";

    private static final String VALUE_SERIALIZER = "value.serializer";

    private static final String KEY_SERIALIZER = "key.serializer";

    private static final String SESSION_TIMEOUT_MS_CONFIG = "30000";

    private final String REGISTRY_URL = "schema.registry.url";



    @DataPrepperPluginConstructor
    public KafkaSink(final PluginSetting pluginSetting, final KafkaSinkConfig kafkaSinkConfig) {
        super(pluginSetting);
        this.kafkaSinkConfig = kafkaSinkConfig;

    }


    @Override
    public void doInitialize() {
        try {
            doInitializeInternal();
        } catch (InvalidPluginConfigurationException e) {
            LOG.error("Invalid plugin configuration, Hence failed to initialize kafka-sink plugin.");
            this.shutdown();
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to initialize kafka-sink plugin.");
            this.shutdown();
            throw e;
        }
    }

    private void doInitializeInternal() {
        executorService = Executors.newFixedThreadPool(totalWorkers);
        sinkInitialized = Boolean.TRUE;
    }

    @Override
    public void doOutput(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }
        try {
            records.forEach(record -> {
                 multithreadedProducer = new MultithreadedProducer(getProducerProperties(), kafkaSinkConfig, record, pluginMetrics, schemaType);
                //TODO: uncomment this line after testing as this is the right way to do things
                //executorService.submit(multithreadedProducer);
                //TODO: remove this line after testing as it executes the thread immediately
                 executorService.execute(multithreadedProducer);
            });

        } catch (Exception e) {
            LOG.error("Failed to setup the Kafka sink Plugin.", e);
            throw new RuntimeException();
        }
    }

    @Override
    public void shutdown() {
        try {
            if (!executorService.awaitTermination(
                    calculateLongestThreadWaitingTime(), TimeUnit.MILLISECONDS)) {

                executorService.shutdownNow();
            }
            LOG.info("Sink threads are waiting for shutting down...");
            executorService.shutdownNow();
        } catch (InterruptedException e) {
            if (e.getCause() instanceof InterruptedException) {
                LOG.error("Interrupted during sink shutdown, exiting uncleanly...");
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        super.shutdown();
        LOG.info("Producer shutdown successfully...");
    }
    //TODO: read the tread waiting time from config once HLD is finalized
    private long calculateLongestThreadWaitingTime() {
        return 1000L;
    }


    private Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkConfig.getBootStrapServers());

        properties.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS_CONFIG);
        //TODO: schema registry is not being tested for now , this needs to be revisited once the basic functionality starts working
        /*  schemaType = getSchemaType(kafkaSinkConfig.getSchemaConfig().getRegistryURL(), kafkaSinkConfig.getTopic(),
                kafkaSinkConfig.getSchemaConfig().getVersion());
        schemaType = "";
        if (schemaType.isEmpty()) {
            schemaType = MessageFormat.PLAINTEXT.toString();
        }*/
        //TODO: we will be testing with plaintext or json now , this needs to be removed after testing and schema type should be fetched
        // from schema registry if avoilable

        schemaType = MessageFormat.PLAINTEXT.toString();

        setPropertiesForSchemaType(properties, schemaType);
        if(kafkaSinkConfig.getAuthConfig().getPlainTextAuthConfig()!=null) {
            AuthenticationPropertyConfigurer.setSaslPlainTextProperties(kafkaSinkConfig, properties);
        }
        else if (kafkaSinkConfig.getAuthConfig().getoAuthConfig()!=null) {
            AuthenticationPropertyConfigurer.setOauthProperties(kafkaSinkConfig, properties);

        }
        return properties;
    }

    private void setPropertiesForSchemaType(Properties properties, final String schemaType) {
        properties.put(KEY_SERIALIZER, StringSerializer.class.getName());
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(VALUE_SERIALIZER, JsonSerializer.class.getName());

        } else if (schemaType.equalsIgnoreCase(MessageFormat.PLAINTEXT.toString())) {
            properties.put(VALUE_SERIALIZER, StringSerializer.class.getName());

        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(VALUE_SERIALIZER, KafkaAvroSerializer.class.getName());
            properties.put(REGISTRY_URL, kafkaSinkConfig.getSchemaConfig().getRegistryURL());
        }
    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    //TODO: Following is copied from kafka source for schema registry. we need to find a better way to keep this a single place for both source and sink.
    // TODO: Need to test with schema registry
  /*  private static boolean validateURL(String url) {
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

    private String getSchemaRegUrl() {
        return kafkaSinkConfig.getSchemaConfig().getRegistryURL();
    }

    private static String getSchemaType(final String registryUrl, final String topicName, final int schemaVersion) {
        StringBuilder response = new StringBuilder();
        String schemaType = "";
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
                if (rootNode.has("schemaType")) {
                    schemaType = MessageFormat.JSON.toString();
                } else {
                    schemaType = MessageFormat.AVRO.toString();
                }
            } else {
                InputStream errorStream = connection.getErrorStream();
                String errorMessage = readErrorMessage(errorStream);
                LOG.error("GET request failed while fetching the schema registry details : {}", errorMessage);
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
    }*/
}

