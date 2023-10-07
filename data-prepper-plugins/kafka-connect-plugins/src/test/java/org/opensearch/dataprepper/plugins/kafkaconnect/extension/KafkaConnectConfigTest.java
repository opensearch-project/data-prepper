/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.extension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.parser.ByteCountDeserializer;
import org.opensearch.dataprepper.parser.DataPrepperDurationDeserializer;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class KafkaConnectConfigTest {

    private static SimpleModule simpleModule = new SimpleModule()
            .addDeserializer(Duration.class, new DataPrepperDurationDeserializer())
            .addDeserializer(ByteCount.class, new ByteCountDeserializer());
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()).registerModule(simpleModule);

    private KafkaConnectConfig makeConfig(String filePath) throws IOException {
        final File configurationFile = new File(filePath);
        final DataPrepperConfiguration dataPrepperConfiguration = OBJECT_MAPPER.readValue(configurationFile, DataPrepperConfiguration.class);
        assertThat(dataPrepperConfiguration, CoreMatchers.notNullValue());
        assertThat(dataPrepperConfiguration.getPipelineExtensions(), CoreMatchers.notNullValue());
        final Map<String, Object> kafkaConnectConfigMap = (Map<String, Object>) dataPrepperConfiguration.getPipelineExtensions().getExtensionMap().get("kafka_connect_config");
        String json = OBJECT_MAPPER.writeValueAsString(kafkaConnectConfigMap);
        Reader reader = new StringReader(json);
        return OBJECT_MAPPER.readValue(reader, KafkaConnectConfig.class);
    }

    @Test
    public void test_config_setter_getter() throws IOException {
        KafkaConnectConfig testConfig = makeConfig("src/test/resources/sample-data-prepper-config-with-kafka-connect-config-extension.yaml");
        AuthConfig authConfig = new AuthConfig();
        AwsConfig awsConfig = new AwsConfig();
        EncryptionConfig encryptionConfig = new EncryptionConfig();
        List<String> bootstrapServer = List.of("testhost:123");
        testConfig.setAuthConfig(authConfig);
        testConfig.setAwsConfig(awsConfig);
        testConfig.setEncryptionConfig(encryptionConfig);
        testConfig.setBootstrapServers(bootstrapServer);
        assertThat(testConfig.getAuthConfig(), is(authConfig));
        assertThat(testConfig.getAwsConfig(), is(awsConfig));
        assertThat(testConfig.getEncryptionConfig(), is(encryptionConfig));
        assertThat(testConfig.getBootstrapServers(), is(bootstrapServer));
        assertThat(testConfig.getConnectorStartTimeout().getSeconds(), is(3L));
        assertThat(testConfig.getConnectStartTimeout().getSeconds(), is(3L));
    }

    @Test
    public void test_config_get_worker_properties() throws IOException {
        final String bootstrapServers = "localhost:9092";
        final Properties authProperties = new Properties();
        authProperties.put("bootstrap.servers", bootstrapServers);
        authProperties.put("testClass", KafkaConnectConfigTest.class);
        authProperties.put("testKey", "testValue");
        KafkaConnectConfig testConfig = makeConfig("src/test/resources/sample-data-prepper-config-with-kafka-connect-config-extension.yaml");
        testConfig.setAuthProperties(authProperties);
        // verify WorkerProperties
        assertThat(testConfig.getWorkerProperties(), notNullValue());
        Map<String, String> workerProperties = testConfig.getWorkerProperties().buildKafkaConnectPropertyMap();
        assertThat(workerProperties.get("bootstrap.servers"), is(bootstrapServers));
        assertThat(workerProperties.get("group.id"), is("test-group"));
        assertThat(workerProperties.get("client.id"), is("test-client"));
        assertThat(workerProperties.get("offset.storage.topic"), is("test-offsets"));
        assertThat(workerProperties.get("config.storage.topic"), is("test-configs"));
        assertThat(workerProperties.get("status.storage.topic"), is("test-status"));
        assertThat(workerProperties.get("key.converter"), is("org.apache.kafka.connect.json.JsonConverter"));
        assertThat(workerProperties.get("value.converter"), is("org.apache.kafka.connect.json.JsonConverter"));
        assertThat(workerProperties.get("offset.storage.partitions"), is("2"));
        assertThat(workerProperties.get("offset.flush.interval.ms"), is("6000"));
        assertThat(workerProperties.get("offset.flush.timeout.ms"), is("500"));
        assertThat(workerProperties.get("status.storage.partitions"), is("1"));
        assertThat(workerProperties.get("heartbeat.interval.ms"), is("300"));
        assertThat(workerProperties.get("session.timeout.ms"), is("3000"));
        assertThat(workerProperties.get("scheduled.rebalance.max.delay.ms"), is("60000"));
        assertThat(workerProperties.get("testClass"), is(this.getClass().getName()));
        assertThat(workerProperties.get("producer.testClass"), is(this.getClass().getName()));
        assertThat(workerProperties.get("testKey"), is(authProperties.getProperty("testKey")));
        assertThat(workerProperties.get("producer.testKey"), is(authProperties.getProperty("testKey")));
    }

    @Test
    public void test_config_default_worker_properties() throws IOException {
        KafkaConnectConfig testConfig = makeConfig("src/test/resources/sample-data-prepper-config-with-default-kafka-connect-config-extension.yaml");
        assertThat(testConfig, notNullValue());
        assertThat(testConfig.getConnectStartTimeout().getSeconds(), is(60L));
        assertThat(testConfig.getConnectorStartTimeout().getSeconds(), is(360L));
        assertThat(testConfig.getBootstrapServers(), nullValue());
        WorkerProperties testWorkerProperties = testConfig.getWorkerProperties();
        assertThat(testWorkerProperties, notNullValue());
        Map<String, String> workerProperties = testWorkerProperties.buildKafkaConnectPropertyMap();
        assertThat(workerProperties.get("bootstrap.servers"), nullValue());
        assertThat(workerProperties.get("offset.storage.partitions"), is("25"));
        assertThat(workerProperties.get("offset.flush.interval.ms"), is("60000"));
        assertThat(workerProperties.get("offset.flush.timeout.ms"), is("5000"));
        assertThat(workerProperties.get("status.storage.partitions"), is("5"));
        assertThat(workerProperties.get("heartbeat.interval.ms"), is("3000"));
        assertThat(workerProperties.get("session.timeout.ms"), is("30000"));
        assertThat(workerProperties.get("scheduled.rebalance.max.delay.ms"), is("300000"));
    }
}
