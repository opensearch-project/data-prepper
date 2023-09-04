package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class KafkaConnectSourceConfigTest {

    @Test
    public void test_kafka_connect_get_connectors() throws IOException {
        final String bootstrapServers = "localhost:9092";
        final Properties authProperties = new Properties();
        authProperties.put("bootstrap.servers", bootstrapServers);
        authProperties.put("testClass", KafkaConnectSourceConfigTest.class);
        authProperties.put("testKey", "testValue");
        KafkaConnectSourceConfig testConfig = buildTestConfig("sample-pipelines.yaml");
        testConfig.setAuthProperty(authProperties);
        assertThat(testConfig, notNullValue());
        assertThat(testConfig.getConnectors(), notNullValue());
        assertThat(testConfig.getConnectors().size(), is(3));
        // verify Connector
        Connector mysqlConnector = testConfig.getConnectors().get(0);
        assertThat(mysqlConnector, instanceOf(Connector.class));
        assertThat(mysqlConnector.getConfig().get("connector.class"), is(MySQLConfig.CONNECTOR_CLASS));
        assertThat(mysqlConnector.getConfig().get("schema.history.internal.kafka.bootstrap.servers"), is(bootstrapServers));
        assertThat(mysqlConnector.getConfig().get("schema.history.internal.producer.testKey"), is(authProperties.getProperty("testKey")));
        assertThat(mysqlConnector.getConfig().get("schema.history.internal.consumer.testKey"), is(authProperties.getProperty("testKey")));
        assertThat(mysqlConnector.getConfig().get("schema.history.internal.producer.testClass"), is(this.getClass().getName()));
        assertThat(mysqlConnector.getConfig().get("schema.history.internal.consumer.testClass"), is(this.getClass().getName()));
        Connector postgresqlConnector = testConfig.getConnectors().get(1);
        assertThat(postgresqlConnector, instanceOf(Connector.class));
        assertThat(postgresqlConnector.getConfig().get("connector.class"), is(PostgreSQLConfig.CONNECTOR_CLASS));
        Connector mongoDbConnector = testConfig.getConnectors().get(2);
        assertThat(mongoDbConnector, instanceOf(Connector.class));
        assertThat(mongoDbConnector.getConfig().get("connector.class"), is(MongoDBConfig.CONNECTOR_CLASS));
    }

    @Test
    public void test_kafka_connect_get_worker_properties() throws IOException {
        final String bootstrapServers = "localhost:9092";
        final Properties authProperties = new Properties();
        authProperties.put("bootstrap.servers", bootstrapServers);
        authProperties.put("testClass", KafkaConnectSourceConfigTest.class);
        authProperties.put("testKey", "testValue");
        KafkaConnectSourceConfig testConfig = buildTestConfig("sample-pipelines.yaml");
        testConfig.setAuthProperty(authProperties);
        // verify WorkerProperties
        assertThat(testConfig.getWorkerProperties(), notNullValue());
        Map<String, String> workerProperties = testConfig.getWorkerProperties().buildKafkaConnectPropertyMap();
        assertThat(workerProperties.get("bootstrap.servers"), is(bootstrapServers));
        assertThat(workerProperties.get("listeners"), is("testhost:123"));
        assertThat(workerProperties.get("group.id"), is("test-group"));
        assertThat(workerProperties.get("offset.storage.topic"), is("test-offsets"));
        assertThat(workerProperties.get("config.storage.topic"), is("test-configs"));
        assertThat(workerProperties.get("status.storage.topic"), is("test-status"));
        assertThat(workerProperties.get("key.converter"), is("test-converter"));
        assertThat(workerProperties.get("key.converter.schema.registry.url"), is("http://testhost:8081/"));
        assertThat(workerProperties.get("value.converter"), is("test-converter"));
        assertThat(workerProperties.get("value.converter.schema.registry.url"), is("http://testhost:8082/"));
        assertThat(workerProperties.get("offset.storage.partitions"), is("2"));
        assertThat(workerProperties.get("offset.flush.interval.ms"), is("6000"));
        assertThat(workerProperties.get("offset.flush.timeout.ms"), is("500"));
        assertThat(workerProperties.get("status.storage.partitions"), is("1"));
        assertThat(workerProperties.get("heartbeat.interval.ms"), is("300"));
        assertThat(workerProperties.get("session.timeout.ms"), is("3000"));
        assertThat(workerProperties.get("producer.client.rack"), is("testhost"));
        assertThat(workerProperties.get("consumer.client.rack"), is("testhost"));
        assertThat(workerProperties.get("testClass"), is(this.getClass().getName()));
        assertThat(workerProperties.get("producer.testClass"), is(this.getClass().getName()));
        assertThat(workerProperties.get("testKey"), is(authProperties.getProperty("testKey")));
        assertThat(workerProperties.get("producer.testKey"), is(authProperties.getProperty("testKey")));
    }

    @Test
    public void test_kafka_connect_config_default() throws IOException {
        KafkaConnectSourceConfig testConfig = buildTestConfig("sample-pipelines-1.yaml");
        assertThat(testConfig, notNullValue());
        WorkerProperties testWorkerProperties = testConfig.getWorkerProperties();
        assertThat(testWorkerProperties, notNullValue());
        assertThat(testWorkerProperties.getOffsetStorageTopic(), is("offset-storage-topic"));
        List<Connector> connectors = testConfig.getConnectors();
        assertThat(connectors.size(), is(0));
    }

    @Test
    public void test_config_setter_getter() throws IOException {
        KafkaConnectSourceConfig testConfig = buildTestConfig("sample-pipelines.yaml");
        AuthConfig authConfig = new AuthConfig();
        AwsConfig awsConfig = new AwsConfig();
        EncryptionConfig encryptionConfig = new EncryptionConfig();
        String bootstrapServer = "testhost:123";
        testConfig.setAuthConfig(authConfig);
        testConfig.setAwsConfig(awsConfig);
        testConfig.setEncryptionConfig(encryptionConfig);
        testConfig.setBootstrapServers(bootstrapServer);
        assertThat(testConfig.getAuthConfig(), is(authConfig));
        assertThat(testConfig.getAwsConfig(), is(awsConfig));
        assertThat(testConfig.getEncryptionConfig(), is(encryptionConfig));
        assertThat(testConfig.getBootStrapServers(), is(bootstrapServer));
    }

    private KafkaConnectSourceConfig buildTestConfig(final String resourceFileName) throws IOException {
        //Added to load Yaml file - Start
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource(resourceFileName).getFile());
        Object data = yaml.load(fileReader);
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConnectConfigMap = (Map<String, Object>) sourceMap.get("kafka_connect");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConnectConfigMap);
            Reader reader = new StringReader(json);
            return mapper.readValue(reader, KafkaConnectSourceConfig.class);
        }
        return null;
    }
}
