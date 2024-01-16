/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MySQLConfigTest {
    @Test
    public void test_get_mysql_connectors() throws IOException {
        final String bootstrapServers = "localhost:9092";
        final Properties authProperties = new Properties();
        authProperties.put("bootstrap.servers", bootstrapServers);
        authProperties.put("testClass", this.getClass());
        authProperties.put("testKey", "testValue");
        MySQLConfig testConfig = buildTestConfig("sample-mysql-pipeline.yaml");
        assertThat(testConfig, notNullValue());
        assertThat(testConfig.buildConnectors(), notNullValue());
        assertThat(testConfig.buildConnectors().size(), is(1));
        // verify Connector
        testConfig.setAuthProperties(authProperties);
        Connector mysqlConnector = testConfig.buildConnectors().get(0);
        assertThat(mysqlConnector, instanceOf(Connector.class));
        final Map<String, String> actualConfig = mysqlConnector.getConfig();
        assertThat(actualConfig.get("connector.class"), is(MySQLConfig.CONNECTOR_CLASS));
        assertThat(actualConfig.get("database.hostname"), is("localhost"));
        assertThat(actualConfig.get("database.port"), is("3306"));
        assertThat(actualConfig.get("database.user"), is("debezium"));
        assertThat(actualConfig.get("database.password"), is("dbz"));
        assertThat(actualConfig.get("snapshot.mode"), is("initial"));
        assertThat(actualConfig.get("topic.prefix"), is("prefix1"));
        assertThat(actualConfig.get("table.include.list"), is("inventory.customers"));
        assertThat(actualConfig.get("schema.history.internal.kafka.bootstrap.servers"), is(bootstrapServers));
        assertThat(actualConfig.get("schema.history.internal.producer.testKey"), is(authProperties.getProperty("testKey")));
        assertThat(actualConfig.get("schema.history.internal.consumer.testKey"), is(authProperties.getProperty("testKey")));
        assertThat(actualConfig.get("schema.history.internal.producer.testClass"), is(this.getClass().getName()));
        assertThat(actualConfig.get("schema.history.internal.consumer.testClass"), is(this.getClass().getName()));
    }

    private MySQLConfig buildTestConfig(final String resourceFileName) throws IOException {
        //Added to load Yaml file - Start
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource(resourceFileName).getFile());
        Object data = yaml.load(fileReader);
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConnectConfigMap = (Map<String, Object>) sourceMap.get("mysql");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConnectConfigMap);
            Reader reader = new StringReader(json);
            return mapper.readValue(reader, MySQLConfig.class);
        }
        return null;
    }
}
