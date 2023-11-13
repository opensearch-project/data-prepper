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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MongoDBConfigTest {

    @Test
    public void test_get_mongodb_connectors() throws IOException {
        MongoDBConfig testConfig = buildTestConfig("sample-mongodb-pipeline.yaml");
        assertThat(testConfig, notNullValue());
        assertThat(testConfig.buildConnectors(), notNullValue());
        assertThat(testConfig.buildConnectors().size(), is(1));
        // verify Connector
        Connector mongodbConnector = testConfig.buildConnectors().get(0);
        assertThat(mongodbConnector, instanceOf(Connector.class));
        final Map<String, String> actualConfig = mongodbConnector.getConfig();
        assertThat(actualConfig.get("connector.class"), is(MongoDBConfig.CONNECTOR_CLASS));
        assertThat(actualConfig.get("mongodb.connection.string"), is("mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"));
        assertThat(actualConfig.get("mongodb.user"), is("debezium"));
        assertThat(actualConfig.get("mongodb.password"), is("dbz"));
        assertThat(actualConfig.get("snapshot.mode"), is("never"));
        assertThat(actualConfig.get("topic.prefix"), is("prefix1"));
        assertThat(actualConfig.get("collection.include.list"), is("test.customers"));
        assertThat(actualConfig.get("mongodb.ssl.enabled"), is("false"));
    }

    @Test
    public void test_get_mongodb_config_props() throws IOException {
        MongoDBConfig testConfig = buildTestConfig("sample-mongodb-pipeline.yaml");
        assertThat(testConfig, notNullValue());
        assertThat(testConfig.getIngestionMode(), is("export_stream"));
        assertThat(testConfig.getCredentialsConfig().getUsername(), is("debezium"));
        assertThat(testConfig.getHostname(), is("localhost"));
        assertThat(testConfig.getPort(), is("27017"));
        assertThat(testConfig.getSSLEnabled(), is(false));
        assertThat(testConfig.getSSLInvalidHostAllowed(), is(false));
        assertThat(testConfig.getCollections().size(), is(1));
        assertThat(testConfig.getExportConfig().getAcknowledgements(), is(false));
        assertThat(testConfig.getExportConfig().getItemsPerPartition(), is(4000));
        assertThat(testConfig.getExportConfig().getReadPreference(), is("secondaryPreferred"));
    }

    private MongoDBConfig buildTestConfig(final String resourceFileName) throws IOException {
        //Added to load Yaml file - Start
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource(resourceFileName).getFile());
        Object data = yaml.load(fileReader);
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConnectConfigMap = (Map<String, Object>) sourceMap.get("mongodb");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConnectConfigMap);
            Reader reader = new StringReader(json);
            return mapper.readValue(reader, MongoDBConfig.class);
        }
        return null;
    }
}
