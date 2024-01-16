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

public class PostgreSQLConfigTest {
    @Test
    public void test_get_postgresql_connectors() throws IOException {
        PostgreSQLConfig testConfig = buildTestConfig("sample-postgres-pipeline.yaml");
        assertThat(testConfig, notNullValue());
        assertThat(testConfig.buildConnectors(), notNullValue());
        assertThat(testConfig.buildConnectors().size(), is(1));
        // verify Connector
        Connector postgresqlConnector = testConfig.buildConnectors().get(0);
        assertThat(postgresqlConnector, instanceOf(Connector.class));
        assertThat(postgresqlConnector.getName(), is("psql.public.customers"));
        final Map<String, String> actualConfig = postgresqlConnector.getConfig();
        assertThat(actualConfig.get("connector.class"), is(PostgreSQLConfig.CONNECTOR_CLASS));
        assertThat(actualConfig.get("plugin.name"), is("pgoutput"));
        assertThat(actualConfig.get("database.hostname"), is("localhost"));
        assertThat(actualConfig.get("database.port"), is("5432"));
        assertThat(actualConfig.get("database.user"), is("debezium"));
        assertThat(actualConfig.get("database.password"), is("dbz"));
        assertThat(actualConfig.get("snapshot.mode"), is("initial"));
        assertThat(actualConfig.get("topic.prefix"), is("psql"));
        assertThat(actualConfig.get("database.dbname"), is("postgres"));
        assertThat(actualConfig.get("table.include.list"), is("public.customers"));
    }

    private PostgreSQLConfig buildTestConfig(final String resourceFileName) throws IOException {
        //Added to load Yaml file - Start
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource(resourceFileName).getFile());
        Object data = yaml.load(fileReader);
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConnectConfigMap = (Map<String, Object>) sourceMap.get("postgresql");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConnectConfigMap);
            Reader reader = new StringReader(json);
            return mapper.readValue(reader, PostgreSQLConfig.class);
        }
        return null;
    }
}
